package com.arondor.common.io.scan;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.arondor.common.io.AsyncIterator;
import com.arondor.common.io.ConfigurableThreadPoolExecutor;

public class DirectoryScanner extends AsyncIterator<String> implements FileScanner
{
    private static final Logger LOGGER = Logger.getLogger(DirectoryScanner.class);

    private static final boolean VERBOSE = LOGGER.isDebugEnabled();

    private final ScheduledThreadPoolExecutor executor = new ConfigurableThreadPoolExecutor(
            "DirectoryScanner_" + System.currentTimeMillis(), 1, 65536);

    public DirectoryScanner()
    {
    }

    /**
     * Check if a path has wildcard characters or not
     * 
     * @param path
     *            the path to check
     * @return true if the path has wildcards, false otherwise
     */
    public static boolean isWildcard(String path)
    {
        return (path.contains("*") || path.contains("?"));
    }

    private List<String> filters;

    private List<String> excludedExtensions = new ArrayList<String>();

    private boolean filterOutInconsistentNames = true;

    private boolean sortFolderChildren = false;

    @Override
    protected boolean doScanOneItem()
    {
        buildList(filters);
        if (executor != null)
        {
            while (true)
            {
                try
                {
                    executorMaybeFinished.acquire();
                    if (VERBOSE)
                    {
                        LOGGER.debug("spawnedThreadsNumber=" + spawnedThreadsNumber.get());
                    }
                    if (spawnedThreadsNumber.get() != 0)
                    {
                        if (VERBOSE)
                        {
                            LOGGER.debug("Still some work to do here !");
                        }
                        continue;
                    }
                    if (VERBOSE)
                    {
                        LOGGER.debug("Shutdown !");
                    }
                    executor.shutdown();
                    executor.awaitTermination(60, TimeUnit.MINUTES);
                    break;
                }
                catch (InterruptedException e)
                {
                    LOGGER.error("Caught exception", e);
                }
            }
        }
        LOGGER.info("Total number of tasks spawned totalSpawnedThreadsNumber=" + totalSpawnedThreadsNumber.get()
                + ", items added=" + getTotalObjectsAdded() + ", iterated=" + getTotalObjectsIterated());
        return false;
    }

    private final Semaphore executorMaybeFinished = new Semaphore(1);

    private final AtomicInteger spawnedThreadsNumber = new AtomicInteger();

    private final AtomicInteger totalSpawnedThreadsNumber = new AtomicInteger();

    private void mayspawn(final Runnable runnable, final String context, boolean spawnable)
    {
        if (VERBOSE)
        {
            LOGGER.debug("Current executor load : active=" + executor.getActiveCount() + ", queue="
                    + executor.getQueue().size());
        }
        if (executor == null)
        {
            spawnable = false;
        }
        if (spawnable && (executor.getActiveCount() + executor.getQueue().size()) < executor.getCorePoolSize() * 2)
        {
            spawnedThreadsNumber.incrementAndGet();
            totalSpawnedThreadsNumber.incrementAndGet();
            executor.execute(new Runnable()
            {

                @Override
                public void run()
                {
                    if (VERBOSE)
                    {
                        LOGGER.debug("Started thread ! spawnedThreadsNumber=" + spawnedThreadsNumber.get()
                                + ", context=" + context);
                    }
                    try
                    {
                        runnable.run();
                    }
                    finally
                    {
                        if (VERBOSE)
                        {
                            LOGGER.debug("Finished thread ! context=" + context);
                        }
                        spawnedThreadsNumber.decrementAndGet();
                        executorMaybeFinished.release();
                    }
                }
            });
            return;
        }
        try
        {
            runnable.run();
        }
        catch (Exception e)
        {
            LOGGER.error("Caught exception", e);
        }
    }

    private void buildList(List<String> paths)
    {
        int total = paths.size();
        int nb = 0;
        for (String path : paths)
        {
            if (VERBOSE)
            {
                LOGGER.debug("At path '" + path + "', (" + nb + " of " + total + ").");
            }
            buildList(path);
            nb++;
        }
    }

    private void buildList(String wildcard)
    {
        /*
         * Simple optimization : if it's not a wildcard, it's fully-defined.
         */
        if (!isWildcard(wildcard))
        {
            File resolvedFile = new File(wildcard);
            if (resolvedFile.exists())
            {
                if (VERBOSE)
                {
                    LOGGER.debug("Found fully-resolved file : '" + resolvedFile.getAbsolutePath() + "'");
                }
                addFile(resolvedFile);
            }
            return;
        }
        wildcard = wildcard.replace('\\', '/');
        String wildcards[] = wildcard.split("/");
        if (wildcards[wildcards.length - 1].equals("**"))
        {
            LOGGER.warn("Invalid pattern " + wildcard + ", could not finish with **");
            return;
        }

        File rootFolder = null;
        String rootFolderPath = "";
        // if (wildcard.startsWith("/"))
        // rootFolderPath = "/";
        int startIdx;
        for (startIdx = 0; startIdx < wildcards.length - 1; startIdx++)
        {
            if (isWildcard(wildcards[startIdx]))
            {
                break;
            }
            rootFolderPath += wildcards[startIdx];
            rootFolderPath += "/";
        }
        if (VERBOSE)
        {
            LOGGER.debug("Found root (wildcard free) path : " + rootFolderPath);
        }
        rootFolder = new File(rootFolderPath);
        if (!rootFolder.exists())
        {
            LOGGER.debug("Path '" + rootFolderPath + "' does not exist !");
            return;
        }
        if (!rootFolder.isDirectory())
        {
            throw new RuntimeException("Path '" + rootFolderPath + "' is not a folder !");
        }
        if (VERBOSE)
        {
            LOGGER.debug("Calling buildList() with rootFolder=" + rootFolder.getAbsolutePath() + ", wildcards="
                    + Arrays.toString(wildcards) + ", startIdx=" + startIdx);
        }
        buildList(rootFolder, wildcards, startIdx);
    }

    private void doListFiles(File parent, FileFilter filter)
    {
        if (isSortFolderChildren())
        {
            doListOrderedFiles(parent, filter);
        }
        else
        {
            parent.listFiles(filter);
        }
    }

    private void doListOrderedFiles(File parent, FileFilter filter)
    {
        File files[] = parent.listFiles();
        Arrays.sort(files, new Comparator<File>()
        {
            @Override
            public int compare(File o1, File o2)
            {
                return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
            }
        });
        for (File f : files)
        {
            filter.accept(f);
        }
    }

    private void buildRecursive(File rootFolder, final String prefix, final Pattern matchPattern)
    {
        if (VERBOSE)
        {
            LOGGER.debug("At buildRecursive(), rootFolder=" + rootFolder.getAbsolutePath() + ", prefix=" + prefix);
        }
        doListFiles(rootFolder, new FileFilter()
        {
            @Override
            public boolean accept(final File child)
            {
                final String currentPath = prefix + "/" + child.getName();
                if (child.isDirectory())
                {
                    mayspawn(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            buildRecursive(child, currentPath, matchPattern);
                        }
                    }, child.getAbsolutePath(), true);
                }
                else
                {
                    if (matchPattern.matcher(currentPath).matches())
                    {
                        addFile(child);
                    }
                }
                return false;
            }
        });
    }

    private Pattern buildRecursivePattern(String[] wildcards, int startIdx)
    {
        StringBuilder regex = null;
        for (int idx = startIdx; idx < wildcards.length; idx++)
        {
            if (regex == null)
            {
                regex = new StringBuilder();
            }
            else
            {
                regex.append("/");
            }
            String wildcard = wildcards[idx];
            if (wildcard.equals("**"))
            {
                regex.append(".*");
            }
            else
            {
                regex.append(folderSyntaxToRegexSyntax(wildcard));
            }
        }
        if (VERBOSE)
        {
            LOGGER.debug("Converted " + Arrays.toString(wildcards) + " idx=" + startIdx + " to " + regex.toString());
        }
        return Pattern.compile(regex.toString());
    }

    private String folderSyntaxToRegexSyntax(String pattern)
    {
        return pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
    }

    private void buildList(File rootFolder, final String wildcards[], final int idx)
    {
        String wildcard = wildcards[idx];
        if (wildcard.indexOf('/') != -1)
        {
            LOGGER.fatal("At rootFolder=" + rootFolder.getAbsolutePath() + ", not supported ! wildcard=" + wildcard);
            throw new IllegalArgumentException(
                    "At rootFolder=" + rootFolder.getAbsolutePath() + ", not supported ! wildcard=" + wildcard);
        }
        final boolean recursiveRegex = wildcard.equals("**");
        if (recursiveRegex)
        {
            buildRecursive(rootFolder, "", buildRecursivePattern(wildcards, idx));
            return;
        }

        final boolean lastPartOfPath = idx >= wildcards.length - 1;
        String regexSrc = wildcard.substring(wildcard.lastIndexOf("/") + 1);
        regexSrc = folderSyntaxToRegexSyntax(regexSrc);

        String regex = regexSrc.toLowerCase();
        final Pattern regexPattern = Pattern.compile(regex);
        /*
         * Marks the recursion flag '**'
         */
        if (VERBOSE)
        {
            LOGGER.debug("buildList() " + rootFolder.getPath() + ", wildcard=" + wildcard + ", regex=" + regex
                    + ", recursive=" + recursiveRegex + ", idx=" + idx);
        }

        doListFiles(rootFolder, new FileFilter()
        {
            @Override
            public boolean accept(final File file)
            {
                if (VERBOSE)
                {
                    LOGGER.debug("At file : " + file.getAbsolutePath());
                }
                String name = file.getName();
                if (recursiveRegex || regexPattern.matcher(name.toLowerCase()).matches())
                {
                    if (VERBOSE)
                    {
                        LOGGER.debug("* matches !");
                    }
                    String absolutePath = file.getAbsolutePath();
                    if (file.isDirectory())
                    {
                        if (!lastPartOfPath)
                        {
                            try
                            {
                                mayspawn(new Runnable()
                                {
                                    @Override
                                    public void run()
                                    {
                                        if (recursiveRegex)
                                        {
                                            buildList(file, wildcards, idx);
                                        }
                                        buildList(file, wildcards, idx + 1);
                                    }
                                }, absolutePath, true);
                            }
                            catch (Exception e)
                            {
                                LOGGER.error("Could not fetch '" + absolutePath + "'", e);
                            }
                        }
                        else
                        {
                            if (VERBOSE)
                            {
                                LOGGER.debug("Matched, but is dir and not the last part of path :" + absolutePath);
                            }
                        }
                    }
                    else if (!recursiveRegex)
                    {
                        addFile(file);
                    }
                }
                return false;
            }

        });
    }

    private String getExtension(File file)
    {
        String name = file.getName();
        int idx = name.lastIndexOf('.');
        if (idx != -1)
        {
            return name.substring(idx + 1);
        }
        return "";
    }

    private void addFile(File file)
    {
        if (!excludedExtensions.isEmpty())
        {
            String extension = getExtension(file);
            if (excludedExtensions.contains(extension))
            {
                if (VERBOSE)
                {
                    LOGGER.debug("Filter out file : " + file.getAbsolutePath() + ", extension=" + extension);
                }
                return;
            }
        }
        if (isFilterOutInconsistentNames())
        {
            String filename = file.getName();
            for (int chr = 0; chr < filename.length(); chr++)
            {
                int codePoint = filename.codePointAt(chr);
                if (codePoint < 0x20)
                {
                    LOGGER.warn("Invalid character for file: " + file.getAbsolutePath());
                    return;
                }
            }
        }
        addObject(file.getAbsolutePath());
    }

    @Override
    public List<String> getIncludedFiles()
    {
        List<String> list = new ArrayList<String>();
        while (hasNext())
        {
            String file = next();
            list.add(file);
        }
        return list;
    }

    @Override
    public void setFilters(List<String> filters)
    {
        this.filters = filters;
    }

    public List<String> getFilters()
    {
        return this.filters;
    }

    public List<String> getExcludedExtensions()
    {
        return excludedExtensions;
    }

    public void setExcludedExtensions(List<String> excludedExtensions)
    {
        this.excludedExtensions = excludedExtensions;
    }

    public void setCorePoolSize(int corePoolSize)
    {
        if (executor != null)
        {
            executor.setCorePoolSize(corePoolSize);
        }
    }

    public int getCorePoolSize()
    {
        if (executor != null)
        {
            return executor.getCorePoolSize();
        }
        return 0;
    }

    public boolean isFilterOutInconsistentNames()
    {
        return filterOutInconsistentNames;
    }

    public void setFilterOutInconsistentNames(boolean filterOutInconsistentNames)
    {
        this.filterOutInconsistentNames = filterOutInconsistentNames;
    }

    public boolean isSortFolderChildren()
    {
        return sortFolderChildren;
    }

    public void setSortFolderChildren(boolean sortFolderChildren)
    {
        this.sortFolderChildren = sortFolderChildren;
    }
}
