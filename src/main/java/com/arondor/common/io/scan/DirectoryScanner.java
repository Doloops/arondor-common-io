package com.arondor.common.io.scan;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.arondor.common.io.AsyncIterator;
import com.arondor.common.io.ConfigurableThreadPoolExecutor;
import com.arondor.common.management.mbean.MBeanObject;

public class DirectoryScanner extends AsyncIterator<String> implements FileScanner
{
    private static final Logger LOGGER = Logger.getLogger(DirectoryScanner.class);

    private static final boolean VERBOSE = LOGGER.isDebugEnabled();

    private final ScheduledThreadPoolExecutor executor = new ConfigurableThreadPoolExecutor(
            "DirectoryScanner_" + System.currentTimeMillis(), 1, 65536);

    public final class DirectoryScannerStats extends MBeanObject
    {
        protected DirectoryScannerStats(String name)
        {
            super(name);
        }

        public boolean isAsync()
        {
            return DirectoryScanner.this.isAsync();
        }

        public int getQueueSize()
        {
            return DirectoryScanner.this.getQueueSize();
        }

        public boolean isRandomize()
        {
            return DirectoryScanner.this.isRandomize();
        }

        public void setRandomize(boolean randomize)
        {
            DirectoryScanner.this.setRandomize(true);
        }

        public int getTotalObjectsAdded()
        {
            return DirectoryScanner.this.getTotalObjectsAdded();
        }

        public int getTotalObjectsIterated()
        {
            return DirectoryScanner.this.getTotalObjectsIterated();
        }

        public boolean isPaused()
        {
            return DirectoryScanner.this.isPaused();
        }

        public void setPaused(boolean paused)
        {
            DirectoryScanner.this.setPaused(paused);
        }

        public void setQueueLimit(int queueLimit)
        {
            DirectoryScanner.this.setQueueLimit(queueLimit);
        }

        public int getQueueLimit()
        {
            return DirectoryScanner.this.getQueueLimit();
        }

        public void setQueueLimitDelay(int queueLimitDelay)
        {
            DirectoryScanner.this.setQueueLimitDelay(queueLimitDelay);
        }

        public int getQueueLimitDelay()
        {
            return DirectoryScanner.this.getQueueLimitDelay();
        }
    }

    private final DirectoryScannerStats directoryScannerStats = new DirectoryScannerStats("DirectoryScanner");

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

    @Override
    public void setFilters(List<String> filters)
    {
        this.filters = filters;
    }

    public List<String> getFilters()
    {
        return this.filters;
    }

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
                            LOGGER.info("Finished thread ! context=" + context);
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
                addPath(resolvedFile.getAbsolutePath());
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
        if (wildcard.startsWith("/"))
            rootFolderPath = "/";
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

    private void buildRecursive(File rootFolder, final String prefix, final Pattern matchPattern)
    {
        if (VERBOSE)
        {
            LOGGER.debug("At buildRecursive(), rootFolder=" + rootFolder.getAbsolutePath() + ", prefix=" + prefix);
        }
        rootFolder.listFiles(new FileFilter()
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
                    }, currentPath, true);
                }
                else
                {
                    if (matchPattern.matcher(currentPath).matches())
                    {
                        addPath(child.getAbsolutePath());
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
        rootFolder.list(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                if (VERBOSE)
                {
                    LOGGER.debug("At dir : " + dir.getAbsolutePath() + ", name : " + name);
                }
                if (recursiveRegex || regexPattern.matcher(name.toLowerCase()).matches())
                {
                    if (VERBOSE)
                    {
                        LOGGER.debug("* matches !");
                    }
                    String absolutePath = dir.getAbsolutePath() + File.separator + name;
                    final File file = new File(absolutePath);
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
                                }, absolutePath, false);
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
                        addPath(absolutePath);
                    }
                }
                return false;
            }
        });
    }

    private void addPath(String absolutePath)
    {
        addObject(absolutePath);
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
}
