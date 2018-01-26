package com.arondor.common.io.scan;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.arondor.common.io.AsyncIterator;

public class DirectoryScanner extends AsyncIterator<String> implements FileScanner
{
    private static final Logger LOGGER = Logger.getLogger(DirectoryScanner.class);

    private static final boolean VERBOSE = LOGGER.isDebugEnabled();

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

    protected List<String> filters;

    @Override
    public void setFilters(List<String> filters)
    {
        this.filters = filters;
    }

    @Override
    protected boolean doScanOneItem()
    {
        buildList(filters);
        return false;
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
            public boolean accept(File child)
            {
                String currentPath = prefix + "/" + child.getName();
                if (child.isDirectory())
                {
                    buildRecursive(child, currentPath, matchPattern);
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
                    File file = new File(absolutePath);
                    if (file.isDirectory())
                    {
                        if (!lastPartOfPath)
                        {
                            try
                            {
                                if (recursiveRegex)
                                {
                                    buildList(file, wildcards, idx);
                                }
                                buildList(file, wildcards, idx + 1);
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
