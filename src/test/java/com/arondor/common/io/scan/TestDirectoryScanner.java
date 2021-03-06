package com.arondor.common.io.scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class TestDirectoryScanner
{
    private static final Logger LOGGER = Logger.getLogger(TestDirectoryScanner.class);

    private static final String substringAfter(String source, String pattern)
    {
        source = source.replace('\\', '/');
        int idx = source.indexOf(pattern);
        if (idx < 0)
        {
            throw new IllegalArgumentException("Pattern " + pattern + " not part of " + source);
        }
        return source.substring(idx + pattern.length());
    }

    @Before
    public void init()
    {
    }

    @Test
    public void testDirScan_test1()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setAsync(false);
        List<String> filters = new ArrayList<String>();
        filters.add("./src/test/resources/test1/**/*.*");
        scanner.setFilters(filters);

        List<String> result = new ArrayList<String>();

        for (String file : scanner)
        {
            result.add(substringAfter(file, "/resources/"));
        }
        Collections.sort(result);
        LOGGER.info("Result " + result);
        Assert.assertEquals(5, result.size());
        Assert.assertEquals("test1/a/b/b1.txt", result.get(0));
        Assert.assertEquals("test1/a/b/c/c1.txt", result.get(1));
        Assert.assertEquals("test1/a/b/c/d/e.txt", result.get(2));
        Assert.assertEquals("test1/a/b/e/f/g.txt", result.get(3));
        Assert.assertEquals("test1/a/b/e/f1.txt", result.get(4));

    }

    @Test
    public void testDirScan_test2()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setAsync(false);
        List<String> filters = new ArrayList<String>();
        filters.add("./src/test/resources/test2/**/*.*");
        scanner.setFilters(filters);

        List<String> result = new ArrayList<String>();

        for (String file : scanner)
        {
            result.add(substringAfter(file, "/resources/"));
        }
        Collections.sort(result);
        LOGGER.info("Result " + result);
        Assert.assertEquals(5, result.size());
        Assert.assertEquals("test2/a/b/b1.txt", result.get(0));
        Assert.assertEquals("test2/a/b/c/c1.txt", result.get(1));
        Assert.assertEquals("test2/a/b/c/d.0/e.txt", result.get(2));
        Assert.assertEquals("test2/a/b/e/f/g.txt", result.get(3));
        Assert.assertEquals("test2/a/b/e/f1.txt", result.get(4));

    }

    @Test
    public void testDirScan_test2_b()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setAsync(false);
        List<String> filters = new ArrayList<String>();
        filters.add("./src/test/resources/test2/**/b/**/*.*");
        scanner.setFilters(filters);

        List<String> result = new ArrayList<String>();

        for (String file : scanner)
        {
            result.add(substringAfter(file, "/resources/"));
        }
        Collections.sort(result);
        LOGGER.info("Result " + result);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals("test2/a/b/c/c1.txt", result.get(0));
        Assert.assertEquals("test2/a/b/c/d.0/e.txt", result.get(1));
        Assert.assertEquals("test2/a/b/e/f/g.txt", result.get(2));
        Assert.assertEquals("test2/a/b/e/f1.txt", result.get(3));

    }

    @Test
    public void testDirScan_test2_c()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setAsync(false);
        List<String> filters = new ArrayList<String>();
        filters.add("./src/test/resources/test2/**/c/**");
        scanner.setFilters(filters);

        List<String> result = new ArrayList<String>();

        for (String file : scanner)
        {
            result.add(substringAfter(file, "/resources/"));
        }
        Collections.sort(result);
        LOGGER.info("Result " + result);
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testDirScan_test3_filterExtensions()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        List<String> excludedExtensions = new ArrayList<String>();
        excludedExtensions.add("pcl");
        scanner.setExcludedExtensions(excludedExtensions);
        scanner.setAsync(false);
        List<String> filters = new ArrayList<String>();
        filters.add("./src/test/resources/test3/*/*/*.*");
        scanner.setFilters(filters);

        List<String> result = new ArrayList<String>();

        for (String file : scanner)
        {
            result.add(substringAfter(file, "/resources/"));
        }
        Collections.sort(result);
        LOGGER.info("Result " + result);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("test3/a/b/2.1.txt", result.get(0));
        Assert.assertEquals("test3/a/b/2.txt", result.get(1));
    }

    @Test
    public void testDirScan_test3_filterExtensions_Wildcard()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        List<String> excludedExtensions = new ArrayList<String>();
        excludedExtensions.add("pcl");
        scanner.setExcludedExtensions(excludedExtensions);
        scanner.setAsync(false);
        List<String> filters = new ArrayList<String>();
        filters.add("./src/test/resources/test3/**/*");
        scanner.setFilters(filters);

        List<String> result = new ArrayList<String>();

        for (String file : scanner)
        {
            result.add(substringAfter(file, "/resources/"));
        }
        Collections.sort(result);
        LOGGER.info("Result " + result);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("test3/a/b/2.1.txt", result.get(0));
        Assert.assertEquals("test3/a/b/2.txt", result.get(1));
        Assert.assertEquals("test3/a/b/3", result.get(2));
    }
}
