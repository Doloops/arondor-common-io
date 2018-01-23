package com.arondor.common.io.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

public class TestDirectoryScanner
{
    private static final Logger LOGGER = Logger.getLogger(TestDirectoryScanner.class);

    @Test
    public void testDirScan()
    {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setAsync(true);
        List<String> filters = new ArrayList<String>();
        filters.add(".\\**\\*.*");
        scanner.setFilters(filters);
        for (String file : scanner)
        {
            LOGGER.info("At file : " + file);
        }
    }
}
