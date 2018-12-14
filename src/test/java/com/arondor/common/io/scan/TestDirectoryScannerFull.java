package com.arondor.common.io.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class TestDirectoryScannerFull
{
    private static final Logger LOGGER = Logger.getLogger(TestDirectoryScannerFull.class);

    @Before
    public void init()
    {
    }

    @Test
    public void testDirScan_test1() throws InterruptedException
    {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setCorePoolSize(8);
        scanner.setAsync(true);
        List<String> filters = new ArrayList<String>();
<<<<<<< HEAD
        filters.add("/home/francois/git/**/*.*");
        // filters.add("c:/**/*.*");
=======
        filters.add("/home/francois/git/*/**/*.*");
>>>>>>> branch 'master' of git@github.com:Doloops/arondor-common-io.git
        scanner.setFilters(filters);

        int count = 0;
        for (String file : scanner)
        {
            count++;
            if (count % 10000 == 0)
            {
                LOGGER.info("At " + file);
                Thread.sleep(100);
            }
        }
        LOGGER.info("Total : " + count);
    }
}
