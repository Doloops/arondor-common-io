package com.arondor.common.io;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.management.MBeanInfo;

import org.apache.log4j.Logger;

import com.arondor.common.management.mbean.MBeanObjectStub;
import com.arondor.common.management.threadpool.NamedThreadFactory;

public class ConfigurableThreadPoolExecutor extends ScheduledThreadPoolExecutor
{
    private final static Logger LOGGER = Logger.getLogger(ConfigurableThreadPoolExecutor.class);

    private String name;

    private int maxTaskCountBeforeOverflow = Integer.MAX_VALUE;

    public int getMaxTaskCountBeforeOverflow()
    {
        return maxTaskCountBeforeOverflow;
    }

    public void setMaxTaskCountBeforeOverflow(int maxTaskCountBeforeOverflow)
    {
        this.maxTaskCountBeforeOverflow = maxTaskCountBeforeOverflow;
    }

    private MBeanObjectStub mbeanObjectStub;

    /**
     * Default/Empty constructor
     */
    public ConfigurableThreadPoolExecutor()
    {
        this("AnonymousThreadPool_" + Math.random(), 1, 1);
    }

    public ConfigurableThreadPoolExecutor(String name, int corePoolSize, int maxTaskCountBeforeOverflow)
    {
        super(corePoolSize);
        this.name = name;
        mbeanObjectStub = new MBeanObjectStub(getClass(), this, name);
        setThreadFactory(new NamedThreadFactory(name));
        setMaxTaskCountBeforeOverflow(maxTaskCountBeforeOverflow);
    }

    public MBeanInfo getMBeanInfo()
    {
        return mbeanObjectStub.getMBeanInfo();
    }

    @Override
    public void finalize()
    {
        mbeanObjectStub.unregister();
    }

    public String getName()
    {
        return name;
    }

    public int getQueueSize()
    {
        return getQueue().size();
    }

    public int getProcessingSize()
    {
        return getQueueSize() + getActiveCount();
    }

    public synchronized boolean isOverflowed()
    {
        // int current = getQueueSize();
        // int max = getMaxTaskCountBeforeOverflow();
        int current = getProcessingSize();
        int max = getCorePoolSize() + getMaxTaskCountBeforeOverflow();
        boolean overflowed = current > max;
        LOGGER.debug("[Overflow " + getName() + "] queued=" + getQueueSize() + ", processing=" + getProcessingSize()
                + ", active=" + getActiveCount() + ", max=" + getMaxTaskCountBeforeOverflow() + ", (current=" + current
                + " > max=" + max + " ?) => overflow=" + overflowed);
        return overflowed;
    }

    private Runnable encapsulateRunnable(final Runnable task)
    {
        return new Runnable()
        {

            @Override
            public void run()
            {
                try
                {
                    task.run();
                }
                catch (RuntimeException e)
                {
                    LOGGER.error("RuntimeException not caught at ThreadPool=" + getName() + ", task=" + task, e);
                }
                catch (Error e)
                {
                    LOGGER.error("Error not caught at ThreadPool=" + getName() + ", task=" + task, e);
                }
                catch (Exception e)
                {
                    LOGGER.error("Exception not caught at ThreadPool=" + getName() + ", task=" + task, e);
                }
            }
        };
    }

    @Override
    public synchronized Future<?> submit(Runnable task)
    {
        try
        {
            return super.submit(encapsulateRunnable(task));
        }
        catch (java.util.concurrent.RejectedExecutionException e)
        {
            LOGGER.error("Rejected Task submit : at ThreadPool=" + getName() + ", task=" + task, e);
            throw new RuntimeException("Rejected Task submit : at ThreadPool=" + getName() + ", task=" + task, e);
        }
    }

}
