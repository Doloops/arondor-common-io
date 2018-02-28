package com.arondor.common.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import com.arondor.common.management.mbean.MBeanObject;

public abstract class AsyncIterator<T> implements Iterator<T>, Iterable<T>
{
    private static final Logger LOGGER = Logger.getLogger(AsyncIterator.class);

    /**
     * Configuration : asynchronous or not
     */
    private boolean async = false;

    /**
     * 
     */
    private int asyncThreads = 1;

    /**
     * Configuration : limit number of elements in queue
     */
    private int queueLimit = 0;

    /**
     * Configuration : delay limit number of elements in queue
     */
    private int queueLimitDelay = 100;

    /**
     * List of file paths to handle
     */
    private List<T> objectList = new ArrayList<T>();

    /**
     * Total number of objects added to the iterator
     */
    private int totalObjectsAdded = 0;

    /**
     * Total number of objects iterated over
     */
    private int totalObjectsIterated = 0;

    /**
     * Is asynchronous scanning called or not ?
     */
    private boolean asyncScanCalled = false;

    /**
     * Is the async iterator in pause ?
     */
    private boolean paused = false;

    /**
     * Is parsing finished or not
     */
    private boolean hasParsed = false;

    /**
     * Is scanning interrupted
     */
    private boolean interrupted = false;

    /**
     * Parse semaphore
     */
    private Semaphore parseSemaphore = new Semaphore(0);

    /**
     * Randomize input values
     */
    private boolean randomize = false;

    @Override
    public boolean hasNext()
    {
        if (isAsync())
        {
            synchronized (this)
            {
                if (!asyncScanCalled)
                {
                    callAsyncScan();
                }
            }
            try
            {
                parseSemaphore.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted !", e);
            }
            synchronized (this)
            {
                // LOGGER.debug("acquired, hasParsed=" + hasParsed + ",
                // isEmpty=" + objectList.isEmpty());
                if (!objectList.isEmpty())
                {
                    /*
                     * We have elements in queue, so we can consume at least one
                     */
                    return true;
                }
                else if (hasParsed)
                {
                    /*
                     * In case we are a bunch waiting sitting on parse Semaphore
                     */
                    parseSemaphore.release();
                    return false;
                }
                else
                {
                    throw new RuntimeException("Files list is empty, but hasParsed is false !");
                }
            }
        }
        else
        {
            if (!objectList.isEmpty())
            {
                return true;
            }
            if (hasParsed)
            {
                return false;
            }
            if (doScanOneItem())
            {
                return true;
            }
            else
            {
                hasParsed = true;
            }
            return !objectList.isEmpty();
        }
    }

    private final Random randomGenerator = new Random();

    @Override
    public synchronized T next()
    {
        if (objectList.isEmpty())
        {
            throw new RuntimeException("next() : objectList is empty !");
        }
        int index = 0;
        if (isRandomize())
        {
            int size = objectList.size();
            if (size > 10)
            {
                index = randomGenerator.nextInt(size);
            }
        }
        T obj = objectList.get(index);
        if (obj == null)
        {
            throw new RuntimeException("objectList.get(" + index + ") returned null !");
        }
        objectList.remove(index);
        totalObjectsIterated++;
        return obj;
    }

    @Override
    public void remove()
    {
        throw new RuntimeException("Invalid remove() on async iterator !");
    }

    public void setAsync(boolean async)
    {
        this.async = async;
    }

    public boolean isAsync()
    {
        return async;
    }

    protected synchronized void doAddObject(T obj)
    {
        objectList.add(obj);
        totalObjectsAdded++;
        if (isAsync())
        {
            parseSemaphore.release();
        }
    }

    protected void addObject(T obj)
    {
        while (isPaused() || (isAsync() && getQueueLimit() > 0 && objectList.size() >= getQueueLimit()))
        {
            if (interrupted)
            {
                throw new RuntimeException("Interrupted parsing !" + this);
            }
            try
            {
                Thread.sleep(getQueueLimitDelay());
            }
            catch (InterruptedException e)
            {
                LOGGER.error("Interrupted while waiting for queue limit...");
                break;
            }
        }
        doAddObject(obj);
    }

    private synchronized void setFinished()
    {
        LOGGER.debug("**** setFinished() *****");
        hasParsed = true;
        if (isAsync())
            parseSemaphore.release();
    }

    /**
     * Method to implement for asynchronous fetching
     * 
     * @return true if scanning shall continue, false if terminated
     */
    protected abstract boolean doScanOneItem();

    protected synchronized void callAsyncScan()
    {
        if (!isAsync())
            throw new RuntimeException("Invalid call to callAsyncScan() : not async !");
        if (asyncScanCalled)
            throw new RuntimeException("Invalid call to callAsyncScan() : already called !");
        asyncScanCalled = true;
        for (int t = 0; t < getAsyncThreads(); t++)
        {
            Thread parsingThread = new Thread()
            {
                @Override
                public void run()
                {
                    LOGGER.info("Starting async thread for " + AsyncIterator.this.getClass().getName());
                    try
                    {
                        while (doScanOneItem() && !interrupted)
                        {
                            /**
                             * We loop against this
                             */
                        }
                    }
                    catch (Throwable t)
                    {
                        LOGGER.error("Could not scan : ", t);
                    }
                    finally
                    {
                        LOGGER.info("Finished async thread for " + AsyncIterator.this.getClass().getName());
                        setFinished();
                    }
                }
            };
            parsingThread.start();
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return this;
    }

    public int getQueueSize()
    {
        return this.objectList.size();
    }

    public void setQueueLimit(int queueLimit)
    {
        this.queueLimit = queueLimit;
    }

    public int getQueueLimit()
    {
        return queueLimit;
    }

    public void setQueueLimitDelay(int queueLimitDelay)
    {
        this.queueLimitDelay = queueLimitDelay;
    }

    public int getQueueLimitDelay()
    {
        return queueLimitDelay;
    }

    public void interruptParsing()
    {
        interrupted = true;
        // if ( isAsync() && parsingThread != null )
        // {
        // parsingThread.interrupt();
        // }
    }

    public void setAsyncThreads(int asyncThreads)
    {
        this.asyncThreads = asyncThreads;
    }

    public int getAsyncThreads()
    {
        return asyncThreads;
    }

    public boolean isRandomize()
    {
        return randomize;
    }

    public void setRandomize(boolean randomize)
    {
        this.randomize = randomize;
    }

    public int getTotalObjectsAdded()
    {
        return totalObjectsAdded;
    }

    public int getTotalObjectsIterated()
    {
        return totalObjectsIterated;
    }

    public boolean isPaused()
    {
        return paused;
    }

    public void setPaused(boolean paused)
    {
        this.paused = paused;
    }

    /**
     * Stats part
     */

    public final class AsyncIteratorStats extends MBeanObject
    {
        protected AsyncIteratorStats(String name)
        {
            super(name);
        }

        public boolean isAsync()
        {
            return AsyncIterator.this.isAsync();
        }

        public int getQueueSize()
        {
            return AsyncIterator.this.getQueueSize();
        }

        public boolean isRandomize()
        {
            return AsyncIterator.this.isRandomize();
        }

        public void setRandomize(boolean randomize)
        {
            AsyncIterator.this.setRandomize(randomize);
        }

        public int getTotalObjectsAdded()
        {
            return AsyncIterator.this.getTotalObjectsAdded();
        }

        public int getTotalObjectsIterated()
        {
            return AsyncIterator.this.getTotalObjectsIterated();
        }

        public boolean isPaused()
        {
            return AsyncIterator.this.isPaused();
        }

        public void setPaused(boolean paused)
        {
            AsyncIterator.this.setPaused(paused);
        }

        public void setQueueLimit(int queueLimit)
        {
            AsyncIterator.this.setQueueLimit(queueLimit);
        }

        public int getQueueLimit()
        {
            return AsyncIterator.this.getQueueLimit();
        }

        public void setQueueLimitDelay(int queueLimitDelay)
        {
            AsyncIterator.this.setQueueLimitDelay(queueLimitDelay);
        }

        public int getQueueLimitDelay()
        {
            return AsyncIterator.this.getQueueLimitDelay();
        }
    }

    private final AsyncIteratorStats asyncIteratorStats = new AsyncIteratorStats(this.getClass().getName());
}
