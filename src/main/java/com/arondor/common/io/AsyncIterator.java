package com.arondor.common.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public abstract class AsyncIterator<T> implements Iterator<T>, Iterable<T>
{
  private static final Logger log = Logger.getLogger(AsyncIterator.class);

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
   * Is asynchronous scanning called or not ? 
   */
  private boolean asyncScanCalled = false;
  
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

  public boolean hasNext()
  {
    if ( isAsync() )
    {
      synchronized (this)
      {
        if ( ! asyncScanCalled )
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
        log.debug("acquired, hasParsed=" + hasParsed + ", isEmpty=" + objectList.isEmpty());
        if ( !objectList.isEmpty() )
        {
          /*
           * We have elements in queue, so we can consume at least one
           */
          return true;
        }
        else if ( hasParsed )
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
      if ( !objectList.isEmpty() )
      {
        return true;
      }
      if ( hasParsed )
      {
        return false;
      }
      if ( doScanOneItem() )
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

  public synchronized T next()
  {
    if ( objectList.isEmpty() )
    {
      throw new RuntimeException("next() : objectList is empty !");
    }
    T obj = objectList.get(0);
    if (obj== null)
      throw new RuntimeException("objectList.get(0) returned null !");
    objectList.remove(0);
    return obj;
  }

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
    if ( isAsync() )
      parseSemaphore.release();
  }
  
  protected void addObject(T obj)
  {
    while ( isAsync() && getQueueLimit() > 0 && objectList.size() >= getQueueLimit() )
    {
      if ( interrupted )
        throw new RuntimeException("Interrupted parsing !" + this);
      try
      {
        Thread.sleep(getQueueLimitDelay());
      }
      catch (InterruptedException e)
      {
        log.error("Interrupted while waiting for queue limit...");
        break;
      }
    }
    doAddObject(obj);
  }

  private synchronized void setFinished()
  {
    log.debug("**** setFinished() *****");
    hasParsed = true;
    if ( isAsync() )
      parseSemaphore.release();
  }
  
  /**
   * Method to implement for asynchronous fetching
   * @return true if scanning shall continue, false if terminated
   */
  protected abstract boolean doScanOneItem();
  
  protected synchronized void callAsyncScan()
  {
    if ( !isAsync() )
      throw new RuntimeException("Invalid call to callAsyncScan() : not async !");
    if ( asyncScanCalled )
      throw new RuntimeException("Invalid call to callAsyncScan() : already called !");
    asyncScanCalled = true;
    for ( int t = 0 ; t < getAsyncThreads() ; t++ )
    {
      Thread parsingThread = new Thread()
      {
        public void run()
        {
          try
          {
            while ( doScanOneItem() && !interrupted )
            {
              /**
               * We loop against this
               */
            }
          }
          catch (Throwable t)
          {
            log.error("Could not scan : ", t);
          }
          finally
          {
            setFinished();
          }
        }
      };
      parsingThread.start();
    }
  }

  public Iterator<T> iterator()
  {
    return this;
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
//    if ( isAsync() && parsingThread != null )
//    {
//      parsingThread.interrupt();
//    }
  }

  public void setAsyncThreads(int asyncThreads)
  {
    this.asyncThreads = asyncThreads;
  }

  public int getAsyncThreads()
  {
    return asyncThreads;
  }
}
