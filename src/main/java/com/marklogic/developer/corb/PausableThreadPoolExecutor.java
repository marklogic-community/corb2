/*
  * * Copyright (c) 2004-2016 MarkLogic Corporation
  * *
  * * Licensed under the Apache License, Version 2.0 (the "License");
  * * you may not use this file except in compliance with the License.
  * * You may obtain a copy of the License at
  * *
  * * http://www.apache.org/licenses/LICENSE-2.0
  * *
  * * Unless required by applicable law or agreed to in writing, software
  * * distributed under the License is distributed on an "AS IS" BASIS,
  * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * * See the License for the specific language governing permissions and
  * * limitations under the License.
  * *
  * * The use of the Apache License does not indicate that this project is
  * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class PausableThreadPoolExecutor extends ThreadPoolExecutor {

 	private static final String FAILED_URI_TOKEN = "FAILED#";
	private boolean isPaused;
    private final ReentrantLock pauseLock = new ReentrantLock();
    private final Condition unpaused = pauseLock.newCondition();
    private TopUriList topUriList;
    private List<String> failedUris;
    
    private final ThreadLocal<Long> startTime = new ThreadLocal<Long>();
    private final ThreadLocal<String> threadName = new ThreadLocal<String>();
    private final AtomicLong totalTime = new AtomicLong();
    public PausableThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                Executors.defaultThreadFactory(), handler);
        this.topUriList = new TopUriList(5);
        
    }
    public PausableThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler,
            Integer numUrisToCapture) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                Executors.defaultThreadFactory(), handler);
        this.topUriList = new TopUriList(numUrisToCapture);
        
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        threadName.set(Thread.currentThread().getName());
        pauseLock.lock();
        try {
            while (isPaused) {
                unpaused.await();
            }
        } catch (InterruptedException ie) {
            t.interrupt();
        } finally {
            pauseLock.unlock();
            startTime.set(System.nanoTime());
        }
    }

	@Override
 	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		try{
			String result = Thread.currentThread().getName();
			Thread.currentThread().setName(threadName.get());
			Boolean failed = result !=null && result.toUpperCase().startsWith(FAILED_URI_TOKEN);
			if(failed){
				if(failedUris==null){
					failedUris = new ArrayList<String>();
				}
				String[] tokens=result.split(FAILED_URI_TOKEN);
				if(tokens.length>1 && tokens[1].length()>0){
					failedUris.add(tokens[1]);
				}
			}
			else if(result !=null){
	        	long endTime = System.nanoTime();
	     		long taskTime = endTime - startTime.get();
	     		long durationInMs = TimeUnit.MILLISECONDS.convert(taskTime, TimeUnit.NANOSECONDS);
	     		
	            this.topUriList.add(result,durationInMs);
	     		totalTime.addAndGet(taskTime); 
	         }
     	}
     	catch(Exception e) {
     		//Ignore
     		e.printStackTrace();
     	}
 	}
 
	public Map<String,Long> getTopUris() {
		return topUriList.getData();
	}
    public Long getTotalTime() {
 		return totalTime.longValue();
 	}

    public boolean isRunning() {
        return !isPaused;
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void pause() {
        pauseLock.lock();
        try {
            isPaused = true;
        } finally {
            pauseLock.unlock();
        }
    }

    public void resume() {
        pauseLock.lock();
        try {
            isPaused = false;
            unpaused.signalAll();
        } finally {
            pauseLock.unlock();
        }
    }
	/**
	 * @return the failedUris
	 */
	public List<String> getFailedUris() {
		return failedUris;
	}
}

class TopUriList{
	class UriObject implements Comparable<UriObject>{
		@Override
		public String toString() {
			return "UriObject [uri=" + uri + ", timeTaken=" + timeTaken + "]";
		}
		String uri;
		Long timeTaken;
		public UriObject(String uri, Long timeTaken) {
			super();
			this.uri = uri;
			this.timeTaken = timeTaken;
		}
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof UriObject){
				UriObject o = (UriObject)obj;
				if(this.timeTaken!=null && o.timeTaken!=null) {
					return this.timeTaken.compareTo(o.timeTaken) == 0;
				}
				else{
					return false;
				}
			}
			else return super.equals(obj);
		}
		@Override
		public int compareTo(UriObject o) {
			if(this.timeTaken!=null && o.timeTaken!=null) {
				return this.timeTaken.compareTo(o.timeTaken);
			}
			else{
				return 0;//should never get here
			}
		}
	}
	TreeSet<UriObject>  list = null;

	public TopUriList(int size) {
		this.size = size;
		list = new TreeSet<UriObject>() {
			private static final long serialVersionUID = 1L;
			public String toString() {
				StringBuffer strBuff = new StringBuffer();
				for (UriObject o : this) {
					strBuff.append(o.toString());
				}
				return strBuff.toString();
			}
		};
	}
	int size=0;
	Map<String,Long> getData(){
		Map<String,Long> map = new HashMap<String,Long>();
		for(UriObject obj:this.list){
			map.put(obj.uri, obj.timeTaken);
		}
		return map;
	}
	void add(String uri, Long timeTaken){
		UriObject newObj=new UriObject(uri, timeTaken);
		if(list.size()<this.size || list.last().compareTo(newObj) <1){
			synchronized (list) {
				if(list.size()>=this.size ){
					for(int i=0; i<=list.size()-this.size; i++){
						list.remove(list.first());						
					}
				}
				list.add(newObj);
			}
		}
	}
}

