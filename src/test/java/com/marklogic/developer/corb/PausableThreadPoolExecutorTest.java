/*
  * * Copyright (c) 2004-2020 MarkLogic Corporation
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import java.util.Timer;
import java.util.TimerTask;
import org.junit.Test;

import static com.marklogic.developer.corb.TransformOptions.FAILED_URI_TOKEN;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class PausableThreadPoolExecutorTest {

    @Test
    public void testPauseIsPausedResumeIsRunning() {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor instance = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);

        assertFalse(instance.isPaused());
        assertTrue(instance.isRunning());

        instance.pause();

        assertTrue(instance.isPaused());
        assertFalse(instance.isRunning());

        instance.resume();

        assertFalse(instance.isPaused());
        assertTrue(instance.isRunning());
    }

    @Test
    public void testBeforeExecute() {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        Thread thread = null;
        Runnable runnable = null;
        executor.pause();

        LocalDateTime startedAt = LocalDateTime.now();
        int howLongToWait = 200;
        TimerTask deferResume = new TimerTask() {
                    @Override
                    public void run() {
                        executor.resume();
                    }
                };
        new Timer().schedule(deferResume, howLongToWait);
        executor.beforeExecute(thread, runnable);
        Duration elapsedTime = Duration.between(startedAt, LocalDateTime.now());
        assertTrue(elapsedTime.toMillis() >= howLongToWait);
    }

    @Test
    public void testAfterExecutePassing() {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        Thread thread = mock(Thread.class);
        Thread.currentThread().setName("passing");
        executor.beforeExecute(thread, runnable);
        executor.afterExecute(runnable, throwable);
        assertEquals(1, executor.getNumSucceededUris());
        assertEquals(0, executor.getNumFailedUris());
    }

    @Test
    public void testAfterExecuteFailing() {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        Thread thread = mock(Thread.class);
        Thread.currentThread().setName(FAILED_URI_TOKEN + "foo");
        executor.beforeExecute(thread, runnable);
        executor.afterExecute(runnable, throwable);
        assertEquals(0, executor.getNumSucceededUris());
        assertEquals(1, executor.getNumFailedUris());
    }

    @Test
    public void testAfterExecuteThrowsException() {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        executor.afterExecute(runnable, throwable);
        assertEquals(0, executor.getNumSucceededUris());
        assertEquals(0, executor.getNumFailedUris());
    }

    @Test
    public void testTopURIs() {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        executor.topUriList.setSize(2);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI2", 5L);
        executor.topUriList.add("URI3", 4L);
        executor.topUriList.add("URI4", 3L);
        executor.topUriList.add("URI5", 2L);
        executor.topUriList.add("URI6", 7L);
        executor.topUriList.add("URI7", 1L);
        executor.topUriList.add("URI8", null);
        assertTrue(executor.topUriList.getData().size()==2);
        assertNotNull(executor.topUriList.getData().get("URI1"));
        assertNotNull(executor.topUriList.getData().get("URI6"));
    }

    @Test
    public void testTopUriListSizeZero(){
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        executor.topUriList.setSize(0);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI2", 5L);
        assertTrue(executor.topUriList.getData().size()==0);
    }

    @Test
    public void testTopUriListSizeOne(){
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        PausableThreadPoolExecutor executor = new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler);
        executor.topUriList.setSize(1);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI2", 5L);
        assertTrue(executor.topUriList.getData().size()==1);
        assertNotNull(executor.topUriList.getData().get("URI1"));
    }

}
