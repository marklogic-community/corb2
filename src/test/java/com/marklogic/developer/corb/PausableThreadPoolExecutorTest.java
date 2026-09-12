/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import static com.marklogic.developer.corb.TransformOptions.FAILED_URI_TOKEN;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class PausableThreadPoolExecutorTest {

    private PausableThreadPoolExecutor newExecutor() {
        return newExecutor(0);
    }

    private PausableThreadPoolExecutor newExecutor(int failedUrisToCapture) {
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);
        TransformOptions options = new TransformOptions();
        options.setNumberOfFailedUris(failedUrisToCapture);
        return new PausableThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, queue, handler, options);
    }

    @SuppressWarnings("unchecked")
    private <T> void setThreadLocalValue(PausableThreadPoolExecutor executor, String fieldName, T value) throws Exception {
        java.lang.reflect.Field field = PausableThreadPoolExecutor.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        ThreadLocal<T> threadLocal = (ThreadLocal<T>) field.get(executor);
        threadLocal.set(value);
    }

    @Test
    void testPauseIsPausedResumeIsRunning() {
        PausableThreadPoolExecutor instance = newExecutor();

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
    void testBeforeExecute() {
        PausableThreadPoolExecutor executor = newExecutor();
        Thread thread = mock(Thread.class);
        Runnable runnable = mock(Runnable.class);
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
    void testAfterExecutePassing() throws Exception {
        PausableThreadPoolExecutor executor = newExecutor();
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        Thread currentThread = Thread.currentThread();
        String originalName = currentThread.getName();
        try {
            currentThread.setName("passing");
            setThreadLocalValue(executor, "threadName", "previous-name");
            setThreadLocalValue(executor, "startTime", System.nanoTime() - 1_000_000L);
            executor.afterExecute(runnable, throwable);
            assertEquals(1, executor.getNumSucceededUris());
            assertEquals(0, executor.getNumFailedUris());
        } finally {
            currentThread.setName(originalName);
        }
    }

    @Test
    void testAfterExecuteFailing() throws Exception {
        PausableThreadPoolExecutor executor = newExecutor(10);
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        Thread currentThread = Thread.currentThread();
        String originalName = currentThread.getName();
        try {
            currentThread.setName(FAILED_URI_TOKEN + "foo");
            setThreadLocalValue(executor, "threadName", "previous-name");
            setThreadLocalValue(executor, "startTime", System.nanoTime() - 1_000_000L);
            executor.afterExecute(runnable, throwable);
            assertEquals(0, executor.getNumSucceededUris());
            assertEquals(1, executor.getNumFailedUris());
            assertEquals(1, executor.getFailedUris().size());
            assertEquals("foo", executor.getFailedUris().get(0));
        } finally {
            currentThread.setName(originalName);
        }
    }

    @Test
    void testAfterExecuteFailsCapturesOnlyConfiguredLimit() {
        PausableThreadPoolExecutor executor = newExecutor(1);
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        Thread thread = mock(Thread.class);
        Thread currentThread = Thread.currentThread();
        String originalName = currentThread.getName();
        try {
            currentThread.setName(FAILED_URI_TOKEN + "first");
            executor.beforeExecute(thread, runnable);
            executor.afterExecute(runnable, throwable);

            currentThread.setName(FAILED_URI_TOKEN + "second");
            executor.beforeExecute(thread, runnable);
            executor.afterExecute(runnable, throwable);

            assertEquals(2, executor.getNumFailedUris());
            assertEquals(1, executor.getFailedUris().size());
            assertEquals("first", executor.getFailedUris().get(0));
        } finally {
            currentThread.setName(originalName);
        }
    }

    @Test
    void testAfterExecuteThrowsException() {
        PausableThreadPoolExecutor executor = newExecutor();
        Runnable runnable = mock(Runnable.class);
        Throwable throwable = mock(Throwable.class);
        executor.afterExecute(runnable, throwable);
        assertEquals(0, executor.getNumSucceededUris());
        assertEquals(0, executor.getNumFailedUris());
    }

    @Test
    void testTopURIs() {
        PausableThreadPoolExecutor executor = newExecutor();
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
        assertEquals(2, executor.topUriList.getData().size());
        assertNotNull(executor.topUriList.getData().get("URI1"));
        assertNotNull(executor.topUriList.getData().get("URI6"));
    }

    @Test
    void testTopUriListSizeZero(){
        PausableThreadPoolExecutor executor = newExecutor();
        executor.topUriList.setSize(0);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI2", 5L);
        assertTrue(executor.topUriList.getData().isEmpty());
    }

    @Test
    void testTopUriListSizeOne(){
        PausableThreadPoolExecutor executor = newExecutor();
        executor.topUriList.setSize(1);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI1", 6L);
        executor.topUriList.add("URI2", 5L);
        assertEquals(1, executor.topUriList.getData().size());
        assertNotNull(executor.topUriList.getData().get("URI1"));
    }

}
