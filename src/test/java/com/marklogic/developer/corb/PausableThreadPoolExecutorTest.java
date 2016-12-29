/*
  * * Copyright (c) 2004-2017 MarkLogic Corporation
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
}
