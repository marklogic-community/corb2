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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class PausableThreadPoolExecutorTest {

    /**
     * Test of resume method, of class PausableThreadPoolExecutor.
     */
    @Test
    public void testPauseIsPausedResumeIsRunning() {
        System.out.println("pause, isPaused, resume");
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

}
