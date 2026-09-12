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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class BaseMonitorTest {

    private BaseMonitor newMonitor() {
        return new BaseMonitor(null);
    }

    private BaseMonitor newMonitor(int numTpsForEtc) {
        Manager manager = mock(Manager.class);
        TransformOptions options = new TransformOptions();
        options.setNumTpsForETC(numTpsForEtc);
        when(manager.getOptions()).thenReturn(options);
        return new BaseMonitor(manager);
    }

    @Test
    void formatTransactionsPerSecond() {
        assertEquals("0.54", BaseMonitor.formatTransactionsPerSecond(0.54321));
        assertEquals("5", BaseMonitor.formatTransactionsPerSecond(5.4321));
        assertEquals("54", BaseMonitor.formatTransactionsPerSecond(54.321));
        assertEquals("543", BaseMonitor.formatTransactionsPerSecond(543.21));
        assertEquals("5,432", BaseMonitor.formatTransactionsPerSecond(5432.1));
        assertEquals("54,321", BaseMonitor.formatTransactionsPerSecond(54321));
    }

    @Test
    void formatTransactionsPerSecondWithoutGrouping() {
        assertEquals("0.54", BaseMonitor.formatTransactionsPerSecond(0.54321, false));
        assertEquals("5", BaseMonitor.formatTransactionsPerSecond(5.4321, false));
        assertEquals("54", BaseMonitor.formatTransactionsPerSecond(54.321, false));
        assertEquals("543", BaseMonitor.formatTransactionsPerSecond(543.21, false));
        assertEquals("5432", BaseMonitor.formatTransactionsPerSecond(5432.1, false));
        assertEquals("54321", BaseMonitor.formatTransactionsPerSecond(54321, false));
    }

    @Test
    void isZeroDetectsOnlyExactZero() {
        assertTrue(BaseMonitor.isZero(0d));
        assertFalse(BaseMonitor.isZero(0.0000001d));
        assertFalse(BaseMonitor.isZero(-0.0000001d));
    }

    @Test
    void calculateTransactionsPerSecondUsesIntervalDelta() {
        assertEquals(3d, BaseMonitor.calculateTransactionsPerSecond(5, 2, 1000, 0), 0.0001d);
        assertEquals(4d, BaseMonitor.calculateTransactionsPerSecond(4, 0, 1000, 0), 0.0001d);
    }

    @Test
    void getEstimatedTimeCompletionHandlesZeroAndPausedValues() {
        assertEquals("00:00:-1", BaseMonitor.getEstimatedTimeCompletion(100, 50, 0d, false));
        assertEquals("00:00:-1 (paused)", BaseMonitor.getEstimatedTimeCompletion(100, 50, 0d, true));
        assertEquals("00:00:50", BaseMonitor.getEstimatedTimeCompletion(100, 50, 1d, false));
    }

    @Test
    void getProgressMessageIncludesFailureCount() {
        String actual = BaseMonitor.getProgressMessage(10, 100, 4, 3, "00:00:15", 2, 2);
        assertEquals("10/100, 2 tasks failed, 4 tps(avg), 3 tps(cur), ETC 00:00:15, 2 active threads.", actual);
    }

    @Test
    void constructorUsesConfiguredNumberOfSamples() {
        BaseMonitor monitor = newMonitor(3);
        assertEquals(3, monitor.numTpsForEtc);
        assertTrue(monitor.tpsForETCList.isEmpty());
    }

    @Test
    void calculateTpsForEtcUsesSlidingWindowAndClearsOnPausedZero() {
        BaseMonitor monitor = newMonitor(2);
        assertEquals(1d, monitor.calculateTpsForETC(1d, false), 0.0001d);
        assertEquals(1.5d, monitor.calculateTpsForETC(2d, false), 0.0001d);
        assertEquals(2.5d, monitor.calculateTpsForETC(3d, false), 0.0001d);

        monitor.calculateTpsForETC(0d, true);
        assertTrue(monitor.tpsForETCList.isEmpty());
    }
}
