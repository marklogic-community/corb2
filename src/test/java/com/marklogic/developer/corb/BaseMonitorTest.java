/*
 * * Copyright (c) 2004-2023 MarkLogic Corporation
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

import org.junit.Test;

import static org.junit.Assert.*;

public class BaseMonitorTest {

    @Test
    public void formatTransactionsPerSecond() {
        Manager manager = new Manager();
        BaseMonitor monitor = new BaseMonitor(manager);
        assertEquals("0.54", monitor.formatTransactionsPerSecond(0.54321));
        assertEquals("5", monitor.formatTransactionsPerSecond(5.4321));
        assertEquals("54", monitor.formatTransactionsPerSecond(54.321));
        assertEquals("543", monitor.formatTransactionsPerSecond(543.21));
        assertEquals("5,432", monitor.formatTransactionsPerSecond(5432.1));
        assertEquals("54,321", monitor.formatTransactionsPerSecond(54321));
    }

    @Test
    public void formatTransactionsPerSecondWithoutGrouping() {
        Manager manager = new Manager();
        BaseMonitor monitor = new BaseMonitor(manager);
        assertEquals("0.54", monitor.formatTransactionsPerSecond(0.54321, false));
        assertEquals("5", monitor.formatTransactionsPerSecond(5.4321, false));
        assertEquals("54", monitor.formatTransactionsPerSecond(54.321, false));
        assertEquals("543", monitor.formatTransactionsPerSecond(543.21, false));
        assertEquals("5432", monitor.formatTransactionsPerSecond(5432.1, false));
        assertEquals("54321", monitor.formatTransactionsPerSecond(54321, false));
    }
}
