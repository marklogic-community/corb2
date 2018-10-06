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
        assertEquals("5432", monitor.formatTransactionsPerSecond(5432.1));
        assertEquals("54321", monitor.formatTransactionsPerSecond(54321));
    }
}
