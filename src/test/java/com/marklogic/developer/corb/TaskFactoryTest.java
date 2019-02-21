/*
 * * Copyright (c) 2004-2019 MarkLogic Corporation
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

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TaskFactoryTest {

    private static final Logger LOG = Logger.getLogger(TaskFactoryTest.class.getName());
    private static final String MODULE = "module";

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrNullManager() {
        String[] uris = null;
        TaskFactory instance = new TaskFactory(null);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrNullPorcessTask() {
        String[] uris = null;
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrNullUrisAndNullContentSource() {
        String[] uris = null;
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrNullInputUriWithContentSourceAndModule() {
        String[] uris = null;
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewProcessTaskStringArr() {
        String[] uris = new String[]{MODULE};
        Manager manager = new Manager();
        manager.options.setProcessTaskClass(ExportBatchToFileTask.class);
        manager.options.setProcessModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrEmptyUris() {
        String[] uris = new String[]{};
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);

        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test
    public void testNewProcessTaskStringArrWithProcessModuleAndContentSource() {
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertNotNull(result);
    }

    @Test
    public void testCustomTimeZone() {
        String[] uris = new String[]{"testCustomTimeZone"};
        String timeZoneID = "Africa/Conakry";
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        manager.getProperties().setProperty(Options.XCC_TIME_ZONE, timeZoneID);
        manager.csp = mock(ContentSourcePool.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertEquals(timeZoneID, ((AbstractTask) result).timeZone.getID());
    }

    @Test
    public void testDefaultTimeZone() {
        String[] uris = new String[]{"testDefaultTimeZone"};
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertNull(((AbstractTask) result).timeZone);
    }

    @Test
    public void testInvalidTimeZone() {
        String[] uris = new String[]{"testInvalidTimeZone"};
        String timeZoneID = "moon/darkside";
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        manager.getProperties().setProperty(Options.XCC_TIME_ZONE, timeZoneID);
        manager.csp = mock(ContentSourcePool.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertEquals(TimeZone.getTimeZone("GMT"), ((AbstractTask) result).timeZone);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrNullProcessModule() {
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.csp = mock(ContentSourcePool.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);

        assertNotNull(result);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTaskStringArrNullContentSource() {
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.options.setProcessModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertNotNull(result);
        fail();
    }

    @Test
    public void testNewProcessTaskStringArrBoolean() {
        String[] uris = new String[]{"a"};
        boolean failOnError = false;
        Manager manager = new Manager();
        manager.options.setProcessModule("mod-print-uri.sjs|ADHOC");
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris, failOnError);
        assertNotNull(result);
    }

    @Test
    public void testNewPreBatchTaskNoBatchTaskOrModule() {
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPreBatchTask();
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPreBatchTaskOnlyPreBatchTaskClass() {
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
        fail();
    }

    @Test
    public void testNewPreBatchTaskWithClassModuleAndSource() {
        Manager manager = new Manager();
        manager.options.setPreBatchModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPreBatchTask();
        assertNotNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewPreBatchTaskNoPreBatchModuleAndContent() {
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPreBatchModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPreBatchTaskWithPreBatchTaskClassAndContent() {
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewPreBatchTask() {
        Manager manager = new Manager();
        manager.options.setPreBatchModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
    }

    @Test
    public void testNewPostBatchTaskNoPostbatchTaskClassOrModule() {
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPostBatchTask();
        assertNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewPostBatchTaskOnlyPostBatchTaskClass() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(null);
        instance.newPostBatchTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewPostBatchTaskNoPostBatchModuleAndContent() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPostBatchModule(MODULE);

        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPostBatchTaskWithClassModuleAndSource() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPostBatchModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPostBatchTask() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
        fail();
    }

    @Test
    public void testNewInitTaskNoPostbatchTaskClassOrModule() {
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTaskWithInitTaskClassOnly() {
        Manager manager = new Manager();
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTaskWithInitModuleOnly() {
        Manager manager = new Manager();
        manager.options.setInitModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTaskWithTaskClassAndInitModule() {
        Manager manager = new Manager();
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.options.setInitModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTaskWithInitModule() {
        Manager manager = new Manager();
        manager.options.setInitModule(MODULE);
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test
    public void testNewInitTaskWithInitModuleAndContentSource() {
        Manager manager = new Manager();
        manager.options.setInitModule(MODULE);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask() {
        Manager manager = new Manager();

        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTaskEmptyModule() {
        try {
            Manager manager = new Manager();
            File emptyModule = File.createTempFile("testNewInitTask", "txt");
            emptyModule.deleteOnExit();
            manager.options.setInitModule(emptyModule.getAbsolutePath() + "|ADHOC");
            manager.options.setInitTaskClass(ExportBatchToFileTask.class);
            manager.csp = mock(ContentSourcePool.class);
            TaskFactory instance = new TaskFactory(manager);
            Task result = instance.newInitTask();
            assertNotNull(result);
            fail();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTaskInline() {
        Manager manager = new Manager();
        manager.options.setInitModule("INLINE-XQUERY|for $i in (1 to 5) $i");
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTaskInlineIsEmpty() {
        Manager manager = new Manager();
        manager.options.setInitModule("INLINE-JAVASCRIPT|");
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.csp = mock(ContentSourcePool.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }
}
