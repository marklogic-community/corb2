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

import com.marklogic.xcc.ContentSource;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TaskFactoryTest {

    /**
     * Test of newProcessTask method, of class TaskFactory.
     */
    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullManager() {
        String[] uris = null;
        TaskFactory instance = new TaskFactory(null);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullPorcessTask() {
        String[] uris = null;
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullUrisAndNullContentSource() {
        String[] uris = null;
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullInputUriWithContentSourceAndModule() {
        String[] uris = null;
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewProcessTask_StringArr() {
        String[] uris = new String[]{"foo"};
        Manager manager = new Manager();
        manager.options.setProcessTaskClass(ExportBatchToFileTask.class);
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_emptyUris() {
        String[] uris = new String[]{};
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);

        TaskFactory instance = new TaskFactory(manager);
        instance.newProcessTask(uris);
        fail();
    }

    @Test
    public void testNewProcessTask_StringArr_withProcessModuleAndContentSource() {
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertNotNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullProcessModule() {
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.contentSource = mock(ContentSource.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);

        assertNotNull(result);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullContentSource() {
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertNotNull(result);
        fail();
    }

    /**
     * Test of newProcessTask method, of class TaskFactory.
     */
    @Test
    public void testNewProcessTask_StringArr_boolean() {
        String[] uris = new String[]{"a"};
        boolean failOnError = false;
        Manager manager = new Manager();
        manager.options.setProcessModule("mod-print-uri.sjs|ADHOC");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris, failOnError);
        assertNotNull(result);
    }

    /**
     * Test of newPreBatchTask method, of class TaskFactory.
     */
    @Test
    public void testNewPreBatchTask_noBatchTaskOrModule() {
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPreBatchTask();
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPreBatchTask_onlyPreBatchTaskClass() {
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
        fail();
    }

    @Test
    public void testNewPreBatchTask_withClassModuleAndSource() {
        Manager manager = new Manager();
        //manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPreBatchModule("foo");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPreBatchTask();
        assertNotNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewPreBatchTask_noPreBatchModuleAndContent() {
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPreBatchModule("foo");

        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPreBatchTask_withPreBatchTaskClassAndContent() {
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewPreBatchTask() {
        Manager manager = new Manager();
        manager.options.setPreBatchModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
    }

    /**
     * Test of newPostBatchTask method, of class TaskFactory.
     */
    @Test
    public void testNewPostBatchTask_noPostbatchTaskClassOrModule() {
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPostBatchTask();
        assertNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewPostBatchTask_onlyPostBatchTaskClass() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(null);
        instance.newPostBatchTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewPostBatchTask_noPostBatchModuleAndContent() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPostBatchModule("foo");

        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPostBatchTask_withClassModuleAndSource() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPostBatchModule("foo");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPostBatchTask() {
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
        fail();
    }

    /**
     * Test of newInitTask method, of class TaskFactory.
     */
    @Test
    public void testNewInitTask_noPostbatchTaskClassOrModule() {
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask_withInitTaskClassOnly() {
        Manager manager = new Manager();
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTask_withInitModuleOnly() {
        Manager manager = new Manager();
        manager.options.setInitModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTask_withTaskClassAndInitModule() {
        Manager manager = new Manager();
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.options.setInitModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTask_withInitModule() {
        Manager manager = new Manager();
        manager.options.setInitModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        instance.newInitTask();
        fail();
    }

    @Test
    public void testNewInitTask_withInitModuleAndContentSource() {
        Manager manager = new Manager();
        manager.options.setInitModule("foo");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask() {
        Manager manager = new Manager();

        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask_emptyModule() throws IOException {
        Manager manager = new Manager();
        File emptyModule = File.createTempFile("testNewInitTask", "txt");
        emptyModule.createNewFile();
        emptyModule.deleteOnExit();
        manager.options.setInitModule(emptyModule.getAbsolutePath() + "|ADHOC");
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask_inline() {
        Manager manager = new Manager();
        manager.options.setInitModule("INLINE-XQUERY|for $i in (1 to 5) $i");
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask_inline_isEmpty() {
        Manager manager = new Manager();
        manager.options.setInitModule("INLINE-JAVASCRIPT|");
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
        fail();
    }
}
