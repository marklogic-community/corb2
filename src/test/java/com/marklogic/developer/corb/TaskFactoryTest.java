/*
 * * Copyright 2005-2015 MarkLogic Corporation
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TaskFactoryTest {

    public TaskFactoryTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of newProcessTask method, of class TaskFactory.
     */
    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullManager() {
        System.out.println("newProcessTask");
        String[] uris = null;
        TaskFactory instance = new TaskFactory(null);
        Task result = instance.newProcessTask(uris);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullPorcessTask() {
        System.out.println("newProcessTask");
        String[] uris = null;
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullUrisAndNullContentSource() {
        System.out.println("newProcessTask");
        String[] uris = null;
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullInputUriWithContentSourceAndModule() {
        System.out.println("newProcessTask");
        String[] uris = null;
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewProcessTask_StringArr() {
        System.out.println("newProcessTask");
        String[] uris = new String[]{"foo"};
        Manager manager = new Manager();
        manager.options.setProcessTaskClass(ExportBatchToFileTask.class);
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_emptyUris() {
        System.out.println("newProcessTask");
        String[] uris = new String[]{};
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        manager.contentSource = mock(ContentSource.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
    }

    @Test
    public void testNewProcessTask_StringArr_withProcessModuleAndContentSource() {
        System.out.println("newProcessTask");
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
        System.out.println("newProcessTask");
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.contentSource = mock(ContentSource.class);

        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);

        assertNotNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewProcessTask_StringArr_nullContentSource() {
        System.out.println("newProcessTask");
        String[] uris = new String[]{"a"};
        Manager manager = new Manager();
        manager.options.setProcessModule("module");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newProcessTask(uris);
        assertNotNull(result);
    }

    /**
     * Test of newProcessTask method, of class TaskFactory.
     */
    @Test
    public void testNewProcessTask_StringArr_boolean() {
        System.out.println("newProcessTask");
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
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPreBatchTask();
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPreBatchTask_onlyPreBatchTaskClass() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
    }

    @Test
    public void testNewPreBatchTask_withClassModuleAndSource() {
        System.out.println("newPreBatchTask");
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
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPreBatchModule("foo");

        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPreBatchTask_withPreBatchTaskClassAndContent() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPreBatchTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPreBatchTask();
    }

    @Test(expected = NullPointerException.class)
    public void testNewPreBatchTask() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPreBatchModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPreBatchTask();
    }

    /**
     * Test of newPostBatchTask method, of class TaskFactory.
     */
    @Test
    public void testNewPostBatchTask_noPostbatchTaskClassOrModule() {
        System.out.println("newPostBatchTask");
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newPostBatchTask();
        assertNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void testNewPostBatchTask_onlyPostBatchTaskClass() {
        System.out.println("newPostBatchTask");
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(null);
        Task result = instance.newPostBatchTask();
    }

    @Test(expected = NullPointerException.class)
    public void testNewPostBatchTask_noPostBatchModuleAndContent() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPostBatchModule("foo");

        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPostBatchTask_withClassModuleAndSource() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.options.setPostBatchModule("foo");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewPostBatchTask() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();
        manager.options.setPostBatchTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        instance.newPostBatchTask();
    }

    /**
     * Test of newInitTask method, of class TaskFactory.
     */
    @Test
    public void testNewInitTask_noPostbatchTaskClassOrModule() {
        System.out.println("newInitTask");
        Manager manager = new Manager();
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask_withInitTaskClassOnly() {
        System.out.println("newInitTask");
        Manager manager = new Manager();
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTask_withInitModuleOnly() {
        System.out.println("newInitTask");
        Manager manager = new Manager();
        manager.options.setInitModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTask_withTaskClassAndInitModule() {
        System.out.println("newInitTask");
        Manager manager = new Manager();
        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.options.setInitModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
    }

    @Test(expected = NullPointerException.class)
    public void testNewInitTask_withInitModule() {
        System.out.println("newInitTask");
        Manager manager = new Manager();
        manager.options.setInitModule("foo");
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
    }

    @Test
    public void testNewInitTask_withInitModuleAndContentSource() {
        System.out.println("newInitTask");
        Manager manager = new Manager();
        manager.options.setInitModule("foo");
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewInitTask() {
        System.out.println("newPreBatchTask");
        Manager manager = new Manager();

        manager.options.setInitTaskClass(ExportBatchToFileTask.class);
        manager.contentSource = mock(ContentSource.class);
        TaskFactory instance = new TaskFactory(manager);
        Task result = instance.newInitTask();
        assertNotNull(result);
    }
}
