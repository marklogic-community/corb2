/*
 * Copyright 2005-2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * The class <code>TestAll</code> builds a suite that can be used to run all
 * of the tests within its package as well as within any subpackages of its
 * package.
 *
 * @generatedBy CodePro at 9/18/15 12:45 PM
 * @author matthew.heckel
 * @version $Revision: 1.0 $
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AbstractDecrypterTest.class,
    AbstractManagerTest.class,
    AbstractTaskTest.class,
    ExportToFileTaskTest.class,
    FileUrisLoaderTest.class,
    HostKeyDecrypterTest.class,
    JasyptDecrypterTest.class,
	ManagerTest.class,
	ModuleExecutorTest.class,
    PostBatchUpdateFileTaskTest.class,
    PreBatchUpdateFileTaskTest.class,
    PrivateKeyDecrypterTest.class,
    TaskFactoryTest.class,
    TrustAnyoneSSLConfigTest.class,
    TwoWaySSLConfigTest.class,
    com.marklogic.developer.corb.util.DateUtilsTest.class,
    com.marklogic.developer.corb.util.FileUtilsTest.class,
    com.marklogic.developer.corb.util.IOUtilsTest.class,
    com.marklogic.developer.corb.util.StringUtilsTest.class
})
public class TestAll {

	/**
	 * Launch the test.
	 *
	 * @param args the command line arguments
	 *
	 * @generatedBy CodePro at 9/18/15 12:45 PM
	 */
	public static void main(String[] args) {
		JUnitCore.runClasses(new Class[] { TestAll.class });
	}
}
