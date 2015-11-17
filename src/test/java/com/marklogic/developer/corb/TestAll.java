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
    ExportToFileTaskTest.class,
    HostKeyDecrypterTest.class,
	ManagerTest.class,
	ModuleExecutorTest.class,
    PrivateKeyDecrypterTest.class,
    TwoWaySSLConfigTest.class
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
