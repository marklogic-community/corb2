/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import com.marklogic.xcc.ResultSequence;

/**
 * Default task implementation for executing XQuery or JavaScript modules.
 * <p>
 * Transform is the simplest task implementation in CoRB. It executes a module
 * against URIs but does not process the result sequence beyond basic validation.
 * This task is used when:
 * </p>
 * <ul>
 * <li>No custom PROCESS-TASK, PRE-BATCH-TASK, POST-BATCH-TASK, or INIT-TASK is specified</li>
 * <li>The module performs all necessary operations (inserts, updates, deletes)</li>
 * <li>No result processing or file export is needed</li>
 * <li>Side effects of the module execution are the primary goal</li>
 * </ul>
 * <p>
 * <b>Typical Use Cases:</b>
 * </p>
 * <ul>
 * <li>Updating documents in place (no export needed)</li>
 * <li>Performing transformations that write results back to the database</li>
 * <li>Executing operations for side effects (logging, audit, notifications)</li>
 * <li>Simple batch operations that don't require custom Java logic</li>
 * </ul>
 * <p>
 * <b>What Transform Does:</b>
 * </p>
 * <ol>
 * <li>Obtains an XCC session from the content source pool</li>
 * <li>Creates a request for the configured module</li>
 * <li>Sets the URI(s) as an external variable</li>
 * <li>Sets any custom input variables</li>
 * <li>Executes the module in MarkLogic</li>
 * <li>Returns success (no result processing)</li>
 * </ol>
 * <p>
 * <b>Contrast with Other Task Classes:</b>
 * </p>
 * <ul>
 * <li>{@link ExportToFileTask} - Writes results to individual files per URI</li>
 * <li>{@link ExportBatchToFileTask} - Writes results to a single batch file</li>
 * <li>{@link PreBatchUpdateFileTask} - Initializes export files with headers</li>
 * <li>{@link PostBatchUpdateFileTask} - Finalizes export files (sorting, compression)</li>
 * <li>Custom tasks - Implement application-specific result processing</li>
 * </ul>
 * <p>
 * Example configuration (using Transform implicitly):
 * </p>
 * <pre>
 * # Transform is used automatically when no PROCESS-TASK is specified
 * PROCESS-MODULE=update-documents.xqy
 * URIS-MODULE=get-uris.xqy
 * THREAD-COUNT=10
 * </pre>

 * <p>
 * Example XQuery module that works with Transform:
 * </p>
 * <pre>{@code
 * xquery version "1.0-ml";
 * declare variable $URI as xs:string external;
 *
 * xdmp:document-set-property(
 *   $URI,
 *   <property name="processed">{fn:current-dateTime()}</property>
 * )
 * }</pre>
 * <p>
 * The module performs the update operation, and Transform simply confirms success.
 * </p>
 * <p>
 * <b>Note:</b> While Transform doesn't process results, the module's return value
 * is still validated to ensure the query executed successfully. Exceptions thrown
 * by the module are propagated according to the FAIL-ON-ERROR setting.
 * </p>
 *
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see AbstractTask
 * @see ExportToFileTask
 * @see ExportBatchToFileTask
 * @see Options#PROCESS_TASK
 * @see Options#PROCESS_MODULE
 */
public class Transform extends AbstractTask {

	/**
	 * Processes the result sequence from module execution.
	 * <p>
	 * This implementation does not process the result sequence. It simply returns
	 * success ("true") to indicate that the module executed without throwing an
	 * exception. This is appropriate when:
     * </p>
	 * <ul>
	 * <li>The module performs all necessary operations internally</li>
	 * <li>No result data needs to be captured or exported</li>
	 * <li>The side effects of module execution are what matter</li>
	 * </ul>
	 * <p>
	 * Subclasses that need to process results should override this method to:
     * </p>
	 * <ul>
	 * <li>Extract data from the result sequence</li>
	 * <li>Write results to files</li>
	 * <li>Perform additional transformations</li>
	 * <li>Aggregate results</li>
	 * </ul>
	 * <p>
	 * The result sequence is closed automatically by {@link AbstractTask#call()}
	 * after this method returns.
	 * </p>
	 *
	 * @param seq the result sequence from module execution (not used by this implementation)
	 * @return "true" to indicate successful execution
	 * @throws CorbException if result processing fails (not thrown by this implementation)
	 * @see AbstractTask#call()
	 * @see ExportToFileTask#processResult(ResultSequence)
	 * @see ExportBatchToFileTask#processResult(ResultSequence)
	 */
	@Override
	protected String processResult(ResultSequence seq) throws CorbException {
		return TRUE;
	}

}
