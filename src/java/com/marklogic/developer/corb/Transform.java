/*
 * Copyright (c)2005-2007 Mark Logic Corporation
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

import java.util.concurrent.Callable;

import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Transform implements Callable {

    private Session session;

    private String inputUri;

    private String moduleUri;

    /**
     * @param session
     * @param inputUri
     * @param moduleUri
     */
    public Transform(Session session, String inputUri, String moduleUri) {
        this.session = session;
        this.inputUri = inputUri;
        this.moduleUri = moduleUri;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#finalize()
     */
    protected void finalize() throws Throwable {
        if (session != null) {
            session.close();
        }
        super.finalize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public Object call() throws Exception {
        Request request = session.newModuleInvoke(moduleUri);
        request.setNewStringVariable("URI", inputUri);
        return session.submitRequest(request).asString();
    }

    /**
     * @return
     */
    public String getUri() {
        return inputUri;
    }

}
