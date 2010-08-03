/*
 * Copyright (c)2005-2010 Mark Logic Corporation
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
public class Transform implements Callable<String> {

    protected String inputUri;

    protected TaskFactory factory;

    /**
     * @param _tf
     * @param _uri
     */
    public Transform(TaskFactory _tf, String _uri) {
        factory = _tf;
        this.inputUri = _uri;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#finalize()
     */
    protected void finalize() throws Throwable {
        super.finalize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public String call() throws Exception {
        // try to avoid thread starvation
        Thread.yield();
        Session session = null;
        try {
            String moduleUri = factory.getModuleUri();
            session = factory.newSession();
            Request request = session.newModuleInvoke(moduleUri);
            request.setNewStringVariable("URI", inputUri);
            return session.submitRequest(request).asString();
        } finally {
            if (null != session) {
                session.close();
            }
        }
    }

    /**
     * @return
     */
    public String getUri() {
        return inputUri;
    }

}
