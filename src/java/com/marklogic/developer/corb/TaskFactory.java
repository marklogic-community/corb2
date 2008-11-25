/*
 * Copyright (c)2005-2008 Mark Logic Corporation
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

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class TaskFactory {
    protected ContentSource contentSource = null;

    protected String moduleUri = null;

    /**
     * @param _cs
     * @param _uri
     */
    public TaskFactory(ContentSource _cs, String _uri) {
        contentSource = _cs;
        moduleUri = _uri;
    }

    /**
     * @param _uri
     * @return
     */
    public Transform newTask(String _uri) {
        if (null == contentSource) {
            throw new NullPointerException("null content source");
        }
        if (null == moduleUri) {
            throw new NullPointerException("null module uri");
        }
        if (null == _uri) {
            throw new NullPointerException("null uri");
        }

        // pass a reference to this factory, for later
        return new Transform(this, _uri);
    }

    /**
     * @return
     */
    public String getModuleUri() {
        return moduleUri;
    }

    /**
     * @return
     */
    public Session newSession() {
        return contentSource.newSession();
    }
}
