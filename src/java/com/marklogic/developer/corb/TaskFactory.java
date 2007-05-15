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

import com.marklogic.xcc.ContentSource;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class TaskFactory {
    private static ContentSource contentSource = null;

    private static String moduleUri = null;

    /**
     * @param uri
     * @return
     */
    public static Task newTask(String uri) {
        if (null == contentSource) {
            throw new NullPointerException("null content source");
        }
        if (null == moduleUri) {
            throw new NullPointerException("null module uri");
        }
        if (null == uri) {
            throw new NullPointerException("null uri");
        }

        // Session isn't threadsafe, so we create one per task
        return new Task(new Transform(contentSource.newSession(), uri,
                moduleUri));
    }

    /**
     * @param _uri
     */
    public static void setModuleUri(String _uri) {
        moduleUri = _uri;
    }

    /**
     * @param _cs
     */
    public static void setContentSource(ContentSource _cs) {
        contentSource = _cs;
    }
}
