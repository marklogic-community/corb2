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

import java.util.concurrent.FutureTask;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public class Task extends FutureTask {

    private Transform transform;

    /**
     * @param arg0
     */
    public Task(Transform transform) {
        super(transform);
        this.transform = transform;
    }

    public Transform getTransform() {
        return transform;
    }

}
