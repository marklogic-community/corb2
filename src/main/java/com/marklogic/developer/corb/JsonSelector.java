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

/**
 * Strategy interface for matching logical JSON paths during loader parsing.
 */
public interface JsonSelector {

    /**
     * Returns true when the current logical JSON path matches this selector.
     *
     * @param currentPath current path such as {@code /items/&#42;/uri}
     * @return true when the selector matches
     */
    boolean matches(String currentPath);

    /**
     * Returns the original selector expression.
     *
     * @return selector expression
     */
    String getExpression();
}
