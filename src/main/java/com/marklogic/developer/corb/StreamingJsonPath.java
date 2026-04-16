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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.marklogic.developer.corb.util.StringUtils.isBlank;

/**
 * Matches a limited, slash-delimited JSON path syntax against logical JSON paths.
 * <p>
 * Supported constructs:
 * </p>
 * <ul>
 *   <li>Absolute paths such as {@code /items/&#42;}</li>
 *   <li>Relative paths such as {@code uri}, treated as {@code //uri}</li>
 *   <li>Descendant matching via {@code //}</li>
 *   <li>Wildcards via {@code *}</li>
 * </ul>
  * @since 2.6.0
 */
public class StreamingJsonPath implements JsonSelector {

    private static final String DESCENDANT = "**";

    private final String expression;
    private final List<String> tokens;

    /**
     * Constructs a StreamingJsonPath with the default selector expression of {@code /*}, which matches all immediate child nodes of the root.
     *
     * @throws CorbException if there is an issue with the default expression (should not occur)
     */
    public StreamingJsonPath() throws CorbException {
        this("/*");
    }

    /**
     * Constructs a StreamingJsonPath with the provided selector expression.
     *
     * @param expression the JSON path selector expression, which can include absolute paths (e.g. "/items/&#42;"), relative paths (e.g. "uri"), descendant matching (e.g. "//name"), and wildcards (e.g. "/items/&#42;/name")
     * @throws CorbException if the expression contains unsupported syntax or is otherwise invalid
     */
    public StreamingJsonPath(String expression) throws CorbException {
        this.expression = normalize(expression);
        this.tokens = Collections.unmodifiableList(parseTokens(this.expression));
    }
    /**
     * Matches the provided JSON path against the selector expression.
     *
     * @param currentPath the JSON path to match, represented as a slash-delimited string (e.g. "/items/0/name")
     * @return true if the currentPath matches the selector expression, false otherwise
     */
    @Override
    public boolean matches(String currentPath) {
        List<String> pathTokens = parsePathTokens(currentPath);
        return matches(0, 0, pathTokens);
    }

    /**
     * Returns the original selector expression.
     *
     * @return the original selector expression
     */
    @Override
    public String getExpression() {
        return expression;
    }

    protected String normalize(String value) throws CorbException {
        String normalized = isBlank(value) ? "/*" : value.trim();
        if (normalized.startsWith("$")) {
            throw new CorbException("JSONPath expressions are not supported by StreamingJsonPath. Use slash-delimited paths such as /items/* or provide an optional JSONPath implementation.");
        }
        validateUnsupportedSyntax(normalized);
        if (!normalized.startsWith("/")) {
            normalized = "//" + normalized;
        }
        return normalized;
    }

    protected void validateUnsupportedSyntax(String value) throws CorbException {
        String[] unsupported = new String[] {"[", "]", "(", ")", "@", "?", ".."};
        for (String token : unsupported) {
            if (value.contains(token)) {
                throw new CorbException("JSON path syntax " + value + " contains unsupported token " + token);
            }
        }
    }

    @Override
    protected final void finalize() throws Throwable {
        super.finalize();
    }

    private static List<String> parseTokens(String value) {
        List<String> parsed = new ArrayList<>();
        int i = 0;
        while (i < value.length()) {
            if (value.charAt(i) == '/') {
                if (i + 1 < value.length() && value.charAt(i + 1) == '/') {
                    parsed.add(DESCENDANT);
                    i += 2;
                } else {
                    i++;
                }
                continue;
            }
            int start = i;
            while (i < value.length() && value.charAt(i) != '/') {
                i++;
            }
            parsed.add(value.substring(start, i));
        }
        return parsed;
    }

    private static List<String> parsePathTokens(String value) {
        if (isBlank(value) || "/".equals(value)) {
            return Collections.emptyList();
        }
        List<String> parsed = new ArrayList<>();
        for (String token : value.split("/")) {
            if (!isBlank(token)) {
                parsed.add(token);
            }
        }
        return parsed;
    }

    private boolean matches(int selectorIndex, int pathIndex, List<String> pathTokens) {
        if (selectorIndex == tokens.size()) {
            return pathIndex == pathTokens.size();
        }

        String selectorToken = tokens.get(selectorIndex);
        if (DESCENDANT.equals(selectorToken)) {
            if (selectorIndex == tokens.size() - 1) {
                return true;
            }
            for (int i = pathIndex; i <= pathTokens.size(); i++) {
                if (matches(selectorIndex + 1, i, pathTokens)) {
                    return true;
                }
            }
            return false;
        }

        if (pathIndex >= pathTokens.size()) {
            return false;
        }

        if ("*".equals(selectorToken) || selectorToken.equals(pathTokens.get(pathIndex))) {
            return matches(selectorIndex + 1, pathIndex + 1, pathTokens);
        }
        return false;
    }
}
