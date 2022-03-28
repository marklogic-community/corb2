/*
  * * Copyright (c) 2004-2022 MarkLogic Corporation
  * *
  * * Licensed under the Apache License, Version 2.0 (the "License");
  * * you may not use this file except in compliance with the License.
  * * You may obtain a copy of the License at
  * *
  * * http://www.apache.org/licenses/LICENSE-2.0
  * *
  * * Unless required by applicable law or agreed to in writing, software
  * * distributed under the License is distributed on an "AS IS" BASIS,
  * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * * See the License for the specific language governing permissions and
  * * limitations under the License.
  * *
  * * The use of the Apache License does not indicate that this project is
  * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles an XPath expression into a regex pattern that is used to evaluate
 * matches for an XPath. To be used by a streaming parser, such as SAX or StAX,
 * in order to evaluate a limited subset of XPath expressions for selection of nodes.
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 */
public class StreamingXPath {

    public static final String SLASH = "/";
    public static final String DOUBLE_SLASH = SLASH + SLASH;
    public static final String STAR = "*";
    public static final String CHILD_AXIS = "child::";
    public static final String DESCENDANT_AXIS = "descendant::";
    public static final String SELF_AXIS = "self::";

    private Pattern regexPath;
    private Pattern localNamePattern = Pattern.compile(".*?((?<!::)(?<=:)[^\\[:]+).*?"); //group matches local name from an axis path

    public StreamingXPath() throws CorbException {
        String regex = parseXPathToRegex("/*/*");
        regexPath = Pattern.compile(regex);
    }

    public StreamingXPath(String xpath) throws CorbException {
        String regex = parseXPathToRegex(xpath);
        regexPath = Pattern.compile(regex);
    }

    protected String normalizeAxes(final String xpath) {
        String normalizedXPath = xpath;
        // replace axis /descendant::foo with //foo
        if (normalizedXPath.contains(DESCENDANT_AXIS)) {
            normalizedXPath = normalizedXPath.replaceAll(SLASH + DESCENDANT_AXIS, DOUBLE_SLASH);
        }
        // replace /child::foo with "/foo"
        if (normalizedXPath.contains(CHILD_AXIS)) {
            normalizedXPath = normalizedXPath.replaceAll(SLASH + CHILD_AXIS, SLASH);
        }
        // /*/self::foo == /*|foo will be less specific and grab false-positives, but better than nothing
        if (normalizedXPath.contains(SLASH + SELF_AXIS)) {
            normalizedXPath = normalizedXPath.replaceAll(SLASH + SELF_AXIS, "|");
        }
        // if match pattern specified (only an element name or partial), then prepend //
        if (!normalizedXPath.startsWith(SLASH)) {
            normalizedXPath = DOUBLE_SLASH + normalizedXPath;
        }
        return normalizedXPath;
    }

    /**
     * Ensure that non-streamable axes are not used.
     *
     * TODO: allow predicates, since we ignore them? Or throw errors for predicates as well?
     *
     * @param axis
     * @throws com.marklogic.developer.corb.CorbException
     */
    protected void validateAxis(String axis) throws CorbException {
        String[] unsupportedAxes = new String[]{
            "ancestor::",
            "ancestor-or-self::",
            "attribute::",
            "descendant-or-self::",
            "following::",
            "following-sibling::",
            "namespace::",
            "parent::",
            "..",
            "preceding::",
            "preceding-sibling::",
            "sibling::",
            SELF_AXIS
        };
        for (String unsupportedAxis : unsupportedAxes) {
            if (axis.contains(unsupportedAxis)) {
                throw new CorbException("XPath axis " + axis + " is not supported and cannot be used with StreamingXPath");
            }
        }
    }

    protected String parseXPathToRegex(String xpath) throws CorbException {
        String normalizedXPath = normalizeAxes(xpath);
        String[] paths = normalizedXPath.split(SLASH);
        StringBuilder regex = new StringBuilder();
        int pathIndex = 0;
        String step;
        for (String path : paths) {
            if (path != null) {
                if (pathIndex == 0 && normalizedXPath.startsWith(SLASH)) {
                    regex
                        .append('^') //anchor to the beginning of the string
                        .append(SLASH); // absolute XPath starting from the root node
                } else {
                    //TODO namespace support with Option.XML_PATH_NAMESPACES=ns1,http://some/uri,ns2,http://other/uri,ns2,http://asdf and QNames in generated XPaths?
                    Matcher localNameMatcher = localNamePattern.matcher(path);
                    step = localNameMatcher.matches() ? localNameMatcher.group(1) : path;
                    validateAxis(step);
                    if (STAR.equals(step)) {
                        step = "[^/]+";
                    }
                    if (step.isEmpty()) {
                        //when "//" descendant XPath is used, everything is optional for this step
                        regex
                            .append("[^/]*/?");
                    } else {
                        regex
                            .append(step)
                            .append(SLASH);
                    }
                }
            }
            pathIndex++;
        }

        regex.setLength(Math.max(regex.length() - 1, 0));//remove the trailing slash
        return regex.toString();
    }

    public boolean matches(String xpath) {
        return regexPath.matcher(xpath).matches();
    }

    public String getRegexPathPattern() {
        return regexPath.pattern();
    }

}
