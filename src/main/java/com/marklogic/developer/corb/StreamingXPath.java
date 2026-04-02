/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
 * Compiles XPath expressions into regex patterns for use with streaming XML parsers.
 * <p>
 * StreamingXPath provides a way to evaluate a limited subset of XPath expressions
 * when using streaming XML parsers (SAX or StAX). Since streaming parsers don't load
 * the entire document into memory, they can't use traditional XPath evaluation. This
 * class converts simple XPath expressions into regular expressions that can match
 * element paths as the parser encounters them.
 * </p>
 * <p>
 * <b>Supported XPath Features:</b>
 * </p>
 * <ul>
 * <li>Absolute paths: {@code /root/child/element}</li>
 * <li>Relative paths: {@code element} (treated as {@code //element})</li>
 * <li>Descendant axis: {@code //element} or {@code /descendant::element}</li>
 * <li>Child axis: {@code /child::element} (normalized to {@code /element})</li>
 * <li>Wildcards: {@code /*} or &#47;root&#47;*&#47;element</li>
 * </ul>
 * <p>
 * <b>Unsupported XPath Features:</b>
 * </p>
 * <ul>
 * <li>Predicates: {@code /element[@attr='value']}</li>
 * <li>Reverse axes: {@code ancestor::}, {@code parent::}, {@code preceding::}, etc.</li>
 * <li>Attribute axis: {@code @attribute}</li>
 * <li>Self axis: {@code self::}</li>
 * <li>Functions: {@code count()}, {@code text()}, etc.</li>
 * <li>Operators: {@code and}, {@code or}, etc.</li>
 * <li>Namespace prefixes (planned feature)</li>
 * </ul>
 * <p>
 * <b>How It Works:</b>
 * </p>
 * <ol>
 * <li>XPath is normalized (axes are simplified)</li>
 * <li>Each path step is converted to a regex component</li>
 * <li>Steps are joined to create a complete regex pattern</li>
 * <li>The pattern is used to match element paths during streaming</li>
 * </ol>
 * <p>
 * Example conversions:
 * </p>
 * <pre>
 * XPath: /root/child        → Regex: ^/root/child
 * XPath: //element          → Regex: [^/]*&#47;?element
 * XPath: /root/&#42;&#47;element    → Regex: ^/root/[^&#47;]+/element
 * XPath: /&#42;/&#42;               → Regex: ^/[^&#47;]+/[^&#47;]+
 * </pre>
 * <p>
 * Usage example:
 * </p>
 * <pre>
 * StreamingXPath xpath = new StreamingXPath("/root/child/element");
 *
 * // During streaming parsing, as elements are encountered:
 * String currentPath = "/root/child/element";
 * if (xpath.matches(currentPath)) {
 *     // This is a matching element, process it
 * }
 * </pre>
 * <p>
 * This class is primarily used by:
 * </p>
 * <ul>
 * <li>{@link FileUrisStreamingXMLLoader} - for streaming large XML files</li>
 * <li>{@link FileUrisXMLLoader} - for selecting nodes from XML files</li>
 * <li>Custom loaders that need to process XML in a memory-efficient manner</li>
 * </ul>
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 */
public class StreamingXPath {

    /** Single forward slash character used as XPath separator */
    public static final String SLASH = "/";
    /** Double forward slash representing the descendant axis in XPath shorthand */
    public static final String DOUBLE_SLASH = SLASH + SLASH;
    /** Wildcard character representing any element name in XPath */
    public static final String STAR = "*";
    /** Child axis in XPath verbose syntax (child::) */
    public static final String CHILD_AXIS = "child::";
    /** Descendant axis in XPath verbose syntax (descendant::) */
    public static final String DESCENDANT_AXIS = "descendant::";
    /** Self axis in XPath verbose syntax (self::) */
    public static final String SELF_AXIS = "self::";

    /** The compiled regex pattern derived from the XPath expression, used to match element paths during streaming */
    private Pattern regexPath;
    /** Pattern to extract the local name from an XPath axis notation (captures the element name after the :: axis specifier) */
    private Pattern localNamePattern = Pattern.compile(".*?((?<!::)(?<=:)[^\\[:]+).*?"); //group matches local name from an axis path

    /**
     * Constructs a StreamingXPath with the default pattern   &#47;*&#47;*.
     * <p>
     * This matches any element at the second level of the document hierarchy.
     * Equivalent to: {@code /root/child}
     * </p>
     *
     * @throws CorbException if the default XPath expression is invalid
     */
    public StreamingXPath() throws CorbException {
        String regex = parseXPathToRegex("/*/*");
        regexPath = Pattern.compile(regex);
    }

    /**
     * Constructs a StreamingXPath for the specified XPath expression.
     * <p>
     * The XPath expression is normalized and converted to a regex pattern that
     * can be used for matching during streaming XML parsing.
     * </p>
     *
     * @param xpath the XPath expression to compile (e.g., "/root/element" or "//element")
     * @throws CorbException if the XPath expression is invalid or contains unsupported features
     */
    public StreamingXPath(String xpath) throws CorbException {
        String regex = parseXPathToRegex(xpath);
        regexPath = Pattern.compile(regex);
    }

    /**
     * Normalizes XPath axes to simplified forms suitable for streaming.
     * <p>
     * Normalization rules:
     * </p>
     * <ul>
     * <li>{@code /descendant::foo} → {@code //foo}</li>
     * <li>{@code /child::foo} → {@code /foo}</li>
     * <li>{@code /self::foo} → {@code |foo} (less specific, may produce false positives)</li>
     * <li>Relative paths (no leading /) → prepend {@code //}</li>
     * </ul>
     * <p>
     * Self axis normalization is approximate and may match more than intended.
     * Reverse axes are not normalized and will be rejected by {@link #validateAxis(String)}.
     * </p>
     *
     * @param xpath the XPath expression to normalize
     * @return normalized XPath expression
     */
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
     * Validates that the XPath axis is supported for streaming.
     * <p>
     * Unsupported axes include all reverse axes and some special axes that require
     * knowledge of the document structure beyond what's available in a streaming parser:
     * </p>
     * <ul>
     * <li>ancestor::</li>
     * <li>ancestor-or-self::</li>
     * <li>attribute:: (or @)</li>
     * <li>descendant-or-self::</li>
     * <li>following::</li>
     * <li>following-sibling::</li>
     * <li>namespace::</li>
     * <li>parent:: (or ..)</li>
     * <li>preceding::</li>
     * <li>preceding-sibling::</li>
     * <li>sibling::</li>
     * <li>self::</li>
     * </ul>
     * <p>
     * TODO: Consider allowing predicates (currently ignored) or throwing errors for them.
     * </p>
     *
     * @param axis the XPath axis or step to validate
     * @throws CorbException if the axis is not supported for streaming
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
            if (axis != null && axis.contains(unsupportedAxis)) {
                throw new CorbException("XPath axis " + axis + " is not supported and cannot be used with StreamingXPath");
            }
        }
    }

    /**
     * Parses an XPath expression into a regular expression pattern.
     * <p>
     * Conversion algorithm:
     * </p>
     * <ol>
     * <li>Normalize axes (simplify axis syntax)</li>
     * <li>Split the path into steps (by /)</li>
     * <li>For each step:
     *   <ul>
     *   <li>Extract the local name (if axis notation is used)</li>
     *   <li>Validate the axis is supported</li>
     *   <li>Convert wildcards ({@code *}) to regex: {@code [^/]+}</li>
     *   <li>Convert descendants ({@code //}) to regex:  [^/]*&#47;?</li>
     *   </ul>
     * </li>
     * <li>Join steps with / separators</li>
     * <li>Anchor absolute paths with ^</li>
     * </ol>
     * <p>
     * Example conversions:
     * </p>
     * <pre>
     * /root/child          → ^/root/child
     * //element            → [^&#47;]*&#47;?element
     * /root/&#42;/child       → ^/root/[^&#47;]+/child
     * /root//descendant    → ^/root/[^&#47;]*&#47;?descendant
     * </pre>
     * <p>
     * TODO: Add namespace support via XML_PATH_NAMESPACES option with QNames in XPaths.
     * </p>
     *
     * @param xpath the XPath expression to parse
     * @return a regex pattern string equivalent to the XPath
     * @throws CorbException if the XPath contains unsupported features
     */
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

    /**
     * Tests whether the given element path matches this XPath pattern.
     * <p>
     * The element path should be the absolute path to an element as it's encountered
     * during streaming parsing, in the form: {@code /root/child/element}
     * </p>
     * <p>
     * Example:
     * </p>
     * <pre>
     * StreamingXPath xpath = new StreamingXPath("/root/child");
     * xpath.matches("/root/child");          // true
     * xpath.matches("/root/child/element");  // false
     * xpath.matches("/root");                // false
     * </pre>
     *
     * @param xpath the element path to test (e.g., "/root/child/element")
     * @return true if the path matches this XPath pattern, false otherwise
     */
    public boolean matches(String xpath) {
        return regexPath.matcher(xpath).matches();
    }

    /**
     * Gets the regex pattern string used for matching.
     * <p>
     * This is useful for debugging or logging to see how the XPath was converted
     * to a regex pattern.
     * </p>
     *
     * @return the compiled regex pattern as a string
     */
    public String getRegexPathPattern() {
        return regexPath.pattern();
    }

}
