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

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Streaming JSON extractor that walks a JSON document and emits values whose
 * logical paths match configured selectors.
 */
class JsonExtractor {

    interface ExtractionHandler {
        void onNode(String currentPath, String rawJson) throws IOException, CorbException;

        void onMetadata(String currentPath, String rawJson) throws IOException, CorbException;
    }

    private final JsonSelector nodeSelector;
    private final JsonSelector metadataSelector;
    private final ExtractionHandler handler;
    private boolean metadataCaptured;
    private long extractedNodeCount;

    JsonExtractor(JsonSelector nodeSelector, JsonSelector metadataSelector, ExtractionHandler handler) {
        this.nodeSelector = nodeSelector;
        this.metadataSelector = metadataSelector;
        this.handler = handler;
    }

    long extract(Reader reader) throws CorbException {
        JsonCharReader jsonReader = new JsonCharReader(reader);
        Deque<String> context = new ArrayDeque<>();
        jsonReader.skipWhitespace(null);
        parseValue(jsonReader, context);
        jsonReader.skipWhitespace(null);
        if (jsonReader.peek() != -1) {
            throw new CorbException("Unexpected trailing JSON content");
        }
        return extractedNodeCount;
    }

    private void parseValue(JsonCharReader reader, Deque<String> context) throws CorbException {
        reader.skipWhitespace(null);
        String currentPath = buildCurrentPath(context);
        boolean nodeMatch = nodeSelector != null && nodeSelector.matches(currentPath);
        boolean metadataMatch = !metadataCaptured && metadataSelector != null && metadataSelector.matches(currentPath);
        if (nodeMatch || metadataMatch) {
            String rawJson = captureValue(reader);
            try {
                if (metadataMatch) {
                    handler.onMetadata(currentPath, rawJson);
                    metadataCaptured = true;
                }
                if (nodeMatch) {
                    handler.onNode(currentPath, rawJson);
                    extractedNodeCount++;
                }
            } catch (IOException ex) {
                throw new CorbException("Problem while processing extracted JSON content", ex);
            }
            return;
        }

        int c = reader.peek();
        switch (c) {
            case '{':
                parseObject(reader, context);
                return;
            case '[':
                parseArray(reader, context);
                return;
            case '"':
                parseString(reader, null);
                return;
            case 't':
                parseLiteral(reader, "true", null);
                return;
            case 'f':
                parseLiteral(reader, "false", null);
                return;
            case 'n':
                parseLiteral(reader, "null", null);
                return;
            default:
                if (c == '-' || Character.isDigit(c)) {
                    parseNumber(reader, null);
                    return;
                }
                throw new CorbException("Unexpected character while parsing JSON: " + (char) c);
        }
    }

    private void parseObject(JsonCharReader reader, Deque<String> context) throws CorbException {
        reader.expect('{', null);
        reader.skipWhitespace(null);
        if (reader.peek() == '}') {
            reader.read(null);
            return;
        }

        while (true) {
            String name = parseString(reader, null);
            reader.skipWhitespace(null);
            reader.expect(':', null);
            context.addLast(name);
            try {
                parseValue(reader, context);
            } finally {
                context.removeLast();
            }
            reader.skipWhitespace(null);
            int c = reader.read(null);
            if (c == '}') {
                return;
            }
            if (c != ',') {
                throw new CorbException("Expected ',' or '}' while parsing JSON object");
            }
            reader.skipWhitespace(null);
        }
    }

    private void parseArray(JsonCharReader reader, Deque<String> context) throws CorbException {
        reader.expect('[', null);
        reader.skipWhitespace(null);
        if (reader.peek() == ']') {
            reader.read(null);
            return;
        }

        while (true) {
            context.addLast("*");
            try {
                parseValue(reader, context);
            } finally {
                context.removeLast();
            }
            reader.skipWhitespace(null);
            int c = reader.read(null);
            if (c == ']') {
                return;
            }
            if (c != ',') {
                throw new CorbException("Expected ',' or ']' while parsing JSON array");
            }
            reader.skipWhitespace(null);
        }
    }

    private String captureValue(JsonCharReader reader) throws CorbException {
        StringBuilder raw = new StringBuilder();
        captureValue(reader, raw);
        return raw.toString();
    }

    private void captureValue(JsonCharReader reader, StringBuilder raw) throws CorbException {
        reader.skipWhitespace(null);
        int c = reader.peek();
        switch (c) {
            case '{':
                captureObject(reader, raw);
                return;
            case '[':
                captureArray(reader, raw);
                return;
            case '"':
                parseString(reader, raw);
                return;
            case 't':
                parseLiteral(reader, "true", raw);
                return;
            case 'f':
                parseLiteral(reader, "false", raw);
                return;
            case 'n':
                parseLiteral(reader, "null", raw);
                return;
            default:
                if (c == '-' || Character.isDigit(c)) {
                    parseNumber(reader, raw);
                    return;
                }
                throw new CorbException("Unexpected character while capturing JSON value: " + (char) c);
        }
    }

    private void captureObject(JsonCharReader reader, StringBuilder raw) throws CorbException {
        reader.expect('{', raw);
        reader.skipWhitespace(raw);
        if (reader.peek() == '}') {
            reader.read(raw);
            return;
        }
        while (true) {
            parseString(reader, raw);
            reader.skipWhitespace(raw);
            reader.expect(':', raw);
            reader.skipWhitespace(raw);
            captureValue(reader, raw);
            reader.skipWhitespace(raw);
            int c = reader.read(raw);
            if (c == '}') {
                return;
            }
            if (c != ',') {
                throw new CorbException("Expected ',' or '}' while capturing JSON object");
            }
            reader.skipWhitespace(raw);
        }
    }

    private void captureArray(JsonCharReader reader, StringBuilder raw) throws CorbException {
        reader.expect('[', raw);
        reader.skipWhitespace(raw);
        if (reader.peek() == ']') {
            reader.read(raw);
            return;
        }
        while (true) {
            captureValue(reader, raw);
            reader.skipWhitespace(raw);
            int c = reader.read(raw);
            if (c == ']') {
                return;
            }
            if (c != ',') {
                throw new CorbException("Expected ',' or ']' while capturing JSON array");
            }
            reader.skipWhitespace(raw);
        }
    }

    private String parseString(JsonCharReader reader, StringBuilder raw) throws CorbException {
        reader.expect('"', raw);
        StringBuilder decoded = new StringBuilder();
        while (true) {
            int c = reader.read(raw);
            if (c == -1) {
                throw new CorbException("Unexpected end of JSON input inside string");
            }
            if (c == '"') {
                return decoded.toString();
            }
            if (c == '\\') {
                int escaped = reader.read(raw);
                if (escaped == -1) {
                    throw new CorbException("Unexpected end of JSON input inside escape sequence");
                }
                switch (escaped) {
                    case '"':
                    case '\\':
                    case '/':
                        decoded.append((char) escaped);
                        break;
                    case 'b':
                        decoded.append('\b');
                        break;
                    case 'f':
                        decoded.append('\f');
                        break;
                    case 'n':
                        decoded.append('\n');
                        break;
                    case 'r':
                        decoded.append('\r');
                        break;
                    case 't':
                        decoded.append('\t');
                        break;
                    case 'u':
                        char unicode = parseUnicodeEscape(reader, raw);
                        decoded.append(unicode);
                        break;
                    default:
                        throw new CorbException("Unsupported JSON escape sequence: \\" + (char) escaped);
                }
            } else {
                decoded.append((char) c);
            }
        }
    }

    private char parseUnicodeEscape(JsonCharReader reader, StringBuilder raw) throws CorbException {
        char[] digits = new char[4];
        for (int i = 0; i < digits.length; i++) {
            int c = reader.read(raw);
            if (c == -1) {
                throw new CorbException("Unexpected end of JSON input inside unicode escape");
            }
            digits[i] = (char) c;
        }
        try {
            return (char) Integer.parseInt(new String(digits), 16);
        } catch (NumberFormatException ex) {
            throw new CorbException("Invalid JSON unicode escape: \\u" + new String(digits), ex);
        }
    }

    private void parseLiteral(JsonCharReader reader, String literal, StringBuilder raw) throws CorbException {
        for (int i = 0; i < literal.length(); i++) {
            int c = reader.read(raw);
            if (c != literal.charAt(i)) {
                throw new CorbException("Invalid JSON literal, expected " + literal);
            }
        }
    }

    private void parseNumber(JsonCharReader reader, StringBuilder raw) throws CorbException {
        int c = reader.peek();
        if (c == '-') {
            reader.read(raw);
            c = reader.peek();
        }
        if (!Character.isDigit(c)) {
            throw new CorbException("Invalid JSON number");
        }
        if (c == '0') {
            reader.read(raw);
        } else {
            consumeDigits(reader, raw);
        }
        if (reader.peek() == '.') {
            reader.read(raw);
            consumeDigits(reader, raw);
        }
        c = reader.peek();
        if (c == 'e' || c == 'E') {
            reader.read(raw);
            c = reader.peek();
            if (c == '+' || c == '-') {
                reader.read(raw);
            }
            consumeDigits(reader, raw);
        }
    }

    private void consumeDigits(JsonCharReader reader, StringBuilder raw) throws CorbException {
        int c = reader.peek();
        if (!Character.isDigit(c)) {
            throw new CorbException("Invalid JSON number");
        }
        while (Character.isDigit(reader.peek())) {
            reader.read(raw);
        }
    }

    private String buildCurrentPath(Deque<String> context) {
        if (context.isEmpty()) {
            return "/";
        }
        StringBuilder path = new StringBuilder();
        for (String s : context) {
            path.append('/').append(s);
        }
        return path.toString();
    }

    private static class JsonCharReader {
        private final Reader reader;
        private int next = Integer.MIN_VALUE;

        JsonCharReader(Reader reader) {
            this.reader = reader;
        }

        int peek() throws CorbException {
            if (next == Integer.MIN_VALUE) {
                try {
                    next = reader.read();
                } catch (IOException ex) {
                    throw new CorbException("Problem while reading the JSON file", ex);
                }
            }
            return next;
        }

        int read(StringBuilder raw) throws CorbException {
            int c = peek();
            next = Integer.MIN_VALUE;
            if (raw != null && c != -1) {
                raw.append((char) c);
            }
            return c;
        }

        void expect(char expected, StringBuilder raw) throws CorbException {
            int c = read(raw);
            if (c != expected) {
                throw new CorbException("Expected '" + expected + "' while parsing JSON");
            }
        }

        void skipWhitespace(StringBuilder raw) throws CorbException {
            while (true) {
                int c = peek();
                if (c == -1 || !Character.isWhitespace(c)) {
                    return;
                }
                read(raw);
            }
        }
    }
}
