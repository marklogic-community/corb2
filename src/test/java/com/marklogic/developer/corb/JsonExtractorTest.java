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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonExtractorTest {

    @Test
    void extractInvokesNodeAndMetadataHandlersWhenBothMatchSamePath() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        JsonExtractor extractor = new JsonExtractor(selector("/value"), selector("/value"), handler);

        long count = extractor.extract(new StringReader("{\"value\":1}"));

        assertEquals(1L, count);
        assertEquals(1, handler.nodes.size());
        assertEquals(1, handler.metadata.size());
        assertEquals("1", handler.nodes.get(0));
        assertEquals("1", handler.metadata.get(0));
    }

    @Test
    void extractCapturesMetadataOnlyOnce() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        JsonExtractor extractor = new JsonExtractor(new StreamingJsonPath("//value"), new StreamingJsonPath("/items/*/meta"), handler);

        long count = extractor.extract(new StringReader("{\"items\":[{\"meta\":\"first\",\"value\":1},{\"meta\":\"second\",\"value\":2}]}"));

        assertEquals(2L, count);
        assertEquals(2, handler.nodes.size());
        assertEquals(1, handler.metadata.size());
        assertEquals("\"first\"", handler.metadata.get(0));
    }

    @Test
    void extractCapturesRootPrimitiveValues() throws Exception {
        assertExtracted("/", "true", "true");
        assertExtracted("/", "false", "false");
        assertExtracted("/", "null", "null");
        assertExtracted("/", "-12.34e+2", "-12.34e+2");
    }

    @Test
    void extractNamedProperty() throws Exception {
        assertExtracted("/value", "{\"notValueFalse\": false,\"notValueNull\": null, \"notValueTrue\": true, \"notValueArray\": [],\"notValueObject\": {}, \"value\": true }", "true");
        assertExtracted("/value", "{\"notValueFalse\": false,\"notValueNull\": null, \"notValueTrue\": true, \"notValueArray\": [],\"notValueObject\": {}, \"value\": {} }", "{}");
        assertExtracted("/value", "{\"notValueFalse\": false,\"notValueNull\": null, \"notValueTrue\": true, \"notValueArray\": [],\"notValueObject\": {}, \"value\": [] }", "[]");
    }

    @Test
    void extractMatchesEscapedPropertyNames() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        JsonExtractor extractor = new JsonExtractor(selector("/a\tb"), null, handler);

        long count = extractor.extract(new StringReader("{\"a\\tb\":1,\"other\":2}"));

        assertEquals(1L, count);
        assertEquals("1", handler.nodes.get(0));
    }

    @Test
    void extractEmptyArray() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        JsonExtractor extractor = new JsonExtractor(selector("/*"), null, handler);

        long count = extractor.extract(new StringReader("[]"));

        assertEquals(0L, count);
    }

    @Test
    void extractMatchesUnicodeEscapedPropertyNames() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        JsonExtractor extractor = new JsonExtractor(selector("/A"), null, handler);

        long count = extractor.extract(new StringReader("{\"\\u0041\":2}"));

        assertEquals(1L, count);
        assertEquals("2", handler.nodes.get(0));
    }

    @Test
    void extractCapturesStringsWithSupportedEscapes() throws Exception {
        assertExtracted("/", "\"a\\bb\\fc\\nd\\re\\tf\\/g\\\\h\\\"i\\u0041\"", "\"a\\bb\\fc\\nd\\re\\tf\\/g\\\\h\\\"i\\u0041\"");
    }

    @Test
    void extractWrapsIoExceptionFromNodeHandler() {
        JsonExtractor extractor = new JsonExtractor(selector("/"), null, new JsonExtractor.ExtractionHandler() {
            @Override
            public void onNode(String currentPath, String rawJson) throws IOException {
                throw new IOException("boom");
            }

            @Override
            public void onMetadata(String currentPath, String rawJson) {
            }
        });

        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("1")));
        assertTrue(ex.getMessage().contains("Problem while processing extracted JSON content"));
    }

    @Test
    void extractWrapsIoExceptionFromMetadataHandler() {
        JsonExtractor extractor = new JsonExtractor(null, selector("/"), new JsonExtractor.ExtractionHandler() {
            @Override
            public void onNode(String currentPath, String rawJson) {
            }

            @Override
            public void onMetadata(String currentPath, String rawJson) throws IOException {
                throw new IOException("boom");
            }
        });

        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("1")));
        assertTrue(ex.getMessage().contains("Problem while processing extracted JSON content"));
    }

    @Test
    void extractRejectsUnexpectedTrailingContent() {
        JsonExtractor extractor = new JsonExtractor(null, null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("{} {}")));
        assertTrue(ex.getMessage().contains("Unexpected trailing JSON content"));
    }

    @Test
    void extractRejectsUnexpectedCharacterWhileParsing() {
        JsonExtractor extractor = new JsonExtractor(null, null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("x")));
        assertTrue(ex.getMessage().contains("Unexpected character while parsing JSON"));
    }

    @Test
    void extractRejectsUnexpectedCharacterWhileCapturing() {
        JsonExtractor extractor = new JsonExtractor(selector("/"), null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("x")));
        assertTrue(ex.getMessage().contains("Unexpected character while capturing JSON value"));
    }

    @Test
    void extractRejectsInvalidObjectSeparator() {
        JsonExtractor extractor = new JsonExtractor(null, null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("{\"a\":1 \"b\":2}")));
        assertTrue(ex.getMessage().contains("Expected ',' or '}' while parsing JSON object"));
    }

    @Test
    void extractRejectsInvalidArraySeparator() {
        JsonExtractor extractor = new JsonExtractor(null, null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("[1 2]")));
        assertTrue(ex.getMessage().contains("Expected ',' or ']' while parsing JSON array"));
    }

    @Test
    void extractRejectsInvalidLiteral() {
        JsonExtractor extractor = new JsonExtractor(null, null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("truX")));
        assertTrue(ex.getMessage().contains("Invalid JSON literal"));
    }

    @Test
    void extractRejectsInvalidNumber() {
        JsonExtractor extractor = new JsonExtractor(null, null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("-x")));
        assertTrue(ex.getMessage().contains("Invalid JSON number"));
    }

    @Test
    void extractRejectsUnexpectedEndInsideString() {
        JsonExtractor extractor = new JsonExtractor(selector("/"), null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("\"abc")));
        assertTrue(ex.getMessage().contains("Unexpected end of JSON input inside string"));
    }

    @Test
    void extractRejectsUnexpectedEndInsideEscapeSequence() {
        JsonExtractor extractor = new JsonExtractor(selector("/"), null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("\"" + "\\")));
        assertTrue(ex.getMessage().contains("Unexpected end of JSON input inside escape sequence"));
    }

    @Test
    void extractRejectsUnexpectedEndInsideUnicodeEscape() {
        JsonExtractor extractor = new JsonExtractor(selector("/"), null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("\"\\u12")));
        assertTrue(ex.getMessage().contains("Unexpected end of JSON input inside unicode escape"));
    }

    @Test
    void extractRejectsInvalidUnicodeEscape() {
        JsonExtractor extractor = new JsonExtractor(selector("/"), null, new RecordingHandler());
        CorbException ex = assertThrows(CorbException.class, () -> extractor.extract(new StringReader("\"\\u00XZ\"")));
        assertTrue(ex.getMessage().contains("Invalid JSON unicode escape"));
    }

    private void assertExtracted(String path, String input, String expectedRaw) throws Exception {
        RecordingHandler handler = new RecordingHandler();
        JsonExtractor extractor = new JsonExtractor(selector(path), null, handler);

        long count = extractor.extract(new StringReader(input));

        assertEquals(1L, count);
        assertEquals(1, handler.nodes.size());
        assertEquals(expectedRaw, handler.nodes.get(0));
    }

    private JsonSelector selector(final String expectedPath) {
        return new JsonSelector() {
            @Override
            public boolean matches(String currentPath) {
                return expectedPath.equals(currentPath);
            }

            @Override
            public String getExpression() {
                return expectedPath;
            }
        };
    }

    private static class RecordingHandler implements JsonExtractor.ExtractionHandler {
        private final List<String> nodes = new ArrayList<String>();
        private final List<String> metadata = new ArrayList<String>();

        @Override
        public void onNode(String currentPath, String rawJson) {
            nodes.add(rawJson);
        }

        @Override
        public void onMetadata(String currentPath, String rawJson) {
            metadata.add(rawJson);
        }
    }
}
