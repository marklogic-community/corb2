package com.marklogic.developer.corb;

import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.Executor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class HTTPServerTest {
    public static final String DUPLICATE_VALUES = "AAABBBCCC";
    public static final String[] ELEMENTS_LOWER = new String[] {"a", "b", "c"};
    public static final String[] ELEMENTS_UPPER = new String[]{"A","B","C"};
    public static final String TEXT_FILE = "src/test/resources//test-file-2.txt";

    @Test
    public void initHttpServer() {
        List<Integer> ports = new ArrayList<>(3);
        ports.add(8);
        ports.add(80);
        ports.add(8000);
        ports.add(80000);
        HTTPServer server = new HTTPServer(ports);
        assertNotNull(server);
    }

    @Test
    public void setPort() throws Exception {
        HTTPServer server = new HTTPServer();
        server.setPort(5000);
        assertEquals(5000, server.port);
    }

    @Test
    public void setSecure() throws Exception {
        HTTPServer server = new HTTPServer();
        server.setSecure(true);
        assertTrue(server.secure);
        server.setSecure(false);
        assertFalse(server.secure);
    }

    @Test
    public void setSocketTimeout() throws Exception {
        HTTPServer server = new HTTPServer();
        server.setSocketTimeout(5);
        assertEquals(5, server.socketTimeout);
    }

    @Test
    public void setExecutor() throws Exception {
        Executor executor = mock(Executor.class);
        HTTPServer server = new HTTPServer();
        server.setExecutor(executor);
        assertEquals(executor, server.executor);
    }

    @Test
    public void getVirtualHost() throws Exception {
        String name = null;
        HTTPServer server = new HTTPServer();

        HTTPServer.VirtualHost host1 = server.getVirtualHost(name);
        assertNotNull(host1);

        name = "does not exist";
        HTTPServer.VirtualHost host2 =  server.getVirtualHost(name);
        assertNull(host2);
    }

    @Test
    public void getVirtualHosts() throws Exception {
        HTTPServer server = new HTTPServer();
        Set<HTTPServer.VirtualHost> host1 = server.getVirtualHosts();
        assertNotNull(host1);
    }

    @Test
    public void addVirtualHost() throws Exception {
        HTTPServer server = new HTTPServer();
        assertFalse(server.hosts.isEmpty());
        HTTPServer.VirtualHost virtualHost = new HTTPServer.VirtualHost("testhost");
        server.addVirtualHost(virtualHost);
        assertEquals(2, server.getVirtualHosts().size());
    }

    @Test
    public void createServerSocket() throws Exception {
        HTTPServer server = new HTTPServer(9898);
        ServerSocket socket = server.createServerSocket();
        assertNotNull(socket);
        socket.close();
    }

    @Test
    public void createServerSocketSecure() throws Exception {
        HTTPServer server = new HTTPServer(9898);
        server.setSecure(true);
        ServerSocket socket = server.createServerSocket();
        assertNotNull(socket);
        socket.close();
    }

    @Test (expected = IllegalArgumentException.class)
    public void createServerSocketAndThrow() throws Exception {
        HTTPServer server = new HTTPServer(-1);
        ServerSocket socket = server.createServerSocket();
        assertNotNull(socket);
        socket.close();
    }

    @Test
    public void startAndStop() throws Exception {
        HTTPServer server = new HTTPServer(9898);
        server.start();
        assertNotNull(server.serv);
        server.stop();
        assertNull(server.serv);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testChunkedInputStream() {
        try (InputStream inputStream = new FileInputStream(TEXT_FILE)) {
            HTTPServer.Headers headers = new HTTPServer.Headers();
            try (HTTPServer.LimitedInputStream limitedInputStream = new HTTPServer.ChunkedInputStream(inputStream, headers)){
             limitedInputStream.limit = 3;
             int read = limitedInputStream.read();
             assertEquals(97, read);
             read = limitedInputStream.read();
             assertEquals(10, read);
             read = limitedInputStream.read();
             assertEquals(99, read);
             limitedInputStream.read();
            }
        } catch(IOException ex) {
            fail();
        }
    }

    @Test
    public void markSupported(){
        try (InputStream inputStream = new FileInputStream(TEXT_FILE)) {
            HTTPServer.LimitedInputStream limitedInputStream = new HTTPServer.LimitedInputStream(inputStream, 1, false);
            assertFalse(limitedInputStream.markSupported());
        } catch (IOException ex){
            fail();
        }
    }

    @Test
    public void available(){
        try (InputStream inputStream = new FileInputStream(TEXT_FILE)) {
            HTTPServer.Headers headers = new HTTPServer.Headers();
            HTTPServer.LimitedInputStream limitedInputStream = new HTTPServer.LimitedInputStream(inputStream, 1, false);
            assertEquals(   1,limitedInputStream.available());
        } catch (IOException ex){
            fail();
        }
    }

    @Test
    public void skip(){
        try (InputStream inputStream = new FileInputStream(TEXT_FILE)) {
            HTTPServer.Headers headers = new HTTPServer.Headers();
            HTTPServer.LimitedInputStream limitedInputStream = new HTTPServer.LimitedInputStream(inputStream, 10, false);
            assertEquals(   2,limitedInputStream.skip(2));
            assertEquals(8, limitedInputStream.skip(500));
        } catch (IOException ex){
            fail();
        }
    }

    @Test (expected = NullPointerException.class)
    public void testChunkedOutputStreamWithNull() {
        OutputStream out = null;
        try (HTTPServer.ChunkedOutputStream inputStream = new HTTPServer.ChunkedOutputStream(out)) {

        } catch (IOException ex){
            fail();
        }
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInitChunkBadSize() {
        try (OutputStream out = new ByteArrayOutputStream();
            HTTPServer.ChunkedOutputStream inputStream = new HTTPServer.ChunkedOutputStream(out)) {
            inputStream.initChunk(-1l);
        } catch (IOException ex){
            fail();
        }
    }

    @Test
    public void testInitChunk() {
        try (OutputStream out = new ByteArrayOutputStream();
             HTTPServer.ChunkedOutputStream inputStream = new HTTPServer.ChunkedOutputStream(out)) {
            inputStream.initChunk(1l);
            assertEquals(1, inputStream.state);
            inputStream.initChunk((5));
            assertEquals("1\r\n\r\n5\r\n", out.toString());
        } catch (IOException ex){
            fail();
        }
    }

    @Test
    public void parseParamsList() {
        List<String[]> params = HTTPServer.parseParamsList("foo=bar&baz=&bat=boom&=&=valNoKey&noval=&ambiguous");

        assertEquals(params.get(0)[0], "foo");
        assertEquals(params.get(0)[1], "bar");

        assertEquals(params.get(1)[0], "baz");
        assertEquals(params.get(1)[1], "");

        assertEquals(params.get(2)[0], "bat");
        assertEquals(params.get(2)[1], "boom");
    }

    @Test
    public void parseParamsListEmptyString() {
        List<String[]> params = HTTPServer.parseParamsList("");
        assertTrue(params.isEmpty());
    }

    @Test
    public void parseParamsListNull() {
        List<String[]> params = HTTPServer.parseParamsList(null);
        assertTrue(params.isEmpty());
    }

    @Test
    public void handleConnection() throws Exception {
        try (InputStream inputStream = new FileInputStream(TEXT_FILE);
             OutputStream out = new ByteArrayOutputStream();) {
            HTTPServer server = new HTTPServer();
            server.handleConnection(inputStream, out);
            String output = out.toString();
            assertTrue(output.contains("400 Bad Request"));
        }
    }

    @Test
    public void addContentTypes() throws Exception {
        String path = "foo";
        String defaultValue = "nope";
        assertEquals(defaultValue, HTTPServer.getContentType(path, defaultValue));
        HTTPServer.addContentType(path, path);
        assertEquals(path, HTTPServer.getContentType("bar.foo", defaultValue));
    }

    @Test
    public void isCompressible() throws Exception {
        assertTrue(HTTPServer.isCompressible("application/json"));
        assertFalse(HTTPServer.isCompressible("application/pdf"));
    }

    @Test
    public void detectLocalHostName() throws Exception {
        assertNotNull(HTTPServer.detectLocalHostName());
    }

    @Test
    public void toMap() throws Exception {
        String key = "key";
        String value = "value";
        String[] pair = new String[] {key, value};
        List<String[]> input = new ArrayList(1);
        input.add(pair);
        Map<String, String> result = HTTPServer.toMap(input);
        assertTrue(result.containsKey(key));
        assertEquals(value, result.get(key));
    }

    @Test
    public void toMapEmpty() throws Exception {
        Map<String, String> result = HTTPServer.toMap(null);
        assertTrue(result.isEmpty());

        List<String[]> input = new ArrayList(0);
        result = HTTPServer.toMap(input);
        assertTrue(result.isEmpty());
    }

    @Test
    public void parseRange() throws Exception {
        long[] result = HTTPServer.parseRange("10-15", 10);
        assertEquals(2, result.length);
        assertEquals(10l, result[0]);
        assertEquals(15l, result[1]);
        result = HTTPServer.parseRange("10-15,16-17", 7);
        assertEquals(2, result.length);
        assertEquals(10l, result[0]);
        assertEquals(17l, result[1]);
    }


    @Test
    public void parseULong() throws Exception {
        assertEquals(123, HTTPServer.parseULong("123", 10), 0.01);
    }


    @Test(expected = NumberFormatException.class)
    public void parseULongNegative() throws Exception {
        HTTPServer.parseULong("-123", 10);
    }


    @Test(expected = NumberFormatException.class)
    public void parseULongPositive() throws Exception {
        HTTPServer.parseULong("+123", 10);
    }

    @Test
    public void parseDate() throws Exception {
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(1994, 11, 6, 8, 49, 37);
        Instant expected = expectedLocalDateTime.toInstant(ZoneOffset.UTC);
        assertEquals(expected, HTTPServer.parseDate("Sun, 06 Nov 1994 08:49:37 GMT").toInstant());
        assertEquals(expected, HTTPServer.parseDate("Sunday, 06-Nov-94 08:49:37 GMT").toInstant());
        assertEquals(expected, HTTPServer.parseDate("Sun Nov 6 08:49:37 1994").toInstant());
    }

    @Test (expected = IllegalArgumentException.class)
    public void parseDateInvalid() throws Exception {
        HTTPServer.parseDate("1999-01-01").toInstant();
    }

    @Test
    public void formatDate() throws Exception {
        assertEquals("Thu, 01 Jan 1970 00:00:00 GMT",HTTPServer.formatDate(0l));
    }

    @Test (expected = IllegalArgumentException.class)
    public void formatDateIsTooSmall() throws Exception {
        HTTPServer.formatDate(-62167393000000l);
        fail();
    }
    @Test(expected = IllegalArgumentException.class)
    public void formatDateIsTooBig() throws Exception {
        HTTPServer.formatDate(253402300899999l);
        fail();
    }

    @Test
    public void splitElements() throws Exception {
        assertArrayEquals(ELEMENTS_LOWER, HTTPServer.splitElements("A,B,C", true));
        assertArrayEquals(ELEMENTS_UPPER, HTTPServer.splitElements("A,B,C", false));
    }

    @Test
    public void split() throws Exception {
        String[] values = HTTPServer.split(DUPLICATE_VALUES, "BBB");
        assertEquals(2, values.length);
        assertEquals("AAA", values[0]);
        assertEquals("CCC", values[1]);
    }

    @Test
    public void join() throws Exception {

        assertEquals("a, b, c", HTTPServer.join(", ", Arrays.asList(ELEMENTS_LOWER)));
    }

    @Test
    public void getParentPath() throws Exception {
        assertEquals("/foo", HTTPServer.getParentPath("/foo/bar"));
        assertEquals("foo", HTTPServer.getParentPath("foo/bar/"));
        assertEquals("foo", HTTPServer.getParentPath("foo/bar"));
    }

    @Test
    public void trimRight() throws Exception {
        assertEquals("AAABBB", HTTPServer.trimRight(DUPLICATE_VALUES, 'C'));
        assertEquals(DUPLICATE_VALUES, HTTPServer.trimRight(DUPLICATE_VALUES, 'A'));
    }

    @Test
    public void trimLeft() throws Exception {
        assertEquals("BBBCCC", HTTPServer.trimLeft(DUPLICATE_VALUES, 'A'));
        assertEquals(DUPLICATE_VALUES, HTTPServer.trimLeft(DUPLICATE_VALUES, 'C'));
    }

    @Test
    public void trimDuplicates() throws Exception {

        assertEquals("ABBBCCC", HTTPServer.trimDuplicates(DUPLICATE_VALUES, 'A'));
        assertEquals("AAABCCC", HTTPServer.trimDuplicates(DUPLICATE_VALUES, 'B'));
        assertEquals("AAABBBC", HTTPServer.trimDuplicates(DUPLICATE_VALUES, 'C'));
        assertEquals(DUPLICATE_VALUES, HTTPServer.trimDuplicates(DUPLICATE_VALUES, 'a'));
    }

    @Test
    public void toSizeApproxString() throws Exception {
        assertEquals("500 ", HTTPServer.toSizeApproxString(500l));
        assertEquals("4.9K", HTTPServer.toSizeApproxString(5000l));
        assertEquals("49K", HTTPServer.toSizeApproxString(50000l));
        assertEquals("4.8M", HTTPServer.toSizeApproxString(5000000l));
        assertEquals("4.7G", HTTPServer.toSizeApproxString(5000000000l));
        assertEquals("4.5T", HTTPServer.toSizeApproxString(5000000000000l));
        assertEquals("4.4P", HTTPServer.toSizeApproxString(5000000000000000l));
        assertEquals("4.3E", HTTPServer.toSizeApproxString(5000000000000000000l));
    }

    @Test
    public void escapeHTML() throws Exception {
        assertEquals("&lt;div style=&quot;color:&#39;red&#39;&quot;&gt;this &amp; that&lt;/div&gt;",
            HTTPServer.escapeHTML("<div style=\"color:'red'\">this & that</div>"));
    }

    @Test
    public void getBytes() throws Exception {
        //verify that multiple strings are consolidated
        assertTrue(Arrays.equals(HTTPServer.getBytes("abc"),HTTPServer.getBytes("a", "b", "c")));
    }

    @Test
    public void getBytesForEmptyValues() throws Exception {
        //verify that multiple strings are consolidated
        assertEquals(0, HTTPServer.getBytes("").length);
    }

    @Test (expected = NullPointerException.class)
    public void getBytesForNull() throws Exception {
        //verify that multiple strings are consolidated
        String data = null;
        HTTPServer.getBytes(data);
        fail();
    }

    @Test
    public void transfer() throws Exception {
        try (
            InputStream input = new FileInputStream(TEXT_FILE);
            OutputStream out = new ByteArrayOutputStream()){
            HTTPServer.transfer(input, out, 5);
            assertEquals("a\nc\ne", out.toString());
        }
    }

    @Test(expected=IOException.class)
    public void transferTooBig() throws Exception {
        try (
            InputStream input = new FileInputStream(TEXT_FILE);
            OutputStream out = new ByteArrayOutputStream()){
            HTTPServer.transfer(input, out, 500);
        }
    }

}
