package com.marklogic.developer.corb;

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class PostBatchUpdateJsonFileTaskTest {

    /*
    @Test
    public void writeBottomContent() throws IOException {
        File test = File.createTempFile("foo","txt");
        test.deleteOnExit();
        try (
             BufferedWriter writer = new BufferedWriter(new FileWriter(test, false))) {
            String line;
            writer.write("[");
            writer.newLine();
            writer.write("{foo: 1},");
            writer.newLine();
            writer.write("{foo: 2}");
            writer.newLine();
            writer.flush();
        }
        //System.out.println(TestUtils.readFile(test));
        System.setProperty("EXPORT-FILE-BOTTOM-CONTENT", "]");
        System.setProperty("EXPORT-FILE-NAME", test.getAbsolutePath());
        //System.setProperty("EXPORT-FILE-DIR", test.getParentFile().getAbsolutePath());
        PostBatchUpdateJsonFileTask puf = new PostBatchUpdateJsonFileTask();
        puf.writeBottomContent();

        System.out.println(TestUtils.readFile(test));
    }

    @Test
    public void parseFile() throws Exception {
        File file = new File("/tmp/test.xml");

        parseFile(file, "Product");
    }
*/
    public static void main(String args[]) throws ParserConfigurationException, IOException {
        File file = new File("/tmp/test.xml");
        parseFile(file, "Product");
    }

    public static void parseFile(File file, String elementName)
        throws ParserConfigurationException, IOException{
        List<Document> good = new ArrayList<>();
        List<String> bad = new ArrayList<>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        StringBuilder buffer = new StringBuilder();
        String line;
        boolean append = false;

        try (Scanner scanner = new Scanner(file)){
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();
                if (line.startsWith('<' + elementName)) {
                    //start accumulating content
                    append = true;
                } else if (line.startsWith("</" + elementName)) {
                    //end of a Product, collect it and try to parse
                    buffer.append(line);
                    append = false;
                    try {
                        builder = factory.newDocumentBuilder();
                        Document document = builder.parse(new InputSource(new StringReader(buffer.toString())));
                        good.add(document);

                        buffer.setLength(0); //reset the buffer to start a new doc
                    } catch (SAXException ex) {
                        bad.add(buffer.toString());
                    }
                }
                if (append) { // accumulate content
                    buffer.append(line);
                }
            }
            System.out.println("Good items: " + good.size() + " Bad items: " + bad.size());
            //do stuff with the results...
        }
    }
}
