package com.marklogic.developer.corb.util;

import com.marklogic.developer.corb.CorbException;
import org.junit.Test;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

public class XmlUtilsTest {
    String dir = "src/test/resources/streamingXMLUrisLoader/";
    File xmlDoc = new File(dir , "EDI.ICF15T.D150217.T113100716.T");
    File schema = new File(dir + "BenefitEnrollment.xsd");

    @Test
    public void schemaValidate() {

        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema);
            assertTrue(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void schemaValidateWithError()  {
        File schema = new File(dir, "NotBenefitEnrollment.xsd");
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema);
            assertFalse(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test (expected = CorbException.class)
    public void schemaValidateMissingFile() throws CorbException {
        File missingFile = new File("does-not-exist.xml");
        List<SAXParseException> exceptionList = XmlUtils.schemaValidate(missingFile, schema);
    }

}
