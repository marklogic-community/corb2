package com.marklogic.developer.corb;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractDecrypter {

    protected transient Properties properties = null;
    protected static final Pattern ENCRYPTED_VALUE_REGEX = Pattern.compile("^ENC\\((.*)\\)$");

    public void init(Properties properties) throws IOException, ClassNotFoundException {
        this.properties = (properties == null ? new Properties() : properties);

        init_decrypter();
    }

    public String getConnectionURI(String uri, String username, String password, String host, String port, String dbname) {
        if (uri != null) {
            return decrypt("XCC-CONNECTION-URI", uri);
        } else {
            return "xcc://" + decrypt("XCC-USERNAME", username) + ":" + decrypt("XCC-PASSWORD", password) + "@"
                    + decrypt("XCC-HOSTNAME", host) + ":" + decrypt("XCC-PORT", port) + (dbname != null ? "/" + decrypt("XCC-DBNAME", dbname) : "");
        }
    }

    protected String decrypt(String property, String value) {
        Matcher match = ENCRYPTED_VALUE_REGEX.matcher(value);
        if (match.matches()) {
            value = match.group(1);
        }
        return doDecrypt(property, value);
    }

    protected abstract void init_decrypter() throws IOException, ClassNotFoundException;

    protected abstract String doDecrypt(String property, String value);

    protected String getProperty(String key) {
        String val = System.getProperty(key);
        if (val == null || val.trim().length() == 0) {
            val = properties.getProperty(key);
        }
        return val != null ? val.trim() : val;
    }
}
