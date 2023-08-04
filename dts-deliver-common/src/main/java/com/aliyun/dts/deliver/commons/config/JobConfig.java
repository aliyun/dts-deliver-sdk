package com.aliyun.dts.deliver.commons.config;

import com.aliyun.dts.deliver.commons.config.DtsProperties;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.exceptions.ConfigErrorException;
import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JobConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobConfig.class);

    private Settings settings;

    public JobConfig(Map<String, String> settingValueMap) {
        Properties properties = new Properties();
        properties.putAll(settingValueMap);
        settings = new Settings(properties);
    }

    public JobConfig(String configPath, Map<String, String> settingValueMap) throws Exception {
        DtsProperties prop = new DtsProperties();

        File configFile = new File(configPath);

        if (configFile == null || !configFile.exists() || configFile.isDirectory()) {
            LOGGER.info("The configuration file of Job is a directory or not existed");
        } else {

            try (InputStreamReader tmpIn = new InputStreamReader(new FileInputStream(configFile), "utf8")) {
                prop.load(tmpIn);
            }
        }

        if (settingValueMap != null) {
            for (Map.Entry<String, String> entry : settingValueMap.entrySet()) {
                prop.setProperty(entry.getKey(), entry.getValue());
            }
        }
        settings = new Settings(prop);
    }

    public static void dumpConfiguration(JobConfig conf, String propertyName, Writer out) {
    }

    public String get(String key) {
        return settings.get(key);
    }

    public Object get(Settings.Setting key) {
        return key.getRawValue(settings);
    }

    public void set(String key, Object value) {
        if (key != null && value != null) {
            this.settings.set(key, value);
        }
    }

    public void set(Settings.Setting key, Object value) {
        if (key != null && value != null) {
            key.setValue(settings, value);
        }
    }

    public Set<String> keys() {
        Set<String> keys = new HashSet<String>();
        for (Map.Entry<String, Object> entry : this.settings.getSettings().entrySet()) {
            String key =  entry.getKey();
            keys.add(key);
        }

        return keys;
    }

    public Settings getSettings() {
        return settings;
    }

    public void writeXml(@Nullable String propertyiName, Writer outWriter)
            throws IOException, IllegalArgumentException {
        Document document = asXmlDocument(propertyiName);

        try {
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(outWriter);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer newTransformer = transformerFactory.newTransformer();

            newTransformer.transform(domSource, streamResult);
        } catch (TransformerException trEx) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_IO_EXCEPTION, trEx);
        }
    }

    /**
     * Return the XML DOM corresponding to this Configuration.
     */
    private synchronized Document asXmlDocument(@Nullable String propertyName)
            throws IOException, IllegalArgumentException {
        Document doc;
        try {
            doc = DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder()
                    .newDocument();
        } catch (ParserConfigurationException pe) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_IO_EXCEPTION, pe);
        }

        Element conf = doc.createElement("configuration");
        doc.appendChild(conf);
        conf.appendChild(doc.createTextNode("\n"));
//        handleDeprecation(); //ensure properties is set and deprecation is handled

        if (!Strings.isNullOrEmpty(propertyName)) {
            if (!settings.getSettings().containsKey(propertyName)) {
                // given property not found, illegal argument
                throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "Property "
                        + propertyName + " not found");
            } else {
                // given property is found, write single property
                appendXMLProperty(doc, conf, propertyName);
                conf.appendChild(doc.createTextNode("\n"));
            }
        } else {
            // append all elements
            for (String key : settings.getSettings().keySet()) {
                appendXMLProperty(doc, conf, key);
                conf.appendChild(doc.createTextNode("\n"));
            }
        }
        return doc;
    }

    /**
     *  Append a property with its attributes to a given {#link Document}
     *  if the property is found in configuration.
     *
     * @param doc
     * @param conf
     * @param propertyName
     */
    private synchronized void appendXMLProperty(Document doc, Element conf,
                                                String propertyName) {
        // skip writing if given property name is empty or null
        if (!Strings.isNullOrEmpty(propertyName)) {
            Object value = settings.getSettings().get(propertyName);
            if (value != null) {
                Element propNode = doc.createElement("property");
                conf.appendChild(propNode);

                Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(propertyName));
                propNode.appendChild(nameNode);

                Element valueNode = doc.createElement("value");
                Settings.Setting setting = settings.getSetting(propertyName);
                if (setting != null) {
                    value = setting.getRawValueWithEncryptFilter(settings);
                }
                valueNode.appendChild(doc.createTextNode(value.toString()));
                propNode.appendChild(valueNode);
            }
        }
    }
}
