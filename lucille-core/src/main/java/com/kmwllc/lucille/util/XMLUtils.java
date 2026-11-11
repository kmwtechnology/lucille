package com.kmwllc.lucille.util;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

/**
 * Util class for configuring XML parsers against XXE and entity expansion.
 */
public class XMLUtils {

  private static final String DISALLOW_DOCTYPE = "http://apache.org/xml/features/disallow-doctype-decl";
  private static final String EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
  private static final String EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
  private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

  private XMLUtils() {
  }

  /**
   * Configures the factory to reject any DOCTYPE. With allowDoctype, a DOCTYPE and its internal subset are accepted, but
   * external DTDs and external entities are never loaded.
   *
   * @param factory the factory to configure
   * @param allowDoctype whether documents may contain a DOCTYPE
   * @throws ParserConfigurationException if the parser does not support a required feature
   * @throws SAXException if the parser does not recognize a required feature
   */
  public static void secure(SAXParserFactory factory, boolean allowDoctype) throws ParserConfigurationException, SAXException {
    factory.setFeature(DISALLOW_DOCTYPE, !allowDoctype);
    factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    factory.setFeature(LOAD_EXTERNAL_DTD, false);
    factory.setXIncludeAware(false);
  }

  /**
   * Configures the factory to reject any DOCTYPE. With allowDoctype, a DOCTYPE and its internal subset are accepted, but
   * external DTDs and external entities are never loaded, and secure processing limits entity expansion.
   *
   * @param factory the factory to configure
   * @param allowDoctype whether documents may contain a DOCTYPE
   * @throws ParserConfigurationException if the parser does not support a required feature
   */
  public static void secure(DocumentBuilderFactory factory, boolean allowDoctype) throws ParserConfigurationException {
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature(DISALLOW_DOCTYPE, !allowDoctype);
    factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    factory.setFeature(LOAD_EXTERNAL_DTD, false);
    factory.setXIncludeAware(false);
  }
}
