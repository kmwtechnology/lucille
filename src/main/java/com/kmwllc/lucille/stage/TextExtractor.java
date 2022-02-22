package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.List;

/**
 * This stage uses Apache Tika to perform text and metadata extraction.
 */
public class TextExtractor extends Stage {
  private static final Logger log = LogManager.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private Parser parser;
  private ParseContext parseCtx;

  public TextExtractor(Config config) {
    super(config);
    textField = config.hasPath("textField") ? config.getString("textField") : "text";
    filePathField = config.hasPath("filePathField") ? config.getString("filePathField") : "filepath";
    parser = new AutoDetectParser();
    parseCtx = new ParseContext();
    parseCtx.set(Parser.class, parser);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = doc.getString(filePathField);

    File f = new File(filePath);
    if (!f.exists()) {
      log.warn("File path not found ", filePath);
    }

    FileInputStream binaryData = null;
    try {
      binaryData = new FileInputStream(f);
    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      // This should never happen.
    }
    // InputStream binaryData = null;

    Metadata metadata = new Metadata();
    StringWriter textData = new StringWriter();
    ContentHandler bch = new BodyContentHandler(textData);
    try {
      parser.parse(binaryData, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      log.warn("Tika Exception: {}", e);
    }

    doc.addToField(textField, textData.toString());
    for (String name : metadata.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name);
      for (String value : metadata.getValues(name)) {
        doc.addToField("tika_" + cleanName, value);
      }
    }
    return null;
  }

  // TODO: this should go on a common utility interface or something.
  private static String cleanFieldName(String name) {
    String cleanName = name.trim().toLowerCase();
    cleanName = cleanName.replaceAll(" ", "_");
    cleanName = cleanName.replaceAll("-", "_");
    cleanName = cleanName.replaceAll(":", "_");
    return cleanName;
  }
}