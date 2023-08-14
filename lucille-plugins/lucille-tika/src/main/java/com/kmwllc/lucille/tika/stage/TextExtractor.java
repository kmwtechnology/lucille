package com.kmwllc.lucille.tika.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.util.Iterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.Base64;

/**
 * This stage uses Apache Tika to perform text and metadata extraction.
 */
public class TextExtractor extends Stage {
  private static final Logger log = LogManager.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private String tikaConfigPath;
  private String byteArrayField;
  private Parser parser;
  private ParseContext parseCtx;

  public TextExtractor(Config config) {
    super(config);
    textField = config.hasPath("textField") ? config.getString("textField") : "text";
    filePathField = config.hasPath("filePathField") ? config.getString("filePathField") : "filepath";
    byteArrayField = config.hasPath("byteArrayField") ? config.getString("byteArrayField") : "byteArray";
    tikaConfigPath = config.hasPath("tikaConfigPath") ? config.getString("tikaConfigPath") : null;
    parseCtx = new ParseContext();
  }

  @Override
  public void start() throws StageException {
    if (this.tikaConfigPath == null) {
      parser = new AutoDetectParser();
    } else {
      try {
        File f = new File(this.tikaConfigPath);
        TikaConfig tc = new TikaConfig(f);
        parser = new AutoDetectParser(tc);
      } catch (Exception e) {
        throw new StageException("Error starting TextExtractor stage.", e);
      }
    }
    parseCtx.set(Parser.class, parser);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // if the document has both a byteArray field and a filePathField, only byteArray will be processed.
    if (doc.has(byteArrayField)) {

      // Local testing:
      /*
        - Make python script to convert a file to byte array
        - copy byte array contents into csv with field name as 'byte_field'
        - Run lucille with this stage to see if that byte array is recognized as a byte array
        - if not, consider how to decode it frm string to byte array
       */
      byte[] byteArray = doc.getBytes(byteArrayField);
      //byteArray = Base64.getDecoder().decode(new String(doc.getString(byteArrayField)).getBytes("UTF-8"));

      InputStream inputStream = new ByteArrayInputStream(byteArray);
      parseInputStream(doc, inputStream);

    } else if (doc.has(filePathField)) {

      String filePath = doc.getString(filePathField);

      try {
        InputStream inputStream = FileUtils.getInputStream(filePath);
        parseInputStream(doc, inputStream);
      } catch (IOException e) {
        throw new StageException("InputStream cannot be parsed or created", e);
      }
    }
    return null;
  }

  private static String cleanFieldName(String name) {
    String cleanName = name.trim().toLowerCase();
    cleanName = cleanName.replaceAll(" ", "_");
    cleanName = cleanName.replaceAll("-", "_");
    cleanName = cleanName.replaceAll(":", "_");
    return cleanName;
  }

  public void parseInputStream(Document doc, InputStream inputStream) {
    Metadata metadata = new Metadata();
    StringWriter textData = new StringWriter();
    ContentHandler bch = new BodyContentHandler(textData);
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      log.warn("Tika Exception: {}", e);
    }

//    doc.addToField(textField, textData.toString());
    doc.addToField(textField, bch.toString());
    parseCtx.toString();
    for (String name : metadata.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name);
      for (String value : metadata.getValues(name)) {
        doc.addToField("tika_" + cleanName, value);
      }
    }
  }
}