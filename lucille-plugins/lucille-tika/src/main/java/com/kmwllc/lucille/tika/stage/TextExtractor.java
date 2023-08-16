package com.kmwllc.lucille.tika.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

// Local testing:
/*
  -
 */

// add tests for further metadata
// add configurability for tika_ prefix for metadata
// add javadoc / comments throughout code
// test locally with vfs connector

/**
 * This stage uses Apache Tika to perform text and metadata extraction.
 */
public class TextExtractor extends Stage {

  private static final Logger log = LogManager.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private String tikaConfigPath;
  private String byteArrayField;
  private String metadataPrefix;
  private Parser parser;
  private ParseContext parseCtx;

  public TextExtractor(Config config) {
    super(config, new StageSpec().withOptionalProperties("text_field", "file_path_field", "byte_array_field", "tika_config_path",
        "metadata_prefix"));
    textField = config.hasPath("text_field") ? config.getString("text_field") : "text";
    filePathField = config.hasPath("file_path_field") ? config.getString("file_path_field") : "filepath";
    byteArrayField = config.hasPath("byte_array_field") ? config.getString("byte_array_field") : "byte_array";
    metadataPrefix = config.hasPath("metadata_prefix") ? config.getString("metadata_prefix") : "tika";
    tikaConfigPath = config.hasPath("tika_config_path") ? config.getString("tika_config_path") : null;
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

      byte[] byteArray = doc.getBytes(byteArrayField);

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
    ContentHandler bch = new BodyContentHandler();
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      log.warn("Tika Exception: {}", e);
    }

    doc.addToField(textField, bch.toString());
    parseCtx.toString();
    for (String name : metadata.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name);
      for (String value : metadata.getValues(name)) {
        doc.addToField(metadataPrefix + "_" + cleanName, value);
      }
    }
  }
}