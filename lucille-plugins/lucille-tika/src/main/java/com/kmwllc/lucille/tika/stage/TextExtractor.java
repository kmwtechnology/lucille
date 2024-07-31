package com.kmwllc.lucille.tika.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
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

/**
 * This stage uses Apache Tika to perform text and metadata extraction
 * <br>
 * Config Parameters -
 * <br>
 * text_field (String, Optional) : name of destination field for parsed data to be placed
 * file_path_field (String, Optional) : name of field from which file path can be extracted, if file_path_field
 * and byte_array_field both not provided, stage will do nothing
 * byte_array_field (String, Optional) : name of field from which byte array data can be extracted
 * tika_config_path (String, Optional) : path to tika config, if not provided will default to empty AutoDetectParser
 * metadata_prefix (String, Optional) : prefix to be appended to fields for metadata information extracted after parsing
 * text_content_limit (Integer, Optional) : limits how large the content of the returned text can be
 * metadata_whitelist (StringList, Optional) : list of metadata names that are to be included in document
 * metadata_blacklist (StringList, Optional) : list of metadata names that are not to be included in document
 */
public class TextExtractor extends Stage {

  private static final Logger log = LogManager.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private String tikaConfigPath;
  private String byteArrayField;
  private String metadataPrefix;
  private Integer textContentLimit;
  private List<String> metadataWhitelist;
  private List<String> metadataBlacklist;
  private Parser parser;
  private ParseContext parseCtx;
  private StandardFileSystemManager fsManager;

  public TextExtractor(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("text_field", "file_path_field", "byte_array_field", "tika_config_path",
        "metadata_prefix", "metadata_whitelist", "metadata_blacklist", "text_content_limit"));
    textField = config.hasPath("text_field") ? config.getString("text_field") : "text";
    filePathField = config.hasPath("file_path_field") ? config.getString("file_path_field") : null;
    byteArrayField = config.hasPath("byte_array_field") ? config.getString("byte_array_field") : null;
    metadataPrefix = config.hasPath("metadata_prefix") ? config.getString("metadata_prefix") : "tika";
    tikaConfigPath = config.hasPath("tika_config_path") ? config.getString("tika_config_path") : null;
    textContentLimit = config.hasPath("text_content_limit") ? config.getInt("text_content_limit") : Integer.MAX_VALUE;
    metadataWhitelist = config.hasPath("metadata_whitelist") ? config.getStringList("metadata_whitelist") : null;
    metadataBlacklist = config.hasPath("metadata_blacklist") ? config.getStringList("metadata_blacklist") : null;
    if (metadataWhitelist != null && metadataBlacklist != null) {
      throw new StageException("Provided both a whitelist and blacklist to the TextExtractor stage");
    }
    if (filePathField != null && byteArrayField != null) {
      throw new StageException("Provided both a file_path_field and byte_array_field to the TextExtractor stage");
    }
    if (filePathField == null && byteArrayField == null) {
      throw new StageException("Provided neither a file_path_field nor byte_array_field to the TextExtractor stage");
    }
    parseCtx = new ParseContext();
    fsManager = new StandardFileSystemManager();
  }

  @Override
  public void start() throws StageException {
    try {
      fsManager.init();
    } catch (FileSystemException e) {
      throw new StageException("Could not initialize FileSystem in TextExtractor Stage", e);
    }
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
  public void stop() {
    if (fsManager != null) {
      fsManager.close();
    }
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

      InputStream inputStream = getFileInputStream(filePath);
      if (inputStream == null) {
        log.warn("Error getting inputStream for file at : {}", filePath);
        return null;
      }
      parseInputStream(doc, inputStream);
    }
    return null;
  }

  private InputStream getFileInputStream(String filePath) {
    FileObject file;
    try {
      file = fsManager.resolveFile(filePath);

      if (file == null) {
        log.warn("File object is null for path: {}", filePath);
        return null;
      }

      if (!file.exists()) {
        log.warn("File not found at: {}", filePath);
        return null;
      }

      FileContent content = file.getContent();
      try {
        return content.getInputStream();
      } catch (FileSystemException e) {
        log.warn("Error opening input stream for file: {}", filePath, e);
        return null;
      }
    } catch (FileSystemException e) {
      log.warn("Error retrieving file contents: {}", filePath, e);
      return null;
    }
  }

  /**
   * Cleans the name of metadata field names to be in line with general standards for documents
   */
  private static String cleanFieldName(String name) {
    String cleanName = name.trim().toLowerCase();
    cleanName = cleanName.replaceAll(" ", "_");
    cleanName = cleanName.replaceAll("-", "_");
    cleanName = cleanName.replaceAll(":", "_");
    return cleanName;
  }

  /**
   * Parses given input stream, close it, and adds the text data and metadata to given document
   */
  public void parseInputStream(Document doc, InputStream inputStream) {
    Metadata metadata = new Metadata();
    ContentHandler bch = new BodyContentHandler(textContentLimit);
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      log.warn("Tika Exception: {}", e.getMessage());
    } finally {
      // close the inputStream regardless if error is thrown
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e2) {
          log.warn("Failed to close inputStream: {}", e2.getMessage());
        }
      }
    }

    doc.addToField(textField, bch.toString());
    String newMetadataPrefix = metadataPrefix == "" ? "": metadataPrefix + "_" ;
    for (String name : metadata.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name);
      if ((metadataBlacklist != null && !metadataBlacklist.contains(cleanName))
          || (metadataWhitelist != null && metadataWhitelist.contains(cleanName))
          || (metadataWhitelist == null) && (metadataBlacklist == null)) {
        for (String value : metadata.getValues(name)) {
          doc.addToField(newMetadataPrefix + cleanName, value);
        }
      }
    }
  }
}