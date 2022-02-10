package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Connector implementation that produces documents from the rows in a given CSV file.
 */
public class CSVConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);

  private final String path;
  private final String lineNumField;
  private final String filenameField;
  private final String filePathField;
  // CSV Connector might need a compound key for uniqueness based on many columns.
  private final List<String> idFields;
  private final String docIdFormat;
  private final char separatorChar;
  private final char quoteChar;
  private final char escapeChar;
  private final boolean lowercaseFields;
  private final List<String> ignoredTerms;
  private final String moveToAfterProcessing;
  private static final String UTF8_BOM = "\uFEFF";

  public CSVConnector(Config config) {
    super(config);
    this.path = config.getString("path");
    this.lineNumField = config.hasPath("lineNumberField") ? config.getString("lineNumberField") : "csvLineNumber";
    this.filenameField = config.hasPath("filenameField") ? config.getString("filenameField") : "filename";
    this.filePathField = config.hasPath("filePathField") ? config.getString("filePathField") : "source";
    String idField = config.hasPath("idField") ? config.getString("idField") : null;
    // Either specify the idField, or idFields 
    if (idField != null) {
      this.idFields = new ArrayList<String>();
      this.idFields.add(idField);
    } else {
      this.idFields = config.hasPath("idFields") ? config.getStringList("idFields") : new ArrayList<String>();
    }
    this.docIdFormat = config.hasPath("docIdFormat") ? config.getString("docIdFormat") : null;
    this.separatorChar = (config.hasPath("useTabs") && config.getBoolean("useTabs")) ? '\t' : ',';
    this.quoteChar = (config.hasPath("interpretQuotes") && !config.getBoolean("interpretQuotes")) ?
        CSVParser.NULL_CHARACTER : CSVParser.DEFAULT_QUOTE_CHARACTER;
    this.escapeChar = (config.hasPath("ignoreEscapeChar") && config.getBoolean("ignoreEscapeChar")) ? 
        CSVParser.NULL_CHARACTER : CSVParser.DEFAULT_ESCAPE_CHARACTER;
    this.lowercaseFields = config.hasPath("lowercaseFields") ? config.getBoolean("lowercaseFields") : false;
    this.ignoredTerms = config.hasPath("ignoredTerms") ? config.getStringList("ignoredTerms") : new ArrayList<>();
    // A directory to move the files to after they are doing being processed.
    this.moveToAfterProcessing = config.hasPath("moveToAfterProcessing") ? config.getString("moveToAfterProcessing") : null; 
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    
    if (moveToAfterProcessing != null) {
      // Create the destination directory if it doesn't exist.
      File destDir = new File(moveToAfterProcessing); 
      if (!destDir.exists()) {
        log.info("Creating archive directory {}", destDir.getAbsolutePath());
        destDir.mkdirs();
      }
    }
    
    // file is on the classpath
    if (path.startsWith("classpath:")) {
      processFile(path, publisher);
      return;
    }
    // file is on the file system
    File pathFile = new File(path);

    if (!pathFile.exists()) {
      throw new ConnectorException("Path [" + path + "] does not exist");
    }

    if (pathFile.isFile()) {
      processFile(path, publisher);
      return;
    }
    // path is a directory
    if (pathFile.isDirectory()) {
      // no recursion supported
      for (File f: pathFile.listFiles()) {
        processFile(f.getAbsolutePath(), publisher);
      }
    }
  }

  private void processFile(String filePath, Publisher publisher) throws ConnectorException {
    int lineNum = 0;
    log.info("Beginning to process file {}", filePath);
    String filename = new File(filePath).getName();
    try (CSVReader csvReader = new CSVReaderBuilder(FileUtils.getReader(filePath)).
        withCSVParser(new CSVParserBuilder().withSeparator(separatorChar).withQuoteChar(quoteChar).withEscapeChar(escapeChar).build()).build()) {
      // log.info("Processing linenumber: {}", lineNum);
      // Assume first line is header
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        return;
      }
      // lowercase column names
      if (lowercaseFields) {
        for (int i = 0; i < header.length; i++)
          header[i] = header[i].toLowerCase();
      }
      // Index the column names
      HashMap<String, Integer> columnIndexMap = new HashMap<String, Integer>();
      for (int i = 0; i < header.length; i++) {
        // check for BOM
        if (i == 0) {
          header[i] = removeBOM(header[i]);
        }

        if (columnIndexMap.containsKey(header[i])) {
          log.warn("Multiple columns with the name {} were discovered in the source csv file.",  header[i]);
          continue;
        }
        columnIndexMap.put(header[i], i);  
      }
      // create a lookup list for column indexes
      List<Integer> idColumns = new ArrayList<Integer>();
      for (String field : idFields) {
        if (lowercaseFields) {
          idColumns.add(columnIndexMap.get(field.toLowerCase()));
        } else {
          idColumns.add(columnIndexMap.get(field));
        }
      }
      // verify that we found all the columns.
      if (idColumns.size() != idFields.size()) {
        log.warn("Mismatch in idFields to column map.");
      }
      // At this point we should have the list of column ids that map to the idFields 
      String[] line;
      
      while ((line = csvReader.readNext()) != null) {
        lineNum++;
        // log.info("Processing linenumber: {}", lineNum);
        // skip blank lines, lines with no value in the first column
        if (line.length == 0 || (line.length == 1 && StringUtils.isBlank(line[0]))) {
          continue;
        }
        if (line.length != header.length) {
          log.warn(String.format("Line %d of the csv has a different number of columns than columns in the header.", lineNum));
          continue;
        }
        String docId = "";
        if (idColumns.size() > 0) {
          // let's get the columns with the values for the id.
          ArrayList<String> idColumnData = new ArrayList<String>();
          for (Integer idx: idColumns) {
            idColumnData.add(line[idx]);
          }
          docId = createDocId(idColumnData); 
        } else {
          // a default unique id for a csv file is filename + line num
          docId = getDocIdPrefix() + filename + "-" + lineNum;
        }

        Document doc = new Document(docId);
        doc.setField(filePathField, path);
        doc.setField(filenameField, filename);
        // log.info("DOC ID: {}", docId);
        int maxIndex = Math.min(header.length, line.length);
        for (int i = 0; i < maxIndex; i++) {
          if (line[i] != null && !ignoredTerms.contains(line[i]) && !Document.RESERVED_FIELDS.contains(header[i])) {
            doc.setField(header[i], line[i]);
          }
        }
        doc.setField(lineNumField, lineNum);

        publisher.publish(doc);
      }
    } catch (Exception e) {
      log.error("Error during CSV processing", e);
      throw new ConnectorException("Error processing CSV file", e);
    }
    // assuming we got here, we were successful processing the csv file
    if (moveToAfterProcessing != null) {
      if (filePath.startsWith("classpath:")) {
        log.warn("Skipping moving classpath file: {} to {}", filePath, moveToAfterProcessing);
      } else {
        // Move the file
        Path source = Paths.get(filePath);
        String fileName = source.getFileName().toString();
        Path dest = Paths.get(moveToAfterProcessing + File.separatorChar + fileName);
        try {
          Files.move(source, dest);
        } catch (IOException e) {
          throw new ConnectorException("Error moving file to destination directory after crawl.", e);
        }
      }
    }
  }

  private String createDocId(ArrayList<String> idColumnData) {
    // format the string with the data and pre-pend the doc id prefix
    if (docIdFormat != null) {
      return this.getDocIdPrefix() + String.format(docIdFormat, idColumnData.toArray());
    } else {
      // no doc id.. just choose the first value in the idColumnData
      // Join the column data with an underscore if no docIdFormat provided.
      String idData = StringUtils.join(idColumnData, "_");
      return createDocId(idData);
    }
  }

  public String toString() {
    return "CSVConnector: " + path;
  }

  public String removeBOM(String s) {
    if (s.startsWith(UTF8_BOM)) {
      s = s.substring(1);
    }
    return s;
  }
}
