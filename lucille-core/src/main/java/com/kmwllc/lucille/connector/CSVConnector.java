package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvValidationException;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Connector implementation that produces documents from the rows in a given CSV file.
 */
public class CSVConnector extends AbstractConnector implements FileHandler {

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
  private final String moveToErrorFolder;
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
    // if both a separator char and useTabs is specified, useTabs takes precedence
    char separator = config.hasPath("separatorChar") ? CharUtils.toChar(config.getString("separatorChar")) : ',';
    this.separatorChar = (config.hasPath("useTabs") && config.getBoolean("useTabs")) ? '\t' : separator;
    this.quoteChar = (config.hasPath("interpretQuotes") && !config.getBoolean("interpretQuotes")) ?
        CSVParser.NULL_CHARACTER : CSVParser.DEFAULT_QUOTE_CHARACTER;
    this.escapeChar = (config.hasPath("ignoreEscapeChar") && config.getBoolean("ignoreEscapeChar")) ?
        CSVParser.NULL_CHARACTER : CSVParser.DEFAULT_ESCAPE_CHARACTER;
    this.lowercaseFields = config.hasPath("lowercaseFields") ? config.getBoolean("lowercaseFields") : false;
    this.moveToErrorFolder = config.hasPath("moveToErrorFolder") ? config.getString("moveToErrorFolder") : null;
    this.ignoredTerms = config.hasPath("ignoredTerms") ? config.getStringList("ignoredTerms") : new ArrayList<>();
    // A directory to move the files to after they are doing being processed.
    this.moveToAfterProcessing = config.hasPath("moveToAfterProcessing") ? config.getString("moveToAfterProcessing") : null;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    createProcessedAndErrorFoldersIfSet();

    File pathFile = new File(path);
    if (!path.startsWith("classpath:") && !pathFile.exists()) {
      throw new ConnectorException("Path " + path + " does not exist");
    }

    if (pathFile.isDirectory()) {
      // no recursion supported
      for (File f : pathFile.listFiles()) {
        try {
          processFile(f.getAbsolutePath(), publisher);
        } catch (ConnectorException e) {
          log.warn("Error Processing CSV File. {}", f.getAbsolutePath(), e);
        }
      }
    } else {
      processFile(path, publisher);
    }
  }

  private void processFile(String filePath, Publisher publisher) throws ConnectorException {
    int lineNum = 0;
    log.info("Beginning to process file {}", filePath);
    String filename = new File(filePath).getName();
    try (CSVReader csvReader = new CSVReaderBuilder(FileUtils.getReader(filePath)).
        withCSVParser(
            new CSVParserBuilder().withSeparator(separatorChar).withQuoteChar(quoteChar).withEscapeChar(escapeChar).build())
        .build()) {
      // log.info("Processing linenumber: {}", lineNum);
      // Assume first line is header
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        return;
      }
      // lowercase column names
      if (lowercaseFields) {
        for (int i = 0; i < header.length; i++) {
          header[i] = header[i].toLowerCase();
        }
      }
      // Index the column names
      HashMap<String, Integer> columnIndexMap = getColumnIndexMap(header);
      // create a lookup list for column indexes and verify we got the same number of idColumns and idFields
      List<Integer> idColumns = getIdColumns(columnIndexMap);
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
          // the line/row number reported here may differ from the physical line number in the file, if the CSV contains
          // a quoted value that spans multiple lines
          log.warn("Logical row {} of the csv has a different number of columns than the header.", lineNum);
          continue;
        }
        Document doc = getDocumentFromLine(idColumns, header, line, filename, filePath, lineNum);
        publisher.publish(doc);
      }
      // assuming we got here, we were successful processing the csv file
      if (moveToAfterProcessing != null) {
        moveFile(filePath, moveToAfterProcessing);
      }
    } catch (CsvException e) { // base class for most opencsv exceptions
      log.error("Error during CSV processing at line {}", e.getLineNumber(), e);
      if (moveToErrorFolder != null) {
        moveFile(filePath, moveToErrorFolder);
      }
    } catch (CsvMalformedLineException e) { // an IOException, not a CsvException
      log.error("Error during CSV processing at line {}", e.getLineNumber(), e);
      if (moveToErrorFolder != null) {
        moveFile(filePath, moveToErrorFolder);
      }
    } catch (Exception e) {
      log.error("Error during CSV processing", e);
      if (moveToErrorFolder != null) {
        moveFile(filePath, moveToErrorFolder);
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

  public void moveFile(String filePath, String option) {
    if (filePath.startsWith("classpath:")) {
      log.warn("Skipping moving classpath file: {} to {}", filePath, moveToAfterProcessing);
      return;
    }

    Path source = Paths.get(filePath);
    String fileName = source.getFileName().toString();
    Path dest = Paths.get(option + File.separatorChar + fileName);
    try {
      Files.move(source, dest);
      log.info("File {} was successfully moved from source {} to destination {}", fileName, source, dest);
    } catch (IOException e) {
      log.warn("Error moving file to destination directory", e);
    }
  }

  @Override
  public Iterator<Document> processFile(Path path) throws Exception {
    // check if path from local file exists
    File pathFile = new File(path.toString());
    if (!pathFile.exists()) {
      throw new ConnectorException("Path " + path + " does not exist");
    }

    CSVReader reader = getCsvReader(path.toString());
    // reader will be closed when iterator hasNext() returns false or if any error occurs during iteration
    return getDocumentIterator(reader, pathFile.getName(), path.toAbsolutePath().normalize().toString());
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws Exception {
    CSVReader reader = getCsvReader(fileContent);
    // reader will be closed when iterator hasNext() returns false or if any error occurs during iteration
    // TODO: add path? For setting path of csv file to path field
    return getDocumentIterator(reader, FilenameUtils.getName(pathStr), pathStr);
  }


  private Iterator<Document> getDocumentIterator(CSVReader reader, String filename, String path) throws Exception {
    return new Iterator<Document>() {;
      final CSVReader csvReader = reader;
      final String[] header = getHeaderFromCSV(csvReader);
      final HashMap<String, Integer> columnIndexMap = getColumnIndexMap(header);
      final List<Integer> idColumns = getIdColumns(columnIndexMap);
      Integer lineNum = 0;
      String[] line;
      final Iterator<String[]> csvIterator = csvReader.iterator();

      @Override
      public boolean hasNext() {
        boolean hasNext = csvIterator.hasNext();
        if (!hasNext) {
          try {
            csvReader.close();
          } catch (IOException e) {
            log.error("Error closing CSVReader", e);
          }
        }
        return hasNext;
      }

      @Override
      public Document next() {
        try {
          if (!hasNext()) {
            log.warn("No more lines to process");
            return null;
          }

          line = csvIterator.next();
          lineNum++;

          if (line.length == 0 || (line.length == 1 && StringUtils.isBlank(line[0]))) {
            log.warn("Skipping blank line {}", lineNum);
            return next();
          }
          if (line.length != header.length) {
            // the line/row number reported here may differ from the physical line number in the file, if the CSV contains
            // a quoted value that spans multiple lines
            log.warn("Logical row {} of the csv has a different number of columns than the header. Skipping line.", lineNum);
            return next();
          }

          return getDocumentFromLine(idColumns, header, line, filename, path, lineNum);
        } catch (Exception e) {
          log.error("Error processing CSV line {}", lineNum, e);
          try {
            csvReader.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
        return null;
      }
    };
  }

  @Override
  public void beforeProcessingFile(Config config, Path path) throws Exception {
    createProcessedAndErrorFoldersIfSet();
  }

  @Override
  public void afterProcessingFile(Config config, Path path) throws Exception {
    config = config.getConfig("csv");
    if (config.hasPath("moveToAfterProcessing")) {
      // move to processed folder
      moveFile(path.toAbsolutePath().normalize(), config.getString("moveToAfterProcessing"));
    }
  }

  @Override
  public void errorProcessingFile(Config config, Path path) {
    config = config.getConfig("csv");
    if (config.hasPath("moveToErrorFolder")) {
      // move to error folder
      moveFile(path.toAbsolutePath().normalize(), config.getString("moveToErrorFolder"));
    }
  }

  public void moveFile(Path absolutePath, String option) {
    if (absolutePath.startsWith("classpath:")) {
      log.warn("Skipping moving classpath file: {} to {}", absolutePath, moveToAfterProcessing);
      return;
    }

    String fileName = absolutePath.getFileName().toString();
    Path dest = Paths.get(option + File.separatorChar + fileName);
    try {
      Files.move(absolutePath, dest);
      log.info("File {} was successfully moved from source {} to destination {}", fileName, absolutePath, dest);
    } catch (IOException e) {
      log.warn("Error moving file to destination directory", e);
    }
  }

  private Document getDocumentFromLine(List<Integer> idColumns, String[] header, String[] line, String filename, String path, int lineNum) {
    String docId = "";
    if (!idColumns.isEmpty()) {
      // let's get the columns with the values for the id.
      ArrayList<String> idColumnData = new ArrayList<String>();
      for (Integer idx : idColumns) {
        idColumnData.add(line[idx]);
      }
      docId = createDocId(idColumnData);
    } else {
      // a default unique id for a csv file is filename + line num
      docId = getDocIdPrefix() + filename + "-" + lineNum;
    }

    Document doc = Document.create(docId);
    if (StringUtils.isNotBlank(path)) {
      doc.setField(filePathField, path);
    }
    doc.setField(filenameField, filename);
    // log.info("DOC ID: {}", docId);
    int maxIndex = Math.min(header.length, line.length);
    for (int i = 0; i < maxIndex; i++) {
      if (line[i] != null && !ignoredTerms.contains(line[i]) && !Document.RESERVED_FIELDS.contains(header[i])) {
        doc.setField(header[i], line[i]);
      }
    }
    doc.setField(lineNumField, lineNum);

    return doc;
  }

  private void createProcessedAndErrorFoldersIfSet() {
    if (moveToAfterProcessing != null) {
      // Create the destination directory if it doesn't exist.
      File destDir = new File(moveToAfterProcessing);
      if (!destDir.exists()) {
        log.info("Creating archive directory {}", destDir.getAbsolutePath());
        destDir.mkdirs();
      }
    }
    log.info("Error folder: {}", moveToErrorFolder);
    if (moveToErrorFolder != null) {
      File errorDir = new File(moveToErrorFolder);
      if (!errorDir.exists()) {
        log.info("Creating error directory {}", errorDir.getAbsolutePath());
        errorDir.mkdirs();
      }
    }
  }

  private List<Integer> getIdColumns(HashMap<String, Integer> columnIndexMap) {
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
    return idColumns;
  }

  private HashMap<String, Integer> getColumnIndexMap(String[] header) {
    HashMap<String, Integer> columnIndexMap = new HashMap<String, Integer>();
    for (int i = 0; i < header.length; i++) {
      // check for BOM
      if (i == 0) {
        header[i] = removeBOM(header[i]);
      }

      if (columnIndexMap.containsKey(header[i])) {
        log.warn("Multiple columns with the name {} were discovered in the source csv file.", header[i]);
        continue;
      }
      columnIndexMap.put(header[i], i);
    }
    return columnIndexMap;
  }

  private String[] getHeaderFromCSV(CSVReader csvReader) throws ConnectorException {
    try {
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        try {
          csvReader.close();
        } catch (IOException e) {
          log.error("Error closing CSVReader", e);
        }
        log.warn("CSV does not contain header row, skipping csv file");
      }
      if (lowercaseFields) {
        for (int i = 0; i < header.length; i++) {
          header[i] = header[i].toLowerCase();
        }
      }
      return header;
    } catch (IOException | CsvValidationException e) {
      try {
        csvReader.close();
      } catch (IOException ex) {
        log.error("Error closing CSVReader", ex);
      }
      throw new ConnectorException("Error reading header from CSV", e);
    }
  }

  public CSVReader getCsvReader(String filePath) throws ConnectorException {
    String filename = new File(filePath).getName();
    try {
      return new CSVReaderBuilder(FileUtils.getReader(filePath)).
          withCSVParser(
              new CSVParserBuilder().withSeparator(separatorChar).withQuoteChar(quoteChar).withEscapeChar(escapeChar).build())
          .build();
    } catch (IOException e) {
      throw new ConnectorException("Error creating CSVReader for file " + filename, e);
    }
  }

  public CSVReader getCsvReader(byte[] fileContent) throws ConnectorException {
    try {
      return new CSVReaderBuilder(new InputStreamReader(new ByteArrayInputStream(fileContent), StandardCharsets.UTF_8))
          .withCSVParser(
              new CSVParserBuilder().withSeparator(separatorChar).withQuoteChar(quoteChar).withEscapeChar(escapeChar).build())
          .build();
    } catch (Exception e) {
      throw new ConnectorException("Error creating CSVReader from byte array", e);
    }
  }

}
