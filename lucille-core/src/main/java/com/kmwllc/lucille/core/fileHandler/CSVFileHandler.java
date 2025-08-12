package com.kmwllc.lucille.core.fileHandler;

import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file handler for CSV files. Extracts documents from the rows of a CSV file.
 */
public class CSVFileHandler extends BaseFileHandler {

  public static final Spec SPEC = SpecBuilder.fileHandler()
      .optionalString("docIdPrefix", "lineNumberField", "separatorChar", "filenameField", "filePathField", "idField", "docIdFormat")
      .optionalList("idFields", new TypeReference<List<String>>() {})
      .optionalList("ignoredTerms", new TypeReference<List<String>>() {})
      .optionalBoolean("useTabs", "interpretQuotes", "ignoreEscapeChar", "lowercaseFields").build();

  private static final Logger log = LoggerFactory.getLogger(CSVFileHandler.class);

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
  private static final String UTF8_BOM = "\uFEFF";

  public CSVFileHandler(Config config) {
    super(config);

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
    this.ignoredTerms = config.hasPath("ignoredTerms") ? config.getStringList("ignoredTerms") : new ArrayList<>();
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    CSVReader csvReader = getCsvReader(inputStream, pathStr);
    String fileName = FilenameUtils.getName(pathStr);

    if (pathStr.contains(ARCHIVE_FILE_SEPARATOR)) {
      String entryName = pathStr.substring(pathStr.lastIndexOf(ARCHIVE_FILE_SEPARATOR) + 1);
      fileName = entryName.substring(entryName.lastIndexOf("/") + 1);
    }

    return getDocumentIterator(csvReader, fileName, pathStr);
  }

  private Iterator<Document> getDocumentIterator(CSVReader reader, String filename, String path) throws FileHandlerException {
    return new Iterator<Document>() {;
      final CSVReader csvReader = reader;
      final String[] header = getHeaderFromCSV(csvReader, path);
      final HashMap<String, Integer> columnIndexMap = getColumnIndexMap(header, path);
      final List<Integer> idColumns = getIdColumns(columnIndexMap, path);
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
            log.error("Error closing CSVReader. ({})", path, e);
          }
        }
        return hasNext;
      }

      @Override
      public Document next() {
        try {
          if (!hasNext()) {
            log.warn("No more lines to process. ({})", path);
            throw new IllegalStateException(String.format("No more lines to process. (%s)", path));
          }

          line = csvIterator.next();
          lineNum++;

          if (line.length == 0 || (line.length == 1 && StringUtils.isBlank(line[0]))) {
            log.warn("Skipping blank line {}. ({})", lineNum, path);
            return next();
          }
          if (line.length != header.length) {
            // the line/row number reported here may differ from the physical line number in the file, if the CSV contains
            // a quoted value that spans multiple lines
            log.warn("Logical row {} has {} columns but the header has {}. Skipping line. ({})", lineNum, line.length, header.length, path);
            return next();
          }

          return getDocumentFromLine(idColumns, header, line, filename, path, lineNum);
        } catch (Exception e) {
          log.error("Error processing CSV line {}. ({})", lineNum, path, e);

          try {
            csvReader.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }

          return null;
        }
      }
    };
  }

  private Document getDocumentFromLine(List<Integer> idColumns, String[] header, String[] line, String filename, String path, int lineNum) {
    String docId = "";
    if (!idColumns.isEmpty()) {
      // let's get the columns with the values for the id.
      ArrayList<String> idColumnData = new ArrayList<String>();
      for (Integer idx : idColumns) {
        idColumnData.add(line[idx]);
      }

      List<String> blankIdFields = new ArrayList<>();
      for (int fieldIndex = 0; fieldIndex < idColumnData.size(); fieldIndex++) {
        String fieldValue = idColumnData.get(fieldIndex);
        if (StringUtils.isBlank(fieldValue)) {
          String fieldName = (fieldIndex < idFields.size()) ? idFields.get(fieldIndex) : ("index:" + fieldIndex);
          blankIdFields.add(fieldName);
        }
      }

      if (!blankIdFields.isEmpty()) {
        log.warn("Missing/blank idFields {} at logical row {}. ({})", blankIdFields, lineNum, path);
      }

      docId = createDocId(idColumnData);
    } else {
      // a default unique id for a csv file is filename + line num
      docId = docIdPrefix + filename + "-" + lineNum;
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

  private List<Integer> getIdColumns(HashMap<String, Integer> columnIndexMap, String path) {
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
      log.warn("Mismatch in idFields to column map. ({})", path);
    }
    return idColumns;
  }

  private HashMap<String, Integer> getColumnIndexMap(String[] header, String path) {
    HashMap<String, Integer> columnIndexMap = new HashMap<String, Integer>();

    // If header is null, just return an empty map. (Allows for graceful handling of empty files)
    if (header == null) {
      return columnIndexMap;
    }

    for (int i = 0; i < header.length; i++) {
      // check for BOM
      if (i == 0) {
        header[i] = removeBOM(header[i]);
      }

      if (columnIndexMap.containsKey(header[i])) {
        log.warn("Multiple columns with the name {} were discovered in the source csv file. ({})", header[i], path);
        continue;
      }
      columnIndexMap.put(header[i], i);
    }
    return columnIndexMap;
  }

  private String[] getHeaderFromCSV(CSVReader csvReader, String path) throws FileHandlerException {
    try {
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        try {
          csvReader.close();
        } catch (IOException e) {
          log.error("Error closing CSVReader. ({})", path, e);
        }
        log.warn("CSV does not contain header row, no Documents will be published for the file. ({})", path);
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
        log.error("Error closing CSVReader. ({})", path, ex);
      }
      throw new FileHandlerException(String.format("Error reading header from CSV. (%s)", path), e);
    }
  }

  private CSVReader getCsvReader(InputStream inputStream, String path) throws FileHandlerException {
    try {
      return new CSVReaderBuilder(new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)))
          .withCSVParser(
              new CSVParserBuilder().withSeparator(separatorChar).withQuoteChar(quoteChar).withEscapeChar(escapeChar).build())
          .build();
    } catch (Exception e) {
      throw new FileHandlerException(String.format("Error creating CSVReader from InputStream. (%s)", path), e);
    }
  }

  private String removeBOM(String s) {
    if (s.startsWith(UTF8_BOM)) {
      s = s.substring(1);
    }
    return s;
  }

  private String createDocId(ArrayList<String> idColumnData) {
    // format the string with the data and pre-pend the doc id prefix
    if (docIdFormat != null) {
      return createDocId(String.format(docIdFormat, idColumnData.toArray()));
    } else {
      // no doc id.. just choose the first value in the idColumnData
      // Join the column data with an underscore if no docIdFormat provided.
      String idData = StringUtils.join(idColumnData, "_");
      return createDocId(idData);
    }
  }

  // TODO: synchronize with abstractConnector
  private String createDocId(String id) {
    return docIdPrefix + id;
  }

}
