package com.kmwllc.lucille.core.fileHandler;

import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file handler for CSV files. Extracts documents from the rows of a CSV file.
 *
 * <p>A row that cannot be turned into a Document is logged and skipped, and the following rows are still processed.
 * This covers a blank line, a row with the wrong number of columns, and a row whose id, built from the idFields, is
 * blank as a whole. An id with only some blank parts, such as "2_" from two columns, is usable. If 10 rows in a row cannot
 * be read the file fails, because the cause is then likely a wrong configuration or the wrong file format.
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
  private final Set<String> ignoredTerms;
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
    this.ignoredTerms = config.hasPath("ignoredTerms") ? Set.copyOf(config.getStringList("ignoredTerms")) : Set.of();
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
      final BadRecordTracker tracker = new BadRecordTracker(path);
      boolean finished = false;
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
          if (!finished) {
            finished = true;
            tracker.logSummary(lineNum);
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

          // rows that cannot be turned into a Document are skipped, so keep reading until there is one, or no more rows
          while (true) {
            line = csvIterator.next();
            lineNum++;

            if (line.length == 0 || (line.length == 1 && StringUtils.isBlank(line[0]))) {
              log.warn("Skipping blank line {}. ({})", lineNum, path);
            } else if (line.length != header.length) {
              // a row with the wrong number of columns is skipped like any other bad record, so that a file where every row
              // mismatches (for example a header with a trailing empty cell) fails instead of yielding no documents.
              // The row number reported here may differ from the physical line number if a quoted value spans lines.
              tracker.bad(lineNum, String.format("row has %d columns but the header has %d", line.length, header.length));
            } else {
              try {
                Document doc = getDocumentFromLine(idColumns, header, line, filename, path, lineNum);
                tracker.good();
                return doc;
              } catch (DocumentException | IllegalArgumentException e) {
                // a problem with this row's data: skip it and carry on with the next row
                tracker.bad(lineNum, e.getMessage());
              }
            }

            if (!hasNext()) {
              // the remaining rows were all skipped; the caller treats null as "no document" and then sees hasNext() is false
              return null;
            }
          }
        } catch (BadRecordTracker.TooManyBadRecordsException e) {
          log.error("Error processing CSV line {}. ({})", lineNum, path, e);
          closeQuietly();
          throw e;
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

      private void closeQuietly() {
        try {
          csvReader.close();
        } catch (IOException e) {
          log.error("Error closing CSVReader. ({})", path, e);
        }
      }
    };
  }

  private Document getDocumentFromLine(List<Integer> idColumns, String[] header, String[] line, String filename, String path, int lineNum)
      throws DocumentException {
    String docId = "";
    if (!idColumns.isEmpty()) {
      // let's get the columns with the values for the id.
      ArrayList<String> idColumnData = new ArrayList<String>();
      for (Integer idx : idColumns) {
        idColumnData.add(line[idx]);
      }

      for (int fieldIndex = 0; fieldIndex < idColumnData.size(); fieldIndex++) {
        String fieldValue = idColumnData.get(fieldIndex);
        if (StringUtils.isBlank(fieldValue)) {
          String fieldName = (fieldIndex < idFields.size()) ? idFields.get(fieldIndex) : ("index:" + fieldIndex);
          log.warn("Missing/blank idField {} at line {}. ({})", fieldName, lineNum, path);
        }
      }

      String rawId = rawDocId(idColumnData);
      // an id with only some blank parts, such as "2_", is usable; one that is blank as a whole is not
      if (!Document.isValidId(rawId)) {
        throw new DocumentException("the id built from columns " + idFields + " is blank");
      }
      docId = createDocId(rawId);
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

  /**
   * Maps the configured idFields to column positions. An idField that is not in the header is a configuration error that
   * would otherwise make every row unusable, so it fails the file up front instead of being reported row by row.
   */
  private List<Integer> getIdColumns(HashMap<String, Integer> columnIndexMap, String path) throws FileHandlerException {
    List<Integer> idColumns = new ArrayList<Integer>();
    // an empty file has no header; there are no rows to build ids for, so there is nothing to check
    if (columnIndexMap.isEmpty()) {
      return idColumns;
    }
    for (String field : idFields) {
      String columnName = lowercaseFields ? field.toLowerCase() : field;
      Integer index = columnIndexMap.get(columnName);
      if (index == null) {
        throw new FileHandlerException(String.format(
            "idField '%s' is not a column in the CSV header %s; check the idField/idFields configuration and lowercaseFields. (%s)",
            field, Arrays.toString(columnIndexMap.keySet().toArray()), path));
      }
      idColumns.add(index);
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
      if (lowercaseFields && header != null) {
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

  // builds the id from the values of the id columns, before the doc id prefix is added
  private String rawDocId(ArrayList<String> idColumnData) {
    if (docIdFormat != null) {
      return String.format(docIdFormat, idColumnData.toArray());
    } else {
      // Join the column data with an underscore if no docIdFormat provided.
      return StringUtils.join(idColumnData, "_");
    }
  }

  // TODO: synchronize with abstractConnector
  private String createDocId(String id) {
    return docIdPrefix + id;
  }

}
