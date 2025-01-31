package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileIterator implements Iterator<Document> {

  private static final Logger log = LoggerFactory.getLogger(ParquetFileIterator.class);

  private final Configuration configuration;
  private final String idField;

  private final RemoteIterator<LocatedFileStatus> statusIterator;
  private LocatedFileStatus currentStatus;
  private long start;
  private final long limit;
  private long count = 0L;

  // File-level vars
  // Close this reader before reassigning it / when the iterator doesn't have next.
  private ParquetFileReader reader;
  private MessageType schema;
  private List<Type> fields;

  // page level vars
  private PageReadStore pages;
  private long nRows = 0;
  private MessageColumnIO columnIO;
  private RecordReader<Group> recordReader;

  public ParquetFileIterator(RemoteIterator<LocatedFileStatus> statusIterator, Configuration configuration, String idField, long start, long limit) {
    this.statusIterator = statusIterator;
    this.configuration = configuration;
    this.idField = idField;
    this.start = start;
    this.limit = limit;
  }

  @Override
  public boolean hasNext() {
    // First, just check if limit has been reached.
    if (limitReached()) {
      try {
        reader.close();
      } catch (IOException e) {
        log.warn("Error closing ParquetFileReader:", e);
      }
      return false;
    }

    try {
      // 1. If we have more rows to operate on, then yes, hasNext.
      // 2. If pages isn't null, we have a chance to call fetchNewPages() and get
      // a new page.
      // 3. If statusIterator has a next file for us to try on, then yes, hasNext.

      // Note that these don't guarantee whether we will actually be able to EXTRACT a document.
      // But these indicate whether we have more mutations we want next() to do, potentially
      // returning null in the process.
      return nRows > 0
          || pages != null
          || statusIterator.hasNext();
    } catch (IOException e) {

      // Error calling hasNext on statusIterator --> close resources.
      try {
        reader.close();
      } catch (IOException ioExc) {
        log.warn("Error closing ParquetFileReader:", ioExc);
      }

      return false;
    }
  }

  @Override
  public Document next() {
    // If the limit is reached, next() should not have been called - no chances
    // to check for new pages/iterators etc.
    if (limitReached()) {
      throw new NoSuchElementException("Requested document from Parquet, but limit reached.");
    }

    if (nRows-- <= 0
        || fetchNewPages()
        || fetchNewFileAndOpenReader()) {
      SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

      if (canSkipAndUpdateStart(1)) {
        return null;
      }

      return createDocumentFromSimpleGroup(simpleGroup);
    } else {
      // We don't close resources here. Allow hasNext() to handle this.
      // (Caller doesn't know if this indicates a skipped document or reaching the end of possible
      // files to parse and return...)
      return null;
    }
  }

  private boolean limitReached() {
    return limit >= 0 && count >= limit;
  }

  // After running out of rows to work with, tries to read the next row group (if possible),
  // skips the row if needed, and then updates columnIO, recordReader, simpleGroup, id, and doc.
  // (also checks for whether we should skipAndUpdateStart(nRows))
  private boolean fetchNewPages() {
    try {
      while ((pages = reader.readNextRowGroup()) != null) {
        nRows = pages.getRowCount();

        if (canSkipAndUpdateStart(nRows)) {
          continue;
        }

        columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        // Now we can call next() and build/return documents.
        return true;
      }
    } catch (IOException e) {
      log.warn("Error reading row group from Parquet File:", e);
    }

    // return false if out of pages or got an exception from reader.
    return false;
  }

  // Tries to get the next file status from the status iterator, and accordingly
  // open the file and modify schema/fields/initialize pages and nRows (read next row). Continues
  // iterating thru the file statuses until we have successful pages that don't need to be skipped.
  // Resources for this iterator should be released if this function returns false.
  private boolean fetchNewFileAndOpenReader() {
    try {
      while (statusIterator.hasNext()) {
        currentStatus = statusIterator.next();

        if (!currentStatus.getPath().getName().endsWith("parquet")) {
          continue;
        }

        if (reader != null) {
          reader.close();
        }

        try {
          reader = ParquetFileReader.open(HadoopInputFile.fromStatus(currentStatus, configuration));
        } catch (IOException e) {
          // If there is an error opening the FileReader for a file, skip to next.
          continue;
        }

        if (canSkipAndUpdateStart(reader.getRecordCount())) {
          continue;
        }

        schema = reader.getFooter().getFileMetaData().getSchema();
        fields = schema.getFields();
        pages = null;

        // If we can't fetch new pages from this file, then try another...
        if (!fetchNewPages()) {
          continue;
        }

        return true;
      }
    } catch (IOException e) {
      log.error("Error iterating on files:", e);
    }

    // False in the event of an exception or ran out of files to iterate over.
    return false;
  }

  private Document createDocumentFromSimpleGroup(SimpleGroup simpleGroup) {
    String id = simpleGroup.getString(idField, 0);
    Document doc = Document.create(id);

    for (int j = 0; j < fields.size(); j++) {
      Type field = fields.get(j);
      String fieldName = field.getName();
      if (fieldName.equals(idField)) {
        continue;
      }
      if (field.isPrimitive()) {
        ParquetFileDocUtils.setDocField(doc, field, simpleGroup, j);
      } else {
        for (int k = 0; k < simpleGroup.getGroup(j, 0).getFieldRepetitionCount(0); k++) {
          Group group = simpleGroup.getGroup(j, 0).getGroup(0, k);
          Type type = group.getType().getType(0);
          if (type.isPrimitive()) {
            ParquetFileDocUtils.addToField(doc, fieldName, type, group);
          }
        }
      }
    }

    count++;
    return doc;
  }

  private boolean canSkipAndUpdateStart(long n) {
    if (start > n) {
      start -= n;
      return true;
    }
    return false;
  }
}
