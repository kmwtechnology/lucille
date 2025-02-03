package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileIterator implements Iterator<Document> {

  private static final Logger log = LoggerFactory.getLogger(ParquetFileIterator.class);

  private final String idField;

  private long start;
  private final long limit;
  private long count = 0L;

  // File-level vars
  // TODO: Many of these can be final after making the changes.
  private final ParquetFileReader reader;
  private final MessageType schema;
  private final List<Type> fields;

  // page level vars
  private PageReadStore pages;
  private long nRows = 0;
  private MessageColumnIO columnIO;
  private RecordReader<Group> recordReader;

  // Make sure to note that the reader will be closed when hasNext() returns false.
  public ParquetFileIterator(ParquetFileReader reader, String idField, long start, long limit) {
    this.reader = reader;
    this.schema = reader.getFileMetaData().getSchema();
    this.fields = schema.getFields();

    this.idField = idField;
    this.start = start;
    this.limit = limit;

    // TODO: Add a check on canSkipAndUpdateStart(reader.getRecordCount())

    if (!canSkipAndUpdateStart(reader.getRecordCount())) {
      // As long as we shouldn't skip this document, start with pages / nRows initialized,
      // preventing hasNext() from returning false.
      fetchNewPages();
    }
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

    // Note that this doesn't guarantee whether we will actually be able to EXTRACT a document.

    // If pages is null, we had attempted to fetchNewPages (ran out of rows) and either
    // ran out or received an exception.
    if (pages == null) {
      try {
        reader.close();
      } catch (IOException e) {
        log.warn("Error closing ParquetFileReader:", e);
      }

      return false;
    } else {
      return true;
    }
  }

  @Override
  public Document next() {
    // If the limit is reached, next() should not have been called - no chances
    // to check for new pages/iterators etc.
    if (limitReached()) {
      throw new NoSuchElementException("Requested document from Parquet, but limit reached.");
    }

    // As long as we have rows to work on or can fetch new pages (which makes sure we have rows to work on)
    while (nRows-- > 0 || fetchNewPages()) {
      // read record regardless of the start parameter
      SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

      if (canSkipAndUpdateStart(1)) {
        continue;
      }

      return createDocumentFromSimpleGroup(simpleGroup);

      // TODO: Where to call fetchNewPages()
    }

    // We don't close resources here. Allow hasNext() to handle this.
    // (Caller doesn't know if this indicates a skipped document or reaching the end of possible
    // files to parse and return...)
    return null;
  }

  // After running out of rows to work with, tries to read the next row group (if possible),
  // skips the row if needed, and then updates columnIO, recordReader, simpleGroup, id, and doc.
  // (also checks for whether we should skipAndUpdateStart(nRows))
  // Returns whether a new page was read successfully. If false, there was either an IOException
  // reading from the Parquet file or there were no more rowGroups to read from.
  private boolean fetchNewPages() {
    try {
      while ((pages = reader.readNextRowGroup()) != null) {
        nRows = pages.getRowCount();

        // checking if we can skip this row group or it doesn't have rows for us to work on
        if (canSkipAndUpdateStart(nRows) || nRows <= 0) {
          continue;
        }

        columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        // Now we can call next() and build/return documents.
        return true;
      }
    } catch (IOException e) {
      log.warn("Error reading row group from Parquet File:", e);
      // Causes hasNext() to return false.
      // TODO: Better way to handle this?
      pages = null;
    }

    // return false if out of pages or got an exception from reader.
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

  private boolean limitReached() {
    return limit >= 0 && count >= limit;
  }

  private boolean canSkipAndUpdateStart(long n) {
    if (start > n) {
      start -= n;
      return true;
    }
    return false;
  }
}
