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

/**
 * An iterator of Lucille Documents extracted from a ParquetFileReader. Resources are closed when hasNext returns false.
 */
public class ParquetFileIterator implements Iterator<Document> {

  private static final Logger log = LoggerFactory.getLogger(ParquetFileIterator.class);

  private final String idField;
  private final long limit;
  private long start;
  private long count = 0L;

  // File-level vars
  private final ParquetFileReader reader;
  private final MessageType schema;
  private final List<Type> fields;

  // page level vars
  private PageReadStore pages;
  private long nRows = 0;
  private RecordReader<Group> recordReader;

  /**
   * @param reader A reader for a Parquet file from which Lucille documents will be extracted. The reader will
   *               be closed by the iterator when hasNext(), or if next() throws an Exception.
   * @param idField An id field which can be found in the Parquet file. This field must be present in the Parquet schema,
   *                or an Exception will be thrown while iterating.
   * @param start The index to start at. When set to n, the first n documents found will be skipped.
   * @param limit The maximum number of Lucille documents to extract and return from the given ParquetReader. Set to -1 for no limit.
   */
  public ParquetFileIterator(ParquetFileReader reader, String idField, long start, long limit) {
    this.reader = reader;
    this.schema = reader.getFileMetaData().getSchema();
    this.fields = schema.getFields();

    this.idField = idField;
    this.start = start;
    this.limit = limit;

    if (!canSkipAndUpdateStart(reader.getRecordCount())) {
      // As long as we shouldn't skip this ENTIRE document, start with pages / nRows initialized,
      // and hasNext() will return true.
      // If we are supposed to skip this entire document, then this won't execute, and hasNext() will return false.
      readNewPages();
    }
  }

  @Override
  public boolean hasNext() {
    if (limitReached() || pages == null) {
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
    if (!hasNext()) {
      throw new NoSuchElementException("Requested document from Parquet, but limit reached.");
    }

    // As long as we have rows to work on or can fetch new pages (which makes sure we have rows to work on)
    while (nRows > 0) {
      nRows--;
      // read record regardless of whether we will skip it...
      SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

      // if this was the last row in the page, try to fetch a new page.
      // (before checking if we should skip this row)
      if (nRows <= 0) {
        readNewPages();
      }

      if (canSkipAndUpdateStart(1)) {
        continue;
      }

      return createDocumentFromSimpleGroup(simpleGroup);
    }

    // This isn't going to occur, but I don't think an exception is needed.
    // hasNext() will be false after this, so the iterator will be able to close.
    log.warn("ParquetFileIterator.next() is returning null.");
    return null;
  }

  /**
   * Attempt to read the next row group from the Parquet reader. Updates nRows and recordReader when successful. Will read another
   * rowGroup and update start if the group has less rows than start. If there are no more rowGroups to read from, or an exception
   * occurs, pages will be null.
   */
  private void readNewPages() {
    try {
      while ((pages = reader.readNextRowGroup()) != null) {
        nRows = pages.getRowCount();

        // checking if we can skip this row group.
        // rowGroups always will have at least one row - an exception would be thrown by Parquet.
        if (canSkipAndUpdateStart(nRows)) {
          continue;
        }

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        // Now we can call next() and build/return documents.
        return;
      }
    } catch (IOException e) {
      log.warn("Error reading row group from Parquet File:", e);
      // Ensures hasNext() will be false. (don't hold onto the old pages...)
      pages = null;
    }
  }

  // Creates a document from the given SimpleGroup, and increments count.
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
        ParquetDocUtils.setDocField(doc, field, simpleGroup, j);
      } else {
        for (int k = 0; k < simpleGroup.getGroup(j, 0).getFieldRepetitionCount(0); k++) {
          Group group = simpleGroup.getGroup(j, 0).getGroup(0, k);
          Type type = group.getType().getType(0);
          if (type.isPrimitive()) {
            ParquetDocUtils.addToField(doc, fieldName, type, group);
          }
        }
      }
    }

    count++;
    return doc;
  }

  // Returns whether the current count is greater than the limit. Always returns true if limit is a negative number.
  private boolean limitReached() {
    return limit >= 0 && count >= limit;
  }

  /**
   * Returns whether n document(s) should be skipped, based on the start parameter. If the n document(s) should be skipped,
   * start is decreased by n.
   */
  private boolean canSkipAndUpdateStart(long n) {
    if (start >= n) {
      start -= n;
      return true;
    }
    return false;
  }
}
