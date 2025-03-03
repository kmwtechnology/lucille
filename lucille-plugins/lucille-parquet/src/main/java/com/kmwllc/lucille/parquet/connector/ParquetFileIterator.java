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
  private long numToSkip;
  private long count = 0L;

  // File-specific info / fields.
  private final ParquetFileReader reader;
  private final MessageType schema;
  private final List<Type> fields;

  // page-specific info - these are modified as we read new sets of pages from the file.
  // pages being set to null is the primary indicator that the iterator has been exhausted.
  // (reaching the limit is the other condition causing the iterator to close.)
  private PageReadStore pages;
  private long nRows = 0L;
  private RecordReader<Group> recordReader;

  /**
   * @param reader A reader for a Parquet file from which Lucille documents will be extracted. The reader will
   *               be closed by the iterator when hasNext(), or if next() throws an Exception.
   * @param idField An id field found in the Parquet file. This field must be present in the Parquet schema,
   *                or an Exception is thrown.
   * @param numToSkip The number of initial rows to skip and not publish documents for. When set to n, the first n documents found will be skipped.
   * @param limit The maximum number of Lucille documents to extract and return from the given ParquetReader. Set to -1 for no limit.
   */
  public ParquetFileIterator(ParquetFileReader reader, String idField, long numToSkip, long limit) {
    this.reader = reader;
    this.schema = reader.getFileMetaData().getSchema();
    if (!schema.containsField(idField)) {
      throw new IllegalArgumentException("Schema for file does not contain idField " + idField + ".");
    }
    this.fields = schema.getFields();

    this.idField = idField;
    this.numToSkip = numToSkip;
    this.limit = limit;

    // Handles whether the entire file should be skipped / initializing recordReader/nRows so hasNext() starts true
    if (!shouldSkipNumDocs(reader.getRecordCount())) {
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
      throw new NoSuchElementException("Requested document from Parquet, but hasNext() is false.");
    }

    // As long as we have rows to work on
    while (nRows > 0) {
      nRows--;
      // read record regardless of whether we will skip it...
      SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

      // if this was the last row in the page, try to fetch a new page for our *next* iteration - which could be
      // from skipping this row or from a subsequent call to next().
      if (nRows <= 0) {
        readNewPages();
      }

      if (shouldSkipNumDocs(1)) {
        continue;
      }

      return createDocumentFromSimpleGroup(simpleGroup);
    }

    // This is unlikely to occur - a combination of skipping a row AND readNewPages() failing due to an IOException within the loop.
    // The constructor handles the case of start being greater than the number of rows to read, so this doesn't execute
    // simply due to "running out".
    // hasNext() WILL be false after this, so the iterator will be able to close.
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
        if (shouldSkipNumDocs(nRows)) {
          continue;
        }

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        // Now we are set up a call to next().
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
   * Returns whether n document(s) should be skipped, based on the numToSkip value.
   * If n document(s) should be skipped, start is decreased by n, and the function returns "true".
   */
  private boolean shouldSkipNumDocs(long n) {
    if (numToSkip >= n) {
      numToSkip -= n;
      return true;
    }
    return false;
  }
}
