package com.kmwllc.lucille.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A BufferedReader whose {@link #readLine()} throws an IOException once the current record exceeds a maximum number of
 * bytes, rather than reading the entire record into memory.
 * <p>
 * A record is everything read by {@link #readLine()} since the last call to {@link #startRecord()}, so a record can span
 * several lines (e.g. a CSV row with a quoted value containing line breaks). Size is measured as the UTF-8 encoding of the
 * characters read, not including line terminators.
 * <p>
 * Line terminators are the same as {@link BufferedReader#readLine()}: '\n', '\r', or "\r\n". Only {@link #readLine()}
 * enforces the limit; the other read methods behave as in BufferedReader.
 */
public class BoundedLineReader extends BufferedReader {

  private final long maxRecordBytes;
  private long recordBytes = 0;

  // true when the previous line ended with '\r', so a following '\n' belongs to the same terminator
  private boolean skipLF = false;

  /**
   * Creates a BoundedLineReader.
   *
   * @param in The Reader to read from.
   * @param maxRecordBytes The maximum number of bytes allowed in a record.
   */
  public BoundedLineReader(Reader in, long maxRecordBytes) {
    super(in);

    if (maxRecordBytes <= 0) {
      throw new IllegalArgumentException("maxRecordBytes must be positive, got " + maxRecordBytes);
    }

    this.maxRecordBytes = maxRecordBytes;
  }

  /**
   * Starts a new record, resetting the number of bytes counted against the limit.
   */
  public void startRecord() {
    recordBytes = 0;
  }

  @Override
  public String readLine() throws IOException {
    StringBuilder line = new StringBuilder();
    int c;

    while ((c = read()) != -1) {
      if (skipLF) {
        skipLF = false;
        if (c == '\n') {
          continue;
        }
      }

      if (c == '\n') {
        return line.toString();
      }

      if (c == '\r') {
        skipLF = true;
        return line.toString();
      }

      recordBytes += utf8Length((char) c);
      if (recordBytes > maxRecordBytes) {
        throw new RecordTooLargeException(maxRecordBytes);
      }

      line.append((char) c);
    }

    return line.isEmpty() ? null : line.toString();
  }

  // the number of bytes the char takes in UTF-8; each half of a surrogate pair counts as 2, so the pair counts as 4
  private static int utf8Length(char c) {
    if (c < 0x80) {
      return 1;
    } else if (c < 0x800 || Character.isSurrogate(c)) {
      return 2;
    } else {
      return 3;
    }
  }

  /**
   * Thrown by {@link BoundedLineReader#readLine()} when the current record exceeds the maximum size.
   */
  public static class RecordTooLargeException extends IOException {

    public RecordTooLargeException(long maxRecordBytes) {
      super("Record exceeds the maximum size of " + maxRecordBytes + " bytes.");
    }
  }
}
