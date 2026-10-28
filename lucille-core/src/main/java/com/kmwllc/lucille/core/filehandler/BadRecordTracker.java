package com.kmwllc.lucille.core.filehandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lets a file handler skip the records of one file that cannot be turned into Documents, such as a JSON line that is
 * malformed or has an invalid id, and carry on with the next record.
 *
 * <p>Each skipped record is logged at ERROR. If {@link #MAX_CONSECUTIVE} records in a row are skipped, the cause is
 * assumed to be systemic, such as a wrong id configuration or a file in the wrong format, and {@link #bad} throws so
 * that the file fails instead of ingesting nothing while logging an error per record.
 */
final class BadRecordTracker {

  private static final Logger log = LoggerFactory.getLogger(BadRecordTracker.class);

  /** The number of consecutive bad records after which processing of the file fails. */
  static final int MAX_CONSECUTIVE = 10;

  private final String path;
  private long skipped = 0;
  private int consecutive = 0;

  BadRecordTracker(String path) {
    this.path = path;
  }

  /** Records that a record was read successfully. */
  void good() {
    consecutive = 0;
  }

  /**
   * Records that a record was skipped, and logs it.
   *
   * @param position the line or row number of the record
   * @param reason why the record was skipped
   * @throws TooManyBadRecordsException if this is the {@link #MAX_CONSECUTIVE}th bad record in a row
   */
  void bad(long position, String reason) {
    skipped++;
    consecutive++;
    log.error("Skipping record {}: {}. ({})", position, reason, path);
    if (consecutive >= MAX_CONSECUTIVE) {
      throw new TooManyBadRecordsException(String.format(
          "%d records in a row could not be read, ending at record %d. Check the file format and the id configuration. (%s)",
          consecutive, position, path));
    }
  }

  /** At the end of the file, logs how many of the lines read were skipped, if any were. */
  void logSummary(long linesRead) {
    if (skipped > 0) {
      log.warn("Skipped {} of {} lines that could not be read. ({})", skipped, linesRead, path);
    }
  }

  /** Thrown when too many consecutive records of a file could not be read. */
  static final class TooManyBadRecordsException extends IllegalStateException {
    TooManyBadRecordsException(String message) {
      super(message);
    }
  }
}
