package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Adds random Dates to documents given parameters.
 *
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>field_name</b> (String, Optional) : Field name of field where data is placed. Defaults to "data"
 * </p>
 * <p>
 * <b>range_start_date</b> (String, Optional) : Date string in ISO format (2024-12-18) representing the start of the range for
 * generating random dates. Defaults to the start of Epoch time, 1970-1-1
 * </p>
 * <p>
 * <b>range_end_date</b> (String, Optional) : Date string in ISO format (2024-12-18) representing the end of the range for
 * generating random dates. Defaults to today's date/time.
 * </p>
 */
public class AddRandomDate extends Stage {

  private final String fieldName;
  private final String rangeStartDateString;
  private final String rangeEndDateString;

  private Date rangeStartDate;
  private Date rangeEndDate;

  public AddRandomDate(Config config) {
    super(config, Spec.stage()
        .withOptionalProperties("field_name", "range_start_date", "range_end_date"));

    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.rangeStartDateString = ConfigUtils.getOrDefault(config, "range_start_date", "");
    this.rangeEndDateString = ConfigUtils.getOrDefault(config, "range_end_date", "");
  }

  @Override
  public void start() throws StageException {
    rangeStartDate = rangeStartDateString.isEmpty() ? Date.from(Instant.ofEpochMilli(0)) : localDateToDate(LocalDate.parse(rangeStartDateString));
    rangeEndDate = rangeEndDateString.isEmpty() ? Date.from(Instant.now()) : localDateToDate(LocalDate.parse(rangeEndDateString));

    if (rangeStartDate.after(rangeEndDate)) {
      throw new StageException("The provided range_start_date is after the range_end_date.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    Date randomDate = Date.from(
        Instant.ofEpochMilli(ThreadLocalRandom.current().nextLong(
        rangeStartDate.getTime(), rangeEndDate.getTime()
    )));

    doc.setField(fieldName,  randomDate);

    return null;
  }

  private Date localDateToDate(LocalDate ld) {
    return Date.from(
        ld.atStartOfDay().toInstant(ZoneOffset.UTC)
    );
  }
}
