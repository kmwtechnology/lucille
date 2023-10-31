package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ParseDateTest {

  private StageFactory factory = StageFactory.of(ParseDate.class);

  @Test
  public void testParseDate() throws Exception {
    Stage stage = factory.get("ParseDateTest/config.conf");

    // Ensure that dates are correctly extracted
    Document doc = Document.create("doc");
    doc.setField("date1", "February 2, 2021");
    stage.processDocument(doc);
    assertEquals("2021-02-02T00:00:00Z", doc.getStringList("output1").get(0));

    // Ensure that several dates can be extracted in one pass, in different formats.
    Document doc2 = Document.create("doc2");
    doc2.setField("date1", "2003|10|25");
    doc2.setField("date2", "2020-2050");
    stage.processDocument(doc2);
    assertEquals("2003-10-25T00:00:00Z", doc2.getStringList("output1").get(0));
    assertEquals("2020-01-01T00:00:00Z", doc2.getStringList("output2").get(0));

    // Test parsing dates from a format String
    Document doc3 = Document.create("doc3");
    doc3.setField("date1", "90/Jul/17");
    doc3.setField("date2", "2023-06-21");
    stage.processDocument(doc3);
    assertEquals("1990-07-17T00:00:00Z", doc3.getStringList("output1").get(0));
    assertEquals("2023-06-21T00:00:00Z", doc3.getStringList("output2").get(0));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ParseDateTest/config.conf");
    assertEquals(
        Set.of(
            "formatters",
            "update_mode",
            "name",
            "format_strs",
            "source",
            "dest",
            "conditions",
            "class",
            "time_zone_id"),
        stage.getLegalProperties());
  }

  @Test
  public void testInvalidTimeZoneId() {
    Throwable exception = assertThrows(StageException.class, () -> factory.get("ParseDateTest/invalidTimeZoneConfig.conf"));
    Throwable cause = exception.getCause().getCause();
    assertEquals(IllegalArgumentException.class, cause.getClass());
    assertEquals("Invalid time zone ID: \"invalid\", must be one of TimeZone.getAvailableIDs()", cause.getMessage());
  }

  @Test
  public void testParseDateWithTimeZone() throws StageException {

    // create a test config without a time zone
    HashMap<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date"));
    configValues.put("dest", List.of("output"));
    configValues.put("format_strs", List.of("yyyy-MM-dd"));

    // create a map of time zones to expected formatted dates
    Map<String, String> timeZoneToFormattedDate = Map.of(
        "EST", "2021-02-02T00:00:00Z",
        "Universal", "2021-02-02T00:00:00Z",
        "Europe/Rome", "2021-02-01T00:00:00Z",
        "Australia/Brisbane", "2021-02-01T00:00:00Z"
    );

    for (Map.Entry<String, String> entry: timeZoneToFormattedDate.entrySet()) {

      // get time zone id from the map
      String timeZoneId = entry.getKey();

      // add a time zone to the config values map
      configValues.put("time_zone_id", timeZoneId);

      // create a stage off the config values map
      Stage stage = factory.get(configValues);

      // create a document with a date field
      Document doc = Document.create("doc");
      doc.setField("date", "2021-02-02");

      // parse date and confirm it matches the expected formatted date
      stage.processDocument(doc);
      assertEquals("Failed for time_zone_id=" + timeZoneId,
          entry.getValue(), doc.getString("output"));
    }
  }

  /* Examples of parsing date step by step based on a string and timezone

  Example 1:

    String stringDate = "2021-02-02";
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("EST"));

    Date date = format.parse(stringDate, new ParsePosition(0));
    assertEquals("Tue Feb 02 00:00:00 EST 2021", date.toString());

    LocalDate localDate = date.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
    assertEquals("2021-02-02T05:00:00Z", date.toInstant().toString());
    assertEquals("2021-02-02T05:00Z[UTC]", date.toInstant().atZone(ZoneId.of("UTC")).toString());
    assertEquals("2021-02-02", localDate.toString());

    String outputDate = DateTimeFormatter.ISO_INSTANT.format(localDate.atStartOfDay().toInstant(ZoneOffset.UTC));
    assertEquals("2021-02-02T00:00", localDate.atStartOfDay().toString());
    assertEquals("2021-02-02T00:00:00Z", localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toString());
    assertEquals("2021-02-02T00:00:00Z", outputDate);

  Example 2:

    String stringDate = "2021-02-02";
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("PST"));

    Date date = format.parse(stringDate, new ParsePosition(0));
    assertEquals("Tue Feb 02 03:00:00 EST 2021", date.toString());

    LocalDate localDate = date.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
    assertEquals("2021-02-02T08:00:00Z", date.toInstant().toString());
    assertEquals("2021-02-02T08:00Z[UTC]", date.toInstant().atZone(ZoneId.of("UTC")).toString());
    assertEquals("2021-02-02", localDate.toString());

    String outputDate = DateTimeFormatter.ISO_INSTANT.format(localDate.atStartOfDay().toInstant(ZoneOffset.UTC));
    assertEquals("2021-02-02T00:00", localDate.atStartOfDay().toString());
    assertEquals("2021-02-02T00:00:00Z", localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toString());
    assertEquals("2021-02-02T00:00:00Z", outputDate);

  Example 3:

    String stringDate = "2021-02-02";
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("Europe/Rome"));

    Date date = format.parse(stringDate, new ParsePosition(0));
    assertEquals("Mon Feb 01 18:00:00 EST 2021", date.toString());

    LocalDate localDate = date.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
    assertEquals("2021-02-01T23:00:00Z", date.toInstant().toString());
    assertEquals("2021-02-01T23:00Z[UTC]", date.toInstant().atZone(ZoneId.of("UTC")).toString());
    assertEquals("2021-02-01", localDate.toString());

    String outputDate = DateTimeFormatter.ISO_INSTANT.format(localDate.atStartOfDay().toInstant(ZoneOffset.UTC));
    assertEquals("2021-02-01T00:00", localDate.atStartOfDay().toString());
    assertEquals("2021-02-01T00:00:00Z", localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toString());
    assertEquals("2021-02-01T00:00:00Z", outputDate);

   */

  @Test
  public void testTemp() throws StageException {
    HashMap<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date1", "date2"));
    configValues.put("dest", List.of("output1", "output2"));
    configValues.put("format_strs", List.of("yyyy-MM-dd z", "yyyy-MM-dd"));
    configValues.put("time_zone_id", "Europe/Rome");

    // one date has a time zone, the other does not
    Document doc = Document.create("doc");
    doc.setField("date1", "2021-02-02");
    doc.setField("date2", "2021-02-02 EST");

    // for date with no timezone the time zone is set to the default time zone
    factory.get(configValues).processDocument(doc);
    assertEquals("2021-02-01T00:00:00Z", doc.getString("output1"));
    assertEquals("2021-02-02T00:00:00Z", doc.getString("output2"));

    // switch the order of format strings
    configValues.put("format_strs", List.of("yyyy-MM-dd", "yyyy-MM-dd z"));

    // todo notice that the time zone is set to the default for a date that has a timezone
    factory.get(configValues).processDocument(doc);
    assertEquals("2021-02-01T00:00:00Z", doc.getString("output1"));
    assertEquals("2021-02-01T00:00:00Z", doc.getString("output2"));
  }
}
