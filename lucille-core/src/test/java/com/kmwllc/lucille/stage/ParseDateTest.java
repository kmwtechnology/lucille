package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import com.kmwllc.lucille.stage.dateformatters.DateMonthStrFormatter;
import com.kmwllc.lucille.stage.dateformatters.DatePipeFormatter;
import com.kmwllc.lucille.stage.dateformatters.DateTwoYearsFormatter;
import com.kmwllc.lucille.stage.dateformatters.UnixTimestampFormatter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.zone.ZoneRulesException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Test;

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
    assertEquals("2090-07-17T00:00:00Z", doc3.getStringList("output1").get(0));
    assertEquals("2023-06-21T00:00:00Z", doc3.getStringList("output2").get(0));

    Document doc4 = Document.create("doc4");
    doc4.setField("date1", "1696109846000");
    stage.processDocument(doc4);
    assertEquals("2023-09-30T21:37:26Z", doc4.getStringList("output1").get(0));

  }

  @Test
  public void testSpec() throws StageException {
    Stage stage = factory.get("ParseDateTest/config.conf");
    assertEquals(
        Set.of(
            "formatters",
            "updateMode",
            "formatStrs",
            "source",
            "dest",
            "timeZoneId",
            "locale"),
        stage.getNonDefaultLegalProperties());
  }

  @Test
  public void testInvalidTimeZoneId() {
    Throwable exception = assertThrows(StageException.class, () -> factory.get("ParseDateTest/invalidTimeZoneConfig.conf"));
    Throwable cause = exception.getCause().getCause();
    assertEquals(ZoneRulesException.class, cause.getClass());
  }

  @Test
  public void testParseDateWithTimeZone() throws StageException {

    // create a test config without a time zone
    Map<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date"));
    configValues.put("dest", List.of("output"));
    configValues.put("formatStrs", List.of("yyyy-MM-dd"));

    // create a map of time zones to expected formatted dates
    List<String> zones = List.of("America/New_York", "Universal", "Europe/Rome", "Australia/Brisbane");

    for (String zone : zones) {

      // add a time zone to the config values map
      configValues.put("timeZoneId", zone);

      // create a stage off the config values map
      Stage stage = factory.get(configValues);

      // create a document with a date field
      Document doc = Document.create("doc");
      doc.setField("date", "2021-02-02");

      // parse date and confirm it matches the expected formatted date
      stage.processDocument(doc);

      TemporalAccessor parsed = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd")
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .toFormatter()
          .withZone(ZoneId.of(zone)).parse("2021-02-02");
      String expected = DateTimeFormatter.ISO_INSTANT.format(parsed);

      assertEquals("Failed for timeZoneId=" + zone,
          expected, doc.getString("output"));
    }
  }

  @Test
  public void testParseDateWithTime() throws StageException {
    Map<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date"));
    configValues.put("dest", List.of("output"));
    configValues.put("formatStrs", List.of("yyyy-MM-dd'T'HH:mm:ssxxxxx[VV]"));
    String dateString = "2011-12-03T10:15:31+01:00:00[Europe/Paris]";

    Stage stage = factory.get(configValues);
    Document doc = Document.create("doc");
    doc.setField("date", dateString);
    stage.processDocument(doc);
    String expected = DateTimeFormatter.ISO_INSTANT.format(ZonedDateTime.parse(dateString,
        DateTimeFormatter.ISO_ZONED_DATE_TIME));
    assertEquals(expected, doc.getString("output"));
  }

  @Test
  public void testParseDateAndReconstruct() throws StageException {
    Random random = new Random();

    // iterate through all available time zones
    for (String zoneIdStr: ZoneId.getAvailableZoneIds()) {

      // create a ParseDate stage with the given timeZoneId specified in the config
      Map<String, Object> configValues = new HashMap<>();
      configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
      configValues.put("source", List.of("date"));
      configValues.put("dest", List.of("output"));
      configValues.put("formatStrs", List.of("yyyy-MM-dd"));
      configValues.put("timeZoneId", zoneIdStr);
      Stage stage = factory.get(configValues);

      // create a random date string in yyyy-MM-dd format and add it to a document
      int year = random.nextInt(2023 - 1900 + 1) + 1900;
      int month = random.nextInt(12) + 1;
      int maxDay = LocalDate.of(year, month, 1).lengthOfMonth();
      int day = random.nextInt(maxDay) + 1;
      LocalDate randomDate = LocalDate.of(year, month, day);
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
      String input = randomDate.format(formatter);
      Document doc = Document.create("doc");
      doc.setField("date", input);

      // process the document and get the output string which should
      // be in Zulu format and look like 2021-02-01T23:00:00Z
      stage.processDocument(doc);
      String output = doc.getString("output");

      // parse the output string into a ZonedDateTime which will be in UTC
      ZonedDateTime parsedOutput = DateTimeFormatter.ISO_ZONED_DATE_TIME.parse(output, ZonedDateTime::from);
      assertEquals(ZoneOffset.UTC, parsedOutput.getZone());

      // convert the UTC date to have the original time zone that the stage was configured with
      ZonedDateTime parsedOutputInOriginalZone = parsedOutput.withZoneSameInstant(ZoneId.of(zoneIdStr));

      // put it in yyyy-MM-dd format and make sure it matches the input provided to the stage
      // if the ParseDate logic weren't working right we could see an input like 1945-05-22 come out as 1945-05-21
      String reconstructedInput = parsedOutputInOriginalZone.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
      assertEquals(input, reconstructedInput);
    }
  }

  @Test
  public void testFormatStringOrder() throws StageException {
    Map<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date1", "date2"));
    configValues.put("dest", List.of("output1", "output2"));
    configValues.put("formatStrs", List.of("yyyy-MM-dd z", "yyyy-MM-dd"));
    configValues.put("timeZoneId", "Europe/Rome");

    // one date has a time zone, the other does not
    Document doc = Document.create("doc");
    doc.setField("date1", "2021-02-02");
    doc.setField("date2", "2021-02-02 EST");

    // for date with no timezone the time zone is set to the default time zone
    factory.get(configValues).processDocument(doc);
    assertEquals("2021-02-01T23:00:00Z", doc.getString("output1"));
    assertEquals("2021-02-02T05:00:00Z", doc.getString("output2"));

    // switch the order of format strings
    configValues.put("formatStrs", List.of("yyyy-MM-dd", "yyyy-MM-dd z"));

    // notice that the time zone is set to the configured default (Europe/Rome) for a date that
    // contains a timezone (EST) because a more general format string occurs first in the  list of format strings
    factory.get(configValues).processDocument(doc);
    assertEquals("2021-02-01T23:00:00Z", doc.getString("output1"));
    assertEquals("2021-02-01T23:00:00Z", doc.getString("output2"));
  }

  private Document parse(Map<String, Object> config, String... values) throws StageException {
    Map<String, Object> full = new HashMap<>(config);
    full.put("source", List.of("date"));
    full.put("dest", List.of("output"));
    full.put("timeZoneId", "UTC");
    Document doc = Document.create("doc");
    for (String value : values) {
      doc.setOrAdd("date", value);
    }
    factory.get(full).processDocument(doc);
    return doc;
  }

  @Test
  public void testTwelveHourFormat() throws StageException {
    Document doc = parse(Map.of("formatStrs", List.of("MM/dd/yyyy hh:mm a"), "locale", "en-US"),
        "03/04/2021 03:15 PM", "03/04/2021 12:30 AM");
    assertEquals(List.of("2021-03-04T15:15:00Z", "2021-03-04T00:30:00Z"), doc.getStringList("output"));

    // without 'a' the hour can't be resolved, so the value is skipped rather than parsed as midnight
    Document noAmPm = parse(Map.of("formatStrs", List.of("MM/dd/yyyy hh:mm")), "03/04/2021 03:15");
    assertFalse(noAmPm.has("output"));
  }

  @Test
  public void testPartialDates() throws StageException {
    Document doc = parse(Map.of("formatStrs", List.of("MMMM yyyy", "yyyy"), "locale", "en-US"), "March 1999", "1850");
    assertEquals(List.of("1999-03-01T00:00:00Z", "1850-01-01T00:00:00Z"), doc.getStringList("output"));

    // a week number isn't dropped in favour of January 1st
    Document week = parse(Map.of("formatStrs", List.of("yyyy-ww"), "locale", "en-US"), "2024-20");
    assertFalse(week.has("output"));
  }

  @Test
  public void testLocale() throws StageException {
    Document doc = parse(Map.of("formatStrs", List.of("MMMM yyyy"), "locale", "fr"), "mars 2020");
    assertEquals("2020-03-01T00:00:00Z", doc.getString("output"));

    // a malformed tag would otherwise fall back to the root locale and silently stop month names parsing
    for (String tag : List.of("en_US", "english")) {
      assertThrows(StageException.class, () -> parse(Map.of("formatStrs", List.of("MMMM yyyy"), "locale", tag), "March 2020"));
    }
  }

  @Test
  public void testFormattersSkipNearMissValues() throws StageException {
    Stage stage = factory.get("ParseDateTest/config.conf");
    Document doc = Document.create("doc");
    for (String value : List.of("2-3", "12/5", "99-2000", "Version 2, 2019", "abc")) {
      doc.setOrAdd("date1", value);
    }
    stage.processDocument(doc);
    assertFalse(doc.has("output1"));

    ZoneId utc = ZoneId.of("UTC");
    assertNull(new DateTwoYearsFormatter().apply("99-2000", utc));
    assertNull(new DatePipeFormatter().apply("2003|13|40", utc));
    assertNull(new DateMonthStrFormatter().apply("Version 2, 2019", utc));
    assertNull(new UnixTimestampFormatter().apply("abc", utc));
    assertEquals(ZonedDateTime.of(2001, 3, 3, 0, 0, 0, 0, utc), new DateMonthStrFormatter().apply("Version 2, March 3, 2001", utc));

    // a prefix before the year is fine; only more digits run together with it make a near miss
    assertEquals(ZonedDateTime.of(2019, 1, 1, 0, 0, 0, 0, utc), new DateTwoYearsFormatter().apply("FY2019-20", utc));
    assertNull(new DateTwoYearsFormatter().apply("12345-678", utc));
    assertEquals(ZonedDateTime.of(2003, 1, 2, 0, 0, 0, 0, utc), new DatePipeFormatter().apply("id_2003|01|02", utc));
    assertNull(new DatePipeFormatter().apply("12003|01|02", utc));
  }

  @Test
  public void testNullValues() throws Exception {
    Stage stage = factory.get(Map.of("source", List.of("f"), "dest", List.of("o"), "formatStrs", List.of("yyyy")));

    Document doc = Document.createFromJson("{\"id\":\"1\",\"f\":null}");
    stage.processDocument(doc);
    assertFalse(doc.has("o"));

    Document mixed = Document.createFromJson("{\"id\":\"1\",\"f\":[null,\"2024\"]}");
    stage.processDocument(mixed);
    assertEquals(1, mixed.getStringList("o").size());
  }
}
