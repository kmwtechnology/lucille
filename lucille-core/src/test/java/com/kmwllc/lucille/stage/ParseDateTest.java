package com.kmwllc.lucille.stage;

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
            "time_zone_id",
            "conditionPolicy"),
        stage.getLegalProperties());
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
    HashMap<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date"));
    configValues.put("dest", List.of("output"));
    configValues.put("format_strs", List.of("yyyy-MM-dd"));

    // create a map of time zones to expected formatted dates
    List<String> zones = List.of("America/New_York", "Universal", "Europe/Rome", "Australia/Brisbane");

    for (String zone : zones) {

      // add a time zone to the config values map
      configValues.put("time_zone_id", zone);

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

      assertEquals("Failed for time_zone_id=" + zone,
          expected, doc.getString("output"));
    }
  }

  @Test
  public void testParseDateWithTime() throws StageException {
    HashMap<String,Object> configValues = new HashMap<>();
    configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
    configValues.put("source", List.of("date"));
    configValues.put("dest", List.of("output"));
    configValues.put("format_strs", List.of("yyyy-MM-dd'T'HH:mm:ssxxxxx[VV]"));
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

      // create a ParseDate stage with the given time_zone_id specified in the config
      HashMap<String, Object> configValues = new HashMap<>();
      configValues.put("class", "com.kmwllc.lucille.stage.ParseDate");
      configValues.put("source", List.of("date"));
      configValues.put("dest", List.of("output"));
      configValues.put("format_strs", List.of("yyyy-MM-dd"));
      configValues.put("time_zone_id", zoneIdStr);
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
    assertEquals("2021-02-01T23:00:00Z", doc.getString("output1"));
    assertEquals("2021-02-02T05:00:00Z", doc.getString("output2"));

    // switch the order of format strings
    configValues.put("format_strs", List.of("yyyy-MM-dd", "yyyy-MM-dd z"));

    // notice that the time zone is set to the configured default (Europe/Rome) for a date that
    // contains a timezone (EST) because a more general format string occurs first in the  list of format strings
    factory.get(configValues).processDocument(doc);
    assertEquals("2021-02-01T23:00:00Z", doc.getString("output1"));
    assertEquals("2021-02-01T23:00:00Z", doc.getString("output2"));
  }
}
