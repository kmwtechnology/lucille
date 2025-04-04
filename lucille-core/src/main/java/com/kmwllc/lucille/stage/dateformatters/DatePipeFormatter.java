package com.kmwllc.lucille.stage.dateformatters;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Attempts to find dates of the form "YYYY|MM|DD" and extract the values into a Java date.
 * Will return null if there are no dates formatted in its style within the String.
 */
public class DatePipeFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  private static final Pattern datePattern = Pattern.compile("\\d{1,4}\\|\\d{1,2}\\|\\d{1,2}");

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    Matcher matcher = datePattern.matcher(value);

    if (!matcher.find()) {
      return null;
    } else {
      String dateStr = matcher.group();
      String[] dateParts = dateStr.split("\\|");

      return ZonedDateTime.of(Integer.parseInt(dateParts[0].substring(0, 4)), Integer.parseInt(dateParts[1]),
          Integer.parseInt(dateParts[2]), 0, 0, 0, 0, zone);
    }
  }
}
