package com.kmwllc.lucille.stage.dateformatters;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Attempts to find dates of the form "January 1, 2000" and extract the values into a Java date.
 * Will return null if there are no dates formatted in its style within the String.
 */
public class DateMonthStrFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  private static final Pattern datePattern = Pattern.compile("\\b\\p{L}+ \\d{1,2}, \\d{4}\\b");
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("LLLL d, u");

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    Matcher matcher = datePattern.matcher(value);

    while (matcher.find()) {
      try {
        return LocalDate.parse(matcher.group(), formatter).atStartOfDay(zone);
      } catch (DateTimeParseException e) {
        // not a month name ("Version 2, 2019") or an invalid day, try the next match
      }
    }
    return null;
  }
}
