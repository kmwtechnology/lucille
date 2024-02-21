package com.kmwllc.lucille.stage.dateformatters;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  Attempts to find dates of the form "January 1, 2000" and extract
 *  the values into a Java date. Will return null of the there are no dates formatted in it's style within the String.
 */
public class DateMonthStrFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  private static final Pattern datePattern = Pattern.compile("\\w+ \\d{1,2}, \\d{1,4}");
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("LLLL d, u");

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    Matcher matcher = datePattern.matcher(value);

    if (!matcher.find()) {
      return null;
    } else {
      String dateStr = matcher.group();
      return LocalDate.parse(dateStr, formatter).atStartOfDay(zone);
    }
  }
}
