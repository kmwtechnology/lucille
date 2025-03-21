package com.kmwllc.lucille.stage.dateformatters;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  Attempts to find dates which only contain a year and extract
 *  the values into a Java date. Since no month or date are specified, they will default to January 1 of the given year.
 *  Will return null of the there are no dates formatted in it's style within the String.
 */
public class DateYearOnlyFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  private static final Pattern datePattern = Pattern.compile("^\\d{2,4}$");

  /** Creates a DateYearOnlyFormatter. */
  public DateYearOnlyFormatter() {}

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    Matcher matcher = datePattern.matcher(value);

    if (matcher.find()) {
      String dateStr = matcher.group();
      return ZonedDateTime.of(Integer.parseInt(dateStr), 1, 1, 0, 0, 0, 0, zone);
    } else {
      return null;
    }

  }
}
