package com.kmwllc.lucille.stage.dateformatters;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  Attempts to find dates which only contain two years only and extract the values into a Java date.
 *  The first year specified will be the year we save, the other year will be discarded.
 *  Since no month or date are specified, they will default to January 1 of the given year.
 *  Will return null if there are no dates formatted in this style within the String.
 */
public class DateTwoYearsFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  private static final Pattern datePattern = Pattern.compile("\\d{1,4}\\W\\d{1,4}");

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    Matcher matcher = datePattern.matcher(value);

    if (!matcher.find()) {
      return null;
    } else {
      String dateStr = matcher.group();
      String firstYear = dateStr.substring(0, 4);

      return ZonedDateTime.of(Integer.parseInt(firstYear), 1, 1, 0, 0, 0, 0, zone);
    }
  }
}
