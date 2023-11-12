package com.kmwllc.lucille.stage.dateformatters;

import java.time.LocalDateTime;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  * Formatter for parsing infobox data into Dates. Attempts to find dates which only contain two years only and extract
 *  the values into a Java date. The first year specified will be the year we save, the other year will be discarded.
 *  Since no month or date are specified, they will default to January 1 of the given year.
 *  Will return null of the there are no dates formatted in it's style within the String.
 */
public class DateTwoYearsFormatter implements Function<String, LocalDateTime> {

  private static final Pattern datePattern = Pattern.compile("\\d{1,4}\\W\\d{1,4}");

  @Override
  public LocalDateTime apply(String value) {
    Matcher matcher = datePattern.matcher(value);

    if (!matcher.find()) {
      return null;
    } else {
      String dateStr = matcher.group();
      String firstYear = dateStr.substring(0, 4);

      return LocalDateTime.of(Integer.parseInt(firstYear), 1, 1,0,0,0);
    }

  }
}
