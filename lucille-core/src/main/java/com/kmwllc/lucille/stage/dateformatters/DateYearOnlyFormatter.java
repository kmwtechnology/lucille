package com.kmwllc.lucille.stage.dateformatters;

import java.time.LocalDateTime;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  * Formatter for parsing infobox data into Dates. Attempts to find dates which only contain a year and extract
 *  the values into a Java date. Since no month or date are specified, they will default to January 1 of the given year.
 *  Will return null of the there are no dates formatted in it's style within the String.
 */
public class DateYearOnlyFormatter implements Function<String, LocalDateTime> {

  private static final Pattern datePattern = Pattern.compile("^\\d{2,4}$");

  @Override
  public LocalDateTime apply(String value) {
    Matcher matcher = datePattern.matcher(value);

    if (matcher.find()) {
      String dateStr = matcher.group();
      return LocalDateTime.of(Integer.parseInt(dateStr), 1, 1, 0, 0, 0);
    } else {
      return null;
    }

  }
}
