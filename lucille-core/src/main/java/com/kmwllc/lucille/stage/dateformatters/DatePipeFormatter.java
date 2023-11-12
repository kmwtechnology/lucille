package com.kmwllc.lucille.stage.dateformatters;

import java.time.LocalDateTime;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Formatter for parsing infobox data into Dates. Attempts to find dates of the form "YYYY|MM|DD" and extract the values
 * into a Java date. Will return null of the there are no dates formatted in it's style within the String.
 */
public class DatePipeFormatter implements Function<String, LocalDateTime> {

  private static final Pattern datePattern = Pattern.compile("\\d{1,4}\\|\\d{1,2}\\|\\d{1,2}");

  @Override
  public LocalDateTime apply(String value) {
    Matcher matcher = datePattern.matcher(value);

    if (!matcher.find()) {
      return null;
    } else {
      String dateStr = matcher.group();
      String[] dateParts = dateStr.split("\\|");

      return LocalDateTime.of(Integer.parseInt(dateParts[0].substring(0, 4)), Integer.parseInt(dateParts[1]),
          Integer.parseInt(dateParts[2]),0,0,0);
    }
  }
}
