package com.kmwllc.lucille.stage.dateformatters;

import java.time.DateTimeException;
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

  private static final Pattern datePattern = Pattern.compile("(?<!\\d)(\\d{4})\\|(\\d{1,2})\\|(\\d{1,2})(?!\\d)");

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    Matcher matcher = datePattern.matcher(value);

    while (matcher.find()) {
      try {
        return ZonedDateTime.of(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)),
            Integer.parseInt(matcher.group(3)), 0, 0, 0, 0, zone);
      } catch (DateTimeException e) {
        // out-of-range month or day, try the next match
      }
    }
    return null;
  }
}
