package com.kmwllc.lucille.stage.dateformatters;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;

/**
 * Formatter for parsing a unix timestamp provided as the number of milliseconds since the epoch.
 * Will return null if the value is not an integer.
 */
public class UnixTimestampFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
    long ms;
    try {
      ms = Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
    return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC);
  }

}
