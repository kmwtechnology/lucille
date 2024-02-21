package com.kmwllc.lucille.stage.dateformatters;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;

/**
 * Formatter for parsing a unix timestamp provided as the number of milliseconds since the epoch.
 */
public class UnixTimestampFormatter implements BiFunction<String, ZoneId, ZonedDateTime> {

  @Override
  public ZonedDateTime apply(String value, ZoneId zone) {
	  Long ms = Long.valueOf(value);
	  Instant instant = Instant.ofEpochMilli(ms) ;
	  return instant.atZone(ZoneOffset.UTC);
  }

}
