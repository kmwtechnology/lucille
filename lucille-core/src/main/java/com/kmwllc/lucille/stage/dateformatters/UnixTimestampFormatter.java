package com.kmwllc.lucille.stage.dateformatters;

import java.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Formatter for parsing a unix timestamp provided as the number of milliseconds since the epoch.
 */
public class UnixTimestampFormatter implements Function<String, LocalDateTime> {
  
  @Override
  public LocalDateTime apply(String value) {
	  // TODO: what about malformed values?
	  Long ms = Long.valueOf(value);
	  Instant instant = Instant.ofEpochMilli(ms) ;
	  LocalDateTime ld = instant.atOffset(ZoneOffset.UTC).toLocalDateTime();
	  return ld;
  }

}
