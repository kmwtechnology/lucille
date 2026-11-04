package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import java.lang.reflect.Constructor;
import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Parses dates into ISO_INSTANT format to be ingested by Solr. If a given date cannot be parsed, it
 * will not be passed through to the destination field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (List&lt;String&gt;) : List of source field names.</li>
 *   <li>dest (List&lt;String&gt;) : List of destination field names. You can either supply the same number of source and destination fields
 *   for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.</li>
 *   <li>formatters (List&lt;Function&gt;) : List of formatter classes to be used for parsing dates. Formatters must implement the
 *   Function&lt;String, LocalDate&gt; Interface.</li>
 *   <li>formatStrs (List&lt;String&gt;, Optional) : A List of format Strings to try and apply to the dates. Defaults to an empty list.</li>
 *   <li>updateMode (String, Optional) : Determines how writing will be handling if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
 *   <li>timeZoneId (String, Optional) : The time zone ID to use when parsing dates. Defaults to the system default.
 *   <a href="https://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html">See here for more info about time zone ids</a></li>
 *   <li>locale (String, Optional) : IETF BCP 47 language tag (e.g. "en-US", "fr") used for month and day names in formatStrs.
 *   An invalid tag fails the stage at startup. Defaults to the JVM default locale.</li>
 * </ul>
 * <p>
 * A format string without a time parses to midnight; one with only a year, or a year and month, parses to the first day of
 * that year or month.
 */
public class ParseDate extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredList("source", new TypeReference<List<String>>(){})
      .requiredList("dest", new TypeReference<List<String>>(){})
      .optionalList("formatStrs", new TypeReference<List<String>>(){})
      .optionalList("formatters", SpecBuilder.withoutDefaults().requiredString("class").build())
      .optionalString("timeZoneId", "updateMode", "locale").build();

  private final List<BiFunction<String, ZoneId, ZonedDateTime>> formatters;
  private final List<DateTimeFormatter> formats;
  // the same patterns with month and day defaulting to 1, tried when a pattern gives no full date (yyyy, MMMM yyyy)
  private final List<DateTimeFormatter> partialFormats;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final UpdateMode updateMode;

  // ZoneId to use when parsing dates that do not include zone information
  private final ZoneId zoneId;

  public ParseDate(Config config) throws StageException {
    super(config);

    this.formatters = new ArrayList<>();

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.updateMode = UpdateMode.fromConfig(config);

    zoneId = getTimeZoneId(config);
    Locale locale = config.hasPath("locale") ? getLocale(config.getString("locale")) : Locale.getDefault();
    formats = getFormats(config, zoneId, locale, false);
    partialFormats = getFormats(config, zoneId, locale, true);
  }

  // forLanguageTag doesn't fail on a bad tag ("en_US" gives the root locale), which would silently stop month names parsing
  private static Locale getLocale(String tag) throws StageException {
    Locale locale = Locale.forLanguageTag(tag);
    boolean known = Arrays.stream(Locale.getAvailableLocales()).anyMatch(l -> l.getLanguage().equals(locale.getLanguage()));
    if (locale.getLanguage().isEmpty() || !known) {
      throw new StageException("Invalid locale: " + tag + ". Use a BCP 47 language tag such as \"en-US\".");
    }
    return locale;
  }

  private static ZoneId getTimeZoneId(Config config) {
    if (!config.hasPath("timeZoneId")) {
      return ZoneId.systemDefault();
    }
    String timeZoneId = config.getString("timeZoneId");
    return ZoneId.of(timeZoneId);
  }

  private static List<DateTimeFormatter> getFormats(Config config, ZoneId zoneId, Locale locale, boolean partial) {

    // convert format strings to a set to remove duplicates
    List<String> formatStrings = !config.hasPath("formatStrs") ? new ArrayList<>() :
        config.getStringList("formatStrs").stream().distinct().collect(Collectors.toList());

    // create date formatters from format strings with a timezone
    List<DateTimeFormatter> formats = new ArrayList<>();

    for (String formatString : formatStrings) {

      // a missing time is filled in by toZonedDateTime rather than parseDefaulting, because a defaulted HOUR_OF_DAY
      // conflicts with the hour resolved from 12-hour patterns (hh:mm a)
      DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().appendPattern(formatString);
      if (partial) {
        // fields left over that don't match the defaults (a week, a quarter) fail the resolver's cross-check
        builder.parseDefaulting(ChronoField.MONTH_OF_YEAR, 1).parseDefaulting(ChronoField.DAY_OF_MONTH, 1);
      }
      formats.add(builder.toFormatter(locale).withZone(zoneId));
    }
    return formats;
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Parse Date");
    StageUtils.validateFieldNumNotZero(destFields, "Parse Date");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Parse Date");

    if (config.hasPath("formatters")) {
      // Instantiate all of the formatters supplied in the Config
      List<? extends Config> formatterClasses = config.getConfigList("formatters");
      for (Config c : formatterClasses) {
        try {
          Class<?> clazz = Class.forName(c.getString("class"));
          Constructor<?> constructor = clazz.getConstructor();
          BiFunction<String, ZoneId, ZonedDateTime> formatter = (BiFunction<String, ZoneId, ZonedDateTime>) constructor.newInstance();
          formatters.add(formatter);
        } catch (Exception e) {
          throw new StageException("Unable to instantiate date formatters.", e);
        }
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      List<String> values = StageUtils.getNonNullStrings(doc, sourceField);
      if (values.isEmpty()) {
        continue;
      }

      // For each String value in this field...
      List<String> outputValues = new ArrayList<>();
      for (String value : values) {

        ZonedDateTime date = null;

        for (int f = 0; f < formats.size() && date == null; f++) {
          date = parse(formats.get(f), value);
          if (date == null) {
            date = parse(partialFormats.get(f), value);
          }
        }

        // Apply all of the date formatters
        if (date == null) {
          for (BiFunction<String, ZoneId, ZonedDateTime> formatter : formatters) {
            date = formatter.apply(value, zoneId);

            if (date != null) {
              break;
            }
          }

          if (date == null) {
            continue;
          }
        }

        // Convert the returned ZonedDateTime into a String in the ISO_INSTANT format for Solr
        String dateStr = DateTimeFormatter.ISO_INSTANT.format(date);
        outputValues.add(dateStr);
      }

      if (outputValues.isEmpty() && destField.equals(sourceField)) {
        doc.removeField(sourceField);
      }

      doc.update(destField, updateMode, outputValues.toArray(new String[0]));
    }
    return null;
  }

  // a pattern only has to match the start of the value
  private static ZonedDateTime parse(DateTimeFormatter format, String value) {
    try {
      return toZonedDateTime(format.parse(value, new ParsePosition(0)));
    } catch (DateTimeException e) {
      return null;
    }
  }

  private static ZonedDateTime toZonedDateTime(TemporalAccessor parsed) {
    LocalDate date = parsed.query(TemporalQueries.localDate());
    if (date == null) {
      return null;
    }
    if (parsed.query(TemporalQueries.localTime()) != null) {
      return ZonedDateTime.from(parsed);
    }
    // an unresolved hour (hh without a) would otherwise become midnight
    if (Arrays.stream(ChronoField.values()).anyMatch(f -> f.isTimeBased() && parsed.isSupported(f))) {
      return null;
    }
    ZoneOffset offset = parsed.query(TemporalQueries.offset());
    return offset != null ? date.atStartOfDay().atOffset(offset).toZonedDateTime() : date.atStartOfDay(parsed.query(TemporalQueries.zone()));
  }
}
