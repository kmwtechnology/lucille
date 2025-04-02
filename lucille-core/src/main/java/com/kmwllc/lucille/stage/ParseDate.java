package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import java.lang.reflect.Constructor;
import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Parses dates into ISO_INSTANT format to be ingested by Solr. If a given date cannot be parsed, it
 * will not be passed through to the destination field.
 * Config Parameters:
 * <p>
 * - source (List&lt;String&gt;) : List of source field names.
 * - dest (List&lt;String&gt;) : List of destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * - formatters (List&lt;Function&gt;) : List of formatter classes to be used for parsing dates. Formatters must implement
 * the Function&lt;String, LocalDate&gt; Interface.
 * - format_strs (List&lt;String&gt;, Optional) : A List of format Strings to try and apply to the dates. Defaults to an empty list.
 * - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 * Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 * - time_zone_id (String, Optional) : The time zone ID to use when parsing dates. Defaults to the system default. <a href="https://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html">See here for more info about time zone ids</a>
 */
public class ParseDate extends Stage {

  private final List<BiFunction<String, ZoneId, ZonedDateTime>> formatters;
  private final List<DateTimeFormatter> formats;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final UpdateMode updateMode;

  // ZoneId to use when parsing dates that do not include zone information
  private final ZoneId zoneId;

  public ParseDate(Config config) {
    super(config, Spec.stage().withRequiredProperties("source", "dest")
        .withOptionalProperties("format_strs", "update_mode", "formatters", "time_zone_id"));

    this.formatters = new ArrayList<>();

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.updateMode = UpdateMode.fromConfig(config);

    zoneId = getTimeZoneId(config);
    formats = getFormats(config, zoneId);
  }

  private static ZoneId getTimeZoneId(Config config) {
    if (!config.hasPath("time_zone_id")) {
      return ZoneId.systemDefault();
    }
    String timeZoneId = config.getString("time_zone_id");
    return ZoneId.of(timeZoneId);
  }

  private static List<DateTimeFormatter> getFormats(Config config, ZoneId zoneId) {

    // convert format strings to a set to remove duplicates
    List<String> formatStrings = !config.hasPath("format_strs") ? new ArrayList<>() :
        config.getStringList("format_strs").stream().distinct().collect(Collectors.toList());

    // create date formatters from format strings with a timezone
    List<DateTimeFormatter> formats = new ArrayList<>();

    for (String formatString : formatStrings) {

      // configure the formatter to use the designated zone id and to set the time to the beginning of the day
      // if no time information is included in the format; time must be populated so that we can call
      // ZonedDateTime.from() on the TemporalAccessor returned by the formatter
      DateTimeFormatter format =
          new DateTimeFormatterBuilder().appendPattern(formatString)
              .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
              .toFormatter()
              .withZone(zoneId);

      formats.add(format);
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

      if (!doc.has(sourceField)) {
        continue;
      }

      // For each String value in this field...
      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {

        ZonedDateTime date = null;

        for (DateTimeFormatter format : formats) {
          TemporalAccessor candidate = null;

          try {
            candidate = format.parse(value, new ParsePosition(0));
          } catch (DateTimeParseException e) {
          }

          if (candidate != null) {
            date = ZonedDateTime.from(candidate);
            break;
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
}
