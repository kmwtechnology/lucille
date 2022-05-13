package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Parses dates into ISO_INSTANT format to be ingested by Solr. If a given date cannot be parsed, it
 * will not be passed through to the destination field.
 * Config Parameters:
 * <p>
 * - source (List<String>) : List of source field names.
 * - dest (List<String>) : List of destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * - formatters (List<Function>) : List of formatter classes to be used for parsing dates. Formatters must implement
 * the Function<String, LocalDate> Interface.
 * - format_strs (List<String>, Optional) : A List of format Strings to try and apply to the dates. Defaults to an empty list.
 * - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 * Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class ParseDate extends Stage {

  private final List<Function<String, LocalDate>> formatters;
  private final List<DateFormat> formats;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final UpdateMode updateMode;

  public ParseDate(Config config) {
    super(config);

    this.formatters = new ArrayList<>();
    this.formats = ConfigUtils.getOrDefault(config, "format_strs", new ArrayList<String>())
      .stream()
      .map(formatString -> new SimpleDateFormat(formatString))
      .collect(Collectors.toUnmodifiableList());

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.updateMode = UpdateMode.fromConfig(config);
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
          Function<String, LocalDate> formatter = (Function<String, LocalDate>) constructor.newInstance();
          formatters.add(formatter);
        } catch (Exception e) {
          throw new StageException("Unable to instantiate date formatters.", e);
        }
      }
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      LocalDate date = null;
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      // For each String value in this field...
      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        for (DateFormat format : formats) {
          format.setLenient(false);
          Date candidate = format.parse(value, new ParsePosition(0));

          if (candidate != null) {
            date = candidate.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
            break;
          }
        }

        // Apply all of the date formatters
        if (date == null) {
          for (Function<String, LocalDate> formatter : formatters) {
            date = formatter.apply(value);

            if (date != null) {
              break;
            }
          }

          if (date == null) {
            continue;
          }
        }

        // Convert the returned LocalDate into a String in the ISO_INSTANT format for Solr
        // TODO : Potentially add Date object to Document
        String dateStr = DateTimeFormatter.ISO_INSTANT.format(date.atStartOfDay().toInstant(ZoneOffset.UTC));
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
