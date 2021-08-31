package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Pipeline;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.dateformatters.DateMonthStrFormatter;
import com.kmwllc.lucille.stage.dateformatters.DatePipeFormatter;
import com.kmwllc.lucille.stage.dateformatters.DateTwoYearsFormatter;
import com.kmwllc.lucille.stage.dateformatters.DateYearOnlyFormatter;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.lang.reflect.Constructor;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ParseDate extends Stage {

  private final List<Function<String, LocalDate>> formatters;
  private final List<String> sourceFields;
  private final List<String> destFields;

  public ParseDate(Config config) {
    super(config);

    this.formatters = new ArrayList<>();
    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Parse Date");
    StageUtils.validateFieldNumNotZero(destFields, "Parse Date");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Parse Date");

    List<? extends Config> formatterClasses = config.getConfigList("formatters");
    for (Config c : formatterClasses) {
      try {
        Class<?> clazz = Class.forName(c.getString("class"));
        Constructor<?> constructor = clazz.getConstructor();
        Function<String, LocalDate> formatter = (Function<String, LocalDate>) constructor.newInstance();
        formatters.add(formatter);
      } catch (Exception e) {
        throw new StageException(e.getMessage());
      }
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    LocalDate date = null;
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      // For each String value in this field...
      for (String value : doc.getStringList(sourceField)) {
        // Apply all of the date formatters
        for (Function<String, LocalDate> formatter : formatters) {
          date = formatter.apply(value);

          if (date != null) {
            break;
          }
        }

        // TODO : Dates which cannot be reformatted are lost
        if (date == null) {
          continue;
        }

        // Convert the returned LocalDate into a String in the ISO_INSTANT format for Solr
        // TODO : Maybe make date output configurable
        String dateStr = DateTimeFormatter.ISO_INSTANT.format(date.atStartOfDay().toInstant(ZoneOffset.UTC));
        doc.addToField(destField, dateStr);
      }
    }
    return null;
  }
}
