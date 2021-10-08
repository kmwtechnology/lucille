package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

/**
 * This Stage removes stop words supplied in the dictionaries from a given list of fields.
 *
 * Config Parameters -
 *
 *   - dictionaries (List<String>) : A list of dictionaries to pull stop words from
 *   - fields (List<String>, Optional) : A list of fields to remove stop words from. If no fields are supplied, this
 *     stage will be applied to every non-reserved field on every document.
 */
public class ApplyStopWords extends Stage {

  private final List<String> dictionaries;
  private final List<String> stopWords;
  private final List<String> fieldNames;

  public ApplyStopWords(Config config) {
    super(config);
    this.dictionaries = config.getStringList("dictionaries");
    this.fieldNames = config.hasPath("fields") ? config.getStringList("fields") : null;
    this.stopWords = new ArrayList<>();
  }

  @Override
  public void start() throws StageException {
    if (dictionaries.isEmpty())
      throw new StageException("Must provide at least one dictionary containing stop words to the ApplyStopWords Stage.");

    for (String dictionary : dictionaries) {
      try (CSVReader reader = new CSVReader(FileUtils.getReader(dictionary))) {
        String[] line;
        while ((line = reader.readNext()) != null) {
          if (line.length > 1)
            throw new StageException("Stop word dictionaries should only have entry per line.");

          stopWords.add(line[0]);
        }
      } catch (Exception e) {
        throw new StageException("Unable to read in the supplied dictionary files.", e);
      }
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    List<String> fields = fieldNames != null ? fieldNames : new ArrayList<>(doc.asMap().keySet());

    for (String field : fields) {
      if (!doc.has(field))
        continue;

      try {
        doc.validateNotReservedField(field);
      } catch (IllegalArgumentException e) {
        continue;
      }

      // TODO : Decide how we want to handle punctuation (currently it is left in, see tests for examples)
      List<String> newValues = new ArrayList<>();
      for (String val : doc.getStringList(field)) {
        for (String stopWord : stopWords) {
          val = val.replaceAll("(?i) *\\b" + stopWord + "\\b *", " ");
        }

        newValues.add(val.trim());
      }

      doc.removeField(field);
      doc.update(field, UpdateMode.DEFAULT, newValues.toArray(new String[0]));
    }

    return null;
  }
}
