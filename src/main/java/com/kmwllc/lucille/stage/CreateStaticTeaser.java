package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.List;

/**
 * Create teasers of a given length from the given source fields. If the character limit is reached in
 * the middle of a word, the teaser will not be truncated until the end of the word. NOTE : If a given field is
 * multivalued, this Stage will only operate on the first value.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : list of source field names.
 *   - dest (List<String>) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - maxLength (Integer) : The maximum number of characters to include in the extracted teaser.
 *   - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *      Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class CreateStaticTeaser extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final int maxLength;
  private final UpdateMode updateMode;

  public CreateStaticTeaser(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.maxLength = config.getInt("maxLength");
    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Create Static Teaser");

    if (maxLength < 1) {
      throw new StageException("Max length is less than 1 for Static Teaser Stage");
    }
  }

  // NOTE : If a given field is multivalued, this Stage will only operate on the first value
  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      String source = sourceFields.get(i);
      String dest = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(source))
        continue;

      String fullText = doc.getStringList(source).get(0);

      // If this field value is shorter than the max length, put the whole String in the destination field
      if (maxLength > fullText.length()) {
        doc.update(dest, updateMode, fullText.trim());
        continue;
      }

      int pointer = maxLength - 1;
      String delims = " .?!,";
      // While we are in the middle of a word, move the pointer forward
      while (pointer > 0 && !delims.contains("" + fullText.charAt(pointer))) {
        pointer--;
      }

      // If this is a continuous String of word characters, truncate it to the maxLength
      if (pointer == 0) {
        doc.update(dest, updateMode, fullText.substring(0, maxLength));
      } else {
        doc.update(dest, updateMode, fullText.substring(0, pointer).trim());
      }
    }

    return null;
  }

  @Override
  public List<String> getPropertyList() {
    return List.of("source", "dest", "maxLength");
  }
}
