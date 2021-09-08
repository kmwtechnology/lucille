package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.List;

/**
 * This Stage will create teasers of a given length from the given source fields. If the character limit is reached in
 * the middle of a word, the teaser will not be truncated until the end of the word. NOTE : If a given field is
 * multivalued, this Stage will only operate on the first value.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : list of source field names.
 *   - dest (List<String>) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - max_length (Integer) : The maximum number of characters to include in the extracted teaser.
 */
public class CreateStaticTeaser extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final int maxLength;

  public CreateStaticTeaser(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.maxLength = config.getInt("max_length");
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
        doc.addToField(dest, fullText.trim());
        continue;
      }

      int pointer = maxLength - 1;
      String delims = " .?!,";
      // While we are in the middle of a word, move the pointer forward
      while (pointer > 0 && !delims.contains("" + fullText.charAt(pointer))) {
        pointer--;
      }

      if (pointer == 0) {
        doc.addToField(dest, fullText.substring(0, maxLength));
      }

      doc.addToField(dest, fullText.substring(0, pointer).trim());
    }

    return null;
  }
}
