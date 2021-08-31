package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.List;

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

      if (maxLength > fullText.length()) {
        doc.addToField(dest, fullText);
        continue;
      }

      int pointer = maxLength - 1;
      String delims = " .?!";
      while (pointer < fullText.length() && !delims.contains("" + fullText.charAt(pointer))) {
        pointer++;
      }

      doc.addToField(dest, fullText.substring(0, ++pointer).trim());
    }

    return null;
  }
}
