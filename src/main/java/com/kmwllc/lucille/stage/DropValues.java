package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.List;

public class DropValues extends Stage {

  private final List<String> sourceFields;
  private final List<String> values;

  public DropValues(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.values = config.getStringList("values");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Drop Values");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (String source : sourceFields) {
      if (!doc.has(source))
        continue;

      // TODO : Move dropping array values to Document
      List<String> fieldVals = doc.getStringList(source);
      for (int i = 0; i < fieldVals.size(); i++) {
        if (values.contains(fieldVals.get(i))) {
          doc.removeFromArray(source, i);
        }
      }
    }

    return null;
  }
}
