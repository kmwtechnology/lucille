package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Map.Entry;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Map;

public class Length extends Stage {

  private final Map<String, Object> fieldMap;

  public Length(Config config) {
    super(config);
    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, Object> e : fieldMap.entrySet()) {
      doc.setField((String) e.getValue(), doc.length(e.getKey()));
    }

    return null;
  }
}
