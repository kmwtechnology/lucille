package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

import java.util.Iterator;
import java.util.Map.Entry;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Map;

/**
 * Determines the length of a field and places the value into a specified field.
 * <br>
 * Config Parameters -
 * <br>
 * fieldMapping (Map&lt;String, String&gt;) : A mapping of the field to check the size of to the name of the field to place the length into.
 */
public class Length extends Stage {

  private final Map<String, Object> fieldMap;

  public Length(Config config) {
    super(config, new StageSpec().withRequiredParents("fieldMapping"));
    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, Object> e : fieldMap.entrySet()) {
      doc.setField((String) e.getValue(), doc.length(e.getKey()));
    }

    return null;
  }
}
