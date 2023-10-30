package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ParseFloats extends Stage {

  private final String field;

  public ParseFloats(Config config) {
    super(config, new StageSpec().withRequiredProperties("field"));
    this.field = config.getString("field");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (doc.has(field)) {
      String value = doc.getString(this.field);
      List<Float> floats = Arrays.stream(value.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(",", "").split(" "))
          .map(v -> Float.parseFloat(v)).collect(Collectors.toList());
      doc.removeField(this.field);
      for (Float d : floats) {
        doc.addToField(this.field, d);
      }
    }
    return null;
  }
}
