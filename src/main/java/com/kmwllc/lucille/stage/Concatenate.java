package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import org.apache.commons.lang.text.StrSubstitutor;

import java.util.HashMap;
import java.util.List;

public class Concatenate extends Stage {

  private final List<String> sourceFields;
  private final String destField;
  private final String formatStr;

  public Concatenate(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destField = config.getString("dest");
    this.formatStr = config.getString("format_string");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Concatenate");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    HashMap<String, String> replacements = new HashMap<>();
    for (String source : sourceFields) {
      if (!doc.has(source))
        continue;

      replacements.put(source, doc.getStringList(source).get(0));
    }

    StrSubstitutor sub = new StrSubstitutor(replacements, "{", "}");
    doc.setField(destField, sub.replace(formatStr));

    return null;
  }
}
