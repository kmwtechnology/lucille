package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;

public class NopStage extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().build();

  public NopStage(Config config) {
    super(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    return null;
  }
}
