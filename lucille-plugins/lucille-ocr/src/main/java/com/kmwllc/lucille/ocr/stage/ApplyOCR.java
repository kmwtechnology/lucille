package com.kmwllc.lucille.ocr.stage;

import java.util.Iterator;
import java.util.List;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

public class ApplyOCR extends Stage {

  private final List<FormTemplate> forms;

  public ApplyOCR(Config config) {
    super(config);
    forms = (List<FormTemplate>) config.getAnyRefList("forms");
    
  }

  @Override
  public Iterator<Document> processDocument(Document arg0) throws StageException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'processDocument'");
  }

}
