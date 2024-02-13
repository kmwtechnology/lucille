package com.kmwllc.lucille.ocr.stage;

import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.StageFactory;
import com.kmwllc.lucille.util.StageUtils;

public class ApplyOCRTest {

  StageFactory factory = StageFactory.of(ApplyOCR.class);


  @Test 
  public void testBasic() throws StageException {
    Stage basic = factory.get("ApplyOCRTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/ApplyOCRTest/data/test.pdf");
    doc.setField("page", 0);

    basic.processDocument(doc);
    System.out.println(doc.getString("first_label"));
  }

}
