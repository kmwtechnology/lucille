package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DecodeBase64Test {

  private StageFactory factory = StageFactory.of(DecodeBase64.class);

  @Test
  public void testBase64Decode() throws Exception {
    Stage stage = factory.get("DecodeBase64Test/config.conf");
    Document doc = Document.create("doc");
    doc.setField("input", "SGVsbG8gd29ybGQu");
    stage.processDocument(doc);
    byte[] result =  doc.getBytes("bytes");
    assertEquals("Hello world.", new String(result)); 
  }

}
