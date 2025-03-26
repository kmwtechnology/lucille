package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import org.junit.Test;

public class PromptOllamaTest {

  private final StageFactory factory = StageFactory.of(PromptOllama.class);

  @Test
  public void sandbox() throws Exception {
    Stage stage = factory.get("PromptOllamaTest/example.conf");

    Document doc = Document.create("doc1");
    doc.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
    doc.setField("sender", "j@abcdef.com");

    stage.processDocument(doc);
  }
}
