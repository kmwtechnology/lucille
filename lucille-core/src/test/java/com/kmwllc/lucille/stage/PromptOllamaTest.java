package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class PromptOllamaTest {

  private final StageFactory factory = StageFactory.of(PromptOllama.class);

  @Test
  public void testOllama() throws Exception {
    Stage stage = factory.get("PromptOllamaTest/example.conf");

    Document doc = Document.create("doc1");
    doc.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
    doc.setField("sender", "j@abcdef.com");

    Document doc2 = Document.create("doc2");
    doc2.setField("message", "We want to make sure we are reporting higher earnings for the next quarter. You have as much leeway as you need. CEO is very focused on beating projections to bolster confidence in the markets.");
    doc2.setField("sender", "e1Jamie@enron.com");

    stage.processDocument(doc);
    stage.processDocument(doc2);

    assertTrue(doc.has("fraud"));
    assertTrue(doc.has("summary"));

    assertTrue(doc2.has("fraud"));
    assertTrue(doc2.has("summary"));
  }

  @Test
  public void testFields() throws Exception {
    Stage stage = factory.get("PromptOllamaTest/fields.conf");

    // TODO: In the mocked test, make sure the request doesn't contain "eps" - it is not in "fields".
    Document doc = Document.create("doc1");
    doc.setField("stock_name", "Apple Inc.");
    doc.setField("company_summary", "Apple Inc. is a global technology company known for its iPhones, Mac computers, iPads, and software ecosystem. It emphasizes innovation, design, privacy, and premium user experiences.");
    doc.setField("pe_multiple", 32.1);
    doc.setField("eps", "6.42");

    Document doc2 = Document.create("doc2");
    doc2.setField("stock_name", "Aerotyne International");
    doc2.setField("company_summary", "Aerotyne International is a cutting-edge firm poised for explosive growth in aerospace technology. Ground-floor opportunity");
    doc2.setField("pe_multiple", "N/A");
    doc2.setField("eps", "-300.21");

    stage.processDocument(doc);
    stage.processDocument(doc2);

    assertTrue(doc.has("rating"));
    assertTrue(doc.has("opinion"));

    assertTrue(doc2.has("rating"));
    assertTrue(doc2.has("opinion"));
  }

  // The LLM isn't outputting a JSON response. requireJSON is set to false by default.
  @Test
  public void testNoJsonPrompt() throws Exception {
    Stage stage = factory.get("PromptOllamaTest/noJsonPrompt.conf");

    Document doc = Document.create("doc1");
    doc.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
    doc.setField("sender", "j@abcdef.com");

    Document doc2 = Document.create("doc2");
    doc2.setField("message", "We want to make sure we are reporting higher earnings for the next quarter. You have as much leeway as you need. CEO is very focused on beating projections to bolster confidence in the markets.");
    doc2.setField("sender", "e1Jamie@enron.com");

    stage.processDocument(doc);
    stage.processDocument(doc2);

    assertTrue(doc.has("ollamaResponse"));
    assertTrue(doc2.has("ollamaResponse"));
  }

  @Test
  public void testRequireJson() throws Exception {
    Stage stage = factory.get("PromptOllamaTest/example.conf");

    Document doc = Document.create("doc1");
    doc.setField("message", "Let's try to keep this hidden, wouldn't want the boss finding out.");
    doc.setField("sender", "j@abcdef.com");

    stage.processDocument(doc);

    assertTrue(doc.has("fraud"));
    assertTrue(doc.has("summary"));

    Stage noJsonStage = factory.get("PromptOllamaTest/noJsonPromptRequireJson.conf");
    assertThrows(StageException.class, () -> noJsonStage.processDocument(doc));
  }
}
