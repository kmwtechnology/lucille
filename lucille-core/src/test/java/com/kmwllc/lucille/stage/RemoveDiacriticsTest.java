package com.kmwllc.lucille.stage;

import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class RemoveDiacriticsTest {

  private StageFactory factory = StageFactory.of(RemoveDiacritics.class);

  @Test
  public void testAccentsAndDiacratics() throws StageException {
    Stage stage = factory.get("RemoveDiacriticsTest/config.conf");
    Document doc = Document.create("id");
    doc.setField("foo", "āăąēîïĩíĝġńñšŝśûůŷ");

    stage.processDocument(doc);

    assertEquals(doc.getString("foo"), "āăąēîïĩíĝġńñšŝśûůŷ");
    assertEquals(doc.getString("bar"), "aaaeiiiiggnnsssuuy");
  }

  @Test
  public void testNullValues() throws Exception {
    Stage stage = factory.get(Map.of("source", "f", "dest", "o"));

    Document doc = Document.createFromJson("{\"id\":\"1\",\"f\":null}");
    stage.processDocument(doc);
    assertFalse(doc.has("o"));

    Document mixed = Document.createFromJson("{\"id\":\"1\",\"f\":[null,\"Hi There\"]}");
    stage.processDocument(mixed);
    assertEquals("Hi There", mixed.getString("o"));
  }

  @Test
  public void testAllValues() throws Exception {
    Stage stage = factory.get(Map.of("source", "a"));
    Document doc = Document.createFromJson("{\"id\":\"1\",\"a\":[\"caf\u00e9\",\"na\u00efve\"]}");
    stage.processDocument(doc);
    assertEquals(List.of("cafe", "naive"), doc.getStringList("a"));

    Document single = Document.create("single");
    single.setField("a", "caf\u00e9");
    stage.processDocument(single);
    assertFalse(single.isMultiValued("a"));
    assertEquals("cafe", single.getString("a"));
  }
}
