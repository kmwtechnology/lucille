package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class LengthTest {

  private static final StageFactory factory = StageFactory.of(Length.class);

  @Test
  public void testLength() throws StageException {
    Stage stage = factory.get("LengthTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("singleton", "one value");
    doc1.setField("customers", "Ace Hardware");
    doc1.setOrAdd("customers", "Home Depot");
    doc1.setOrAdd("customers", "Lowes Hardware");
    stage.processDocument(doc1);
    assertEquals("0", doc1.getString("empty_length"));
    assertEquals("3", doc1.getString("customers_length"));
    assertEquals("1", doc1.getString("singleton_length"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("LengthTest/config.conf");
    assertEquals(Set.of("name", "conditions", "class", "conditionReductionLogic"), stage.getLegalProperties());
  }
}
