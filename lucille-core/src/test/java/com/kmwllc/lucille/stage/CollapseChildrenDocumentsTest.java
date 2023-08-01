package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class CollapseChildrenDocumentsTest {

  private StageFactory factory = StageFactory.of(CollapseChildrenDocuments.class);

  @Test
  public void testCollapsedFields() throws StageException {
    Stage stage = factory.get("CollapseChildrenDocumentsTest/config.conf");

    Document doc = Document.create("doc");
    doc.setField("parent", true);
    
    Document child1 = Document.create("child1");
    child1.setField("childfield1", "foo1");
    child1.setField("childfield2", "bar1");
    doc.addChild(child1);

    Document child2 = Document.create("child2");
    child2.setField("childfield1", "foo2");
    child2.setField("childfield2", "bar2");
    child2.setField("childfield3", "baz2");
    doc.addChild(child2);

    stage.processDocument(doc);
    
    assertFalse(doc.hasChildren());
    assertTrue(doc.has("childfield1"));
    assertTrue(doc.has("childfield2"));
    assertFalse(doc.has("childfield3"));
    
    List<String> values = doc.getStringList("childfield1");
    assertEquals(values.size(), 2);
    assertEquals(values.get(0), "foo1");
    assertEquals(values.get(1), "foo2");
    
  }

  @Test
  public void testCollapsedFieldsKeepChildren() throws StageException {
    Stage stage = factory.get("CollapseChildrenDocumentsTest/config_keep_children.conf");

    Document doc = Document.create("doc");
    doc.setField("parent", true);
    
    Document child1 = Document.create("child1");
    child1.setField("childfield1", "foo1");
    child1.setField("childfield2", "bar1");
    doc.addChild(child1);

    Document child2 = Document.create("child2");
    child2.setField("childfield1", "foo2");
    child2.setField("childfield2", "bar2");
    child2.setField("childfield3", "baz2");
    doc.addChild(child2);

    stage.processDocument(doc);
    
    assertTrue(doc.hasChildren());
    assertTrue(doc.has("childfield1"));
    assertTrue(doc.has("childfield2"));
    assertFalse(doc.has("childfield3"));
    
    List<String> values = doc.getStringList("childfield1");
    assertEquals(values.size(), 2);
    assertEquals(values.get(0), "foo1");
    assertEquals(values.get(1), "foo2");
    
  }

  
}
