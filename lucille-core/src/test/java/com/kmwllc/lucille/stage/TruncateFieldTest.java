package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class TruncateFieldTest {

  StageFactory factory = StageFactory.of(TruncateField.class);

  @Test 
  public void testRequiredPropertiesNotProvided() {
   assertThrows(StageException.class, () -> {factory.get("TruncateFieldTest/no-source.conf");});
   assertThrows(StageException.class, () -> {factory.get("TruncateFieldTest/no-max-size.conf");});
  }

  @Test 
  public void testIncorrectConfigTypes() {
   assertThrows(StageException.class, () -> {factory.get("TruncateFieldTest/source-config-not-string.conf");});
   assertThrows(StageException.class, () -> {factory.get("TruncateFieldTest/max-size-not-int.conf");});
   assertThrows(StageException.class, () -> {factory.get("TruncateFieldTest/destination-not-string.conf");});
   assertThrows(StageException.class, () -> {factory.get("TruncateFieldTest/negative-size.conf");});
  }

  @Test 
  public void testInPlace() throws StageException {
    Stage stage = factory.get("TruncateFieldTest/in-place.conf");
    Document doc = Document.create("doc1");
    doc.setField("source", "some string");

    stage.processDocument(doc);
    assertEquals("some ", doc.getString("source"));
    assertEquals("doc1", doc.getString("id"));
  }
  
  @Test 
  public void testDestinationField() throws StageException {
    Stage stage = factory.get("TruncateFieldTest/destination-field.conf");
    Document doc = Document.create("doc1");
    doc.setField("source", "some string");

    stage.processDocument(doc);
    assertEquals("some string", doc.getString("source"));
    assertEquals("some ", doc.getString("destination"));
    assertEquals("doc1", doc.getString("id"));
  }

  @Test 
  public void testOverwritesDesinationField() throws StageException {
    Stage stage = factory.get("TruncateFieldTest/destination-field.conf");
    Document doc = Document.create("doc1");
    doc.setField("source", "some string");
    doc.setField("destination", "random content");

    stage.processDocument(doc);
    assertEquals("some string", doc.getString("source"));
    assertEquals("some ", doc.getString("destination"));
    assertEquals("doc1", doc.getString("id"));
  }
}
