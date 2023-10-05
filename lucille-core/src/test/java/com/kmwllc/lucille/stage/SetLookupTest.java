package com.kmwllc.lucille.stage;

import static org.junit.Assert.*;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class SetLookupTest {

  private final StageFactory factory = StageFactory.of(SetLookup.class);

  @Test
  public void testFileDoesNotExist() {
    Throwable e = assertThrows(StageException.class, () -> factory.get("SetLookupTest/missing_file.conf"));
    assertEquals("File does not exist: classpath:SetLookupTest/missing.txt", e.getMessage());
  }

  @Test
  public void testSetLookup() throws StageException {
    Stage stage = factory.get("SetLookupTest/config.conf");

    Document doc = Document.create("id");
    doc.setField("field", "a");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    doc.setField("field", "hello world");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));
  }

  @Test
  public void testIgnoreCase() throws StageException {
    Stage stage = factory.get("SetLookupTest/config_ignore_case.conf");

    Document doc = Document.create("id");
    doc.setField("field", "A");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));


    doc.setField("field", "HELLO WORLD");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    doc.setField("field", "b");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));
  }

  @Test
  public void testIgnoreMissingSource() throws StageException {

    // by default will return false
    Stage stage = factory.get("SetLookupTest/config.conf");

    Document doc = Document.create("id");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    // will return true
    stage = factory.get("SetLookupTest/config_ignore_missing_source.conf");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));
  }
}