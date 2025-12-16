package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DictionaryLookupTest {

  private final StageFactory factory = StageFactory.of(DictionaryLookup.class);

  @Test
  public void testDictionaryLookup() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/config.conf");

    // Ensure that exact matches are correctly extracted
    Document doc = Document.create("doc");
    doc.setField("input1", "China");
    stage.processDocument(doc);
    assertEquals("China", doc.getStringList("output1").get(0));

    // Ensure that multiple matches can be extracted, even in multivalued fields
    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "Canada");
    doc2.addToField("input1", "United States");
    doc2.setField("input3", "Taiwan");
    stage.processDocument(doc2);
    assertEquals("Canada", doc2.getStringList("output1").get(0));
    assertEquals("United States", doc2.getStringList("output1").get(1));
    assertEquals("Taiwan", doc2.getStringList("output3").get(0));

    // ensure that partial matches do not get extracted
    Document doc3 = Document.create("doc2");
    doc3.setField("input2", "United States of America");
    stage.processDocument(doc3);
    assertNull(doc3.getStringList("output2"));

    // Ensure that if there are multiple payloads, that the payloads get mapped to the output as expected.
    Document doc4 = Document.create("doc4");
    doc4.setField("input1", "foo");
    stage.processDocument(doc4);
    // System.out.println(doc4);
    List<String> vals = doc4.getStringList("output1");
    assertEquals(vals.size(), 3);
    assertEquals(vals.get(0), "bar");
    assertEquals(vals.get(1), "baz");
    assertEquals(vals.get(2), "boom");

  }

  @Test
  public void testDictionaryLookupIgnoreCase() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/config_ignore_case.conf");

    Document doc = Document.create("doc");
    // lower case input
    doc.setField("input1", "china");
    stage.processDocument(doc);
    // assert output is original case
    assertEquals("China", doc.getStringList("output1").get(0));

  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/config.conf");
    assertEquals(
        Set.of(
            "ignoreCase",
            "usePayloads",
            "updateMode",
            "name",
            "source",
            "dest",
            "conditions",
            "class",
            "dictPath",
            "setOnly",
            "useAnyMatch",
            "ignoreMissingSource",
            "conditionPolicy",
            "gcp", "azure", "s3"),
        stage.getLegalProperties());
  }

  @Test
  public void testFileDoesNotExist() {
    assertThrows(StageException.class, () -> factory.get("DictionaryLookupTest/missing_file.conf"));
  }

  @Test
  public void testSetLookup() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/set_config.conf");

    Document doc = Document.create("id");
    doc.setField("field", "a");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    doc.setField("field", "hello world");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    // value preserved in the set after it was found
    Document doc2 = Document.create("id2");
    doc2.setField("field", "a");
    stage.processDocument(doc2);
    assertTrue(doc2.getBoolean("setContains"));

    // default to using all match - "SOMETHING_ELSE" not in the dictionary
    Document doc3 = Document.create("doc3");
    doc3.setOrAdd("field", "a");
    doc3.setOrAdd("field", "SOMETHING_ELSE");
    stage.processDocument(doc3);
    assertFalse(doc3.getBoolean("setContains"));
  }

  @Test
  public void testSetLookupAnyMatch() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/set_config_any_match.conf");

    // one field has a match, the other field (source2) is empty / not specified
    Document doc = Document.create("id");
    doc.setField("source1", "a");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    // no matches
    Document doc2 = Document.create("id2");
    doc2.setField("source1", "hello world");
    stage.processDocument(doc2);
    assertFalse(doc2.getBoolean("setContains"));

    // "SOMETHING_ELSE" not in the dictionary, no match on source2
    Document doc3 = Document.create("id3");
    doc3.setOrAdd("source1", "a");
    doc3.setOrAdd("source2", "SOMETHING_ELSE");
    stage.processDocument(doc3);
    assertTrue(doc3.getBoolean("setContains"));

    // both match
    Document doc4 = Document.create("id4");
    doc4.setOrAdd("source1", "a");
    doc4.setOrAdd("source2", "a");
    stage.processDocument(doc4);
    assertTrue(doc4.getBoolean("setContains"));
  }

  @Test
  public void testIgnoreCase() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/set_config_ignore_case.conf");

    Document doc = Document.create("id");
    doc.setField("field", "A");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    doc.setField("field", "HELLO WORLD");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    // this will only work if we remove the existing field from the document before processing
    doc.setField("field", "b");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));
  }

  @Test
  public void testIgnoreMissingSource() throws StageException {

    // by default will return false
    Stage stage = factory.get("DictionaryLookupTest/set_config.conf");

    Document doc = Document.create("id");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    // will return true
    stage = factory.get("DictionaryLookupTest/set_config_ignore_missing_source.conf");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));
  }

  @Test
  public void testIgnoreMultiFieldMissingSource() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/set_config_ignore_missing_source_multi.conf");

    // two fields are missing so we return true
    Document doc = Document.create("id");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    // one field is missing and the one that is present is in the dictionary, so we return true
    doc.setField("field2", "a");
    stage.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    // one field is missing and the one that is present is not in the dictionary, so we return true
    doc.setField("field1", "HELLO WORLD");
    stage.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));
  }

  @Test
  public void testMultiSourceField() throws StageException {
    Stage stage1 = factory.get("DictionaryLookupTest/set_config_multi_field_1_dest.conf");

    Document doc = Document.create("id");
    doc.setField("field1", "a");
    doc.setField("field2", "b");
    // values of both fields are in the dictionary
    stage1.processDocument(doc);
    assertTrue(doc.getBoolean("setContains"));

    // document is missing one of the required fields
    doc.removeField("field2");
    stage1.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    // document has one field that is not in the dictionary
    doc.setField("field2", "Hello world");
    stage1.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));

    // document is missing first required field
    doc.removeField("field1");
    doc.setField("field2", "a");
    stage1.processDocument(doc);
    assertFalse(doc.getBoolean("setContains"));
  }

  @Test
  public void testMultiSourceMultiDest() throws StageException {
    Stage stage1 = factory.get("DictionaryLookupTest/set_config_multi_field_multi_dest.conf");
    Document doc = Document.create("id");

    // values of both fields are in the dictionary
    doc.setField("field1", "a");
    doc.setField("field2", "b");
    stage1.processDocument(doc);
    assertTrue(doc.getBoolean("setContains1"));
    assertTrue(doc.getBoolean("setContains2"));

    // document is missing one of the required fields
    doc.removeField("field2");
    stage1.processDocument(doc);
    assertTrue(doc.getBoolean("setContains1"));
    assertFalse(doc.getBoolean("setContains2"));

    // document has one field that is not in the dictionary
    doc.setField("field2", "Hello world");
    stage1.processDocument(doc);
    assertTrue(doc.getBoolean("setContains1"));
    assertFalse(doc.getBoolean("setContains2"));

    // document is missing first required field
    doc.setField("field1", "hello world");
    doc.setField("field2", "a");
    stage1.processDocument(doc);
    assertFalse(doc.getBoolean("setContains1"));
    assertTrue(doc.getBoolean("setContains2"));
  }

  @Test
  public void testInvalidSet() {
    assertThrows(StageException.class, () -> factory.get("DictionaryLookupTest/set_config_invalid.conf"));
  }

  @Test
  public void testInvalidConfig() {
    assertThrows(StageException.class, () -> factory.get("DictionaryLookupTest/set_config_invalid_update1.conf"));

    Throwable e = assertThrows(StageException.class, () -> factory.get("DictionaryLookupTest/set_config_invalid_update2.conf"));
    assertEquals("when setOnly is true, updateMode must be set to overwrite", e.getMessage());
  }
}
