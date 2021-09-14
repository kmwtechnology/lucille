package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.util.StageUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class DocumentTest {

  @Test(expected = NullPointerException.class)
  public void testCreateWithoutId1() throws Exception {
    new Document((String) null);
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId2() throws Exception {
    Document.fromJsonString("{\"field1\":\"val1\"}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId3() throws Exception {
    Document.fromJsonString("{\"id\":\"\"}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId4() throws Exception {
    Document.fromJsonString("{\"id\":null}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId5() throws Exception {
    Document.fromJsonString("{\"id\":1}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId6() throws Exception {
    new Document(new ObjectMapper().createObjectNode());
  }

  @Test
  public void testCreateFromJsonString() throws Exception {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    Document document = Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
    assertEquals("val1", document.getString("field1"));
    assertEquals("val2", document.getString("field2"));
  }

  @Test
  public void testCreateFromID() throws Exception {
    Document document = new Document("123");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
  }

  @Test
  public void testSetAndGetField() throws Exception {
    Document document = new Document("123");
    assertFalse(document.has("field1"));
    document.setField("field1", "val1");
    assertTrue(document.has("field1"));
    assertEquals("val1", document.getString("field1"));
  }

  @Test
  public void testGetUnsetField() {
    Document document = new Document("id");
    assertFalse(document.has("test_field"));
    assertNull(document.getString("test_field"));
    assertNull(document.getStringList("test_field"));
  }

  @Test
  public void testAddToField() throws Exception {
    Document document = new Document("123");
    assertFalse(document.has("field1"));
    document.addToField("field1", "val1");
    document.addToField("field1", "val2");
    document.addToField("field1", "val3");
    List<String> expected = Arrays.asList("val1", "val2", "val3");
    assertEquals(expected, document.getStringList("field1"));
  }

  @Test
  public void testWriteToField() throws Exception {
    Document document = new Document("doc");
    assertFalse(document.has("field"));
    document.update("field", UpdateMode.APPEND, "hello there");
    assertEquals("hello there", document.getStringList("field").get(0));
    document.update("field", UpdateMode.APPEND, "some more text", "and some more");
    assertEquals(3, document.getStringList("field").size());
    assertEquals("hello there", document.getStringList("field").get(0));
    assertEquals("some more text", document.getStringList("field").get(1));
    assertEquals("and some more", document.getStringList("field").get(2));
    document.update("field", UpdateMode.OVERWRITE, "this is it now");
    assertEquals(1, document.getStringList("field").size());
    assertEquals("this is it now", document.getString("field"));
    document.update("field", UpdateMode.SKIP, "this won't be written");
    assertEquals(1, document.getStringList("field").size());
    assertEquals("this is it now", document.getString("field"));
  }

  @Test
  public void testGetStringsSingleValued() {
    Document document = new Document("doc");
    document.setField("pets", "dog");
    assertFalse(document.isMultiValued("pets"));
    assertEquals("dog", document.getString("pets"));
    assertEquals(Collections.singletonList("dog"), document.getStringList("pets"));
  }

  @Test
  public void testGetStringsMultiValued() {
    Document document = new Document("doc");
    document.setField("pets", "dog");
    assertFalse(document.isMultiValued("pets"));
    document.addToField("pets", "cat");
    assertTrue(document.isMultiValued("pets"));
    document.addToField("pets", "fish");
    assertEquals(Arrays.asList("dog", "cat", "fish"), document.getStringList("pets"));
  }


  @Test
  public void testGetStringMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", "val1");
    document.addToField("field1", "val2");
    assertEquals("val1", document.getString("field1"));
  }

  @Test
  public void testChildren() throws Exception {
    Document parent = new Document("parent");
    Document child1 = new Document("child1");
    child1.setField("field1", "val1");
    Document child2 = new Document("child2");
    child2.setField("field1", "val1b");
    child1.setField("field2", "val2");
    parent.addChild(child1);
    parent.addChild(child2);
    List<Document> children = parent.getChildren();
    assertEquals(2, children.size());
    assertEquals(child1, children.get(0));
    assertEquals(child2, children.get(1));
    Document deserializedParent = Document.fromJsonString(parent.toString());
    assertEquals(parent, Document.fromJsonString(parent.toString()));
    List<Document> deserializedChildren = deserializedParent.getChildren();
    assertEquals(child1, deserializedChildren.get(0));
    assertEquals(child2, deserializedChildren.get(1));
  }

  @Test
  public void testEmptyChildren() throws Exception {
    Document parent = new Document("parent");
    List<Document> children = parent.getChildren();
    assertEquals(0, children.size());
    assertEquals(parent, Document.fromJsonString(parent.toString()));
  }

  @Test
  public void testNullHandling() throws Exception {
    // set a field to null and confirm that we get back a null when we call getString(), not the string "null"
    Document document = new Document("doc");
    document.setField("field1", (String)null);
    assertEquals(null, document.getString("field1"));

    // convert the field to a list, add another null, and confirm that getStringList returns an array with two nulls
    document.addToField("field1", (String) null);
    List<String> field1 = document.getStringList("field1");
    assertEquals(null, field1.get(0));
    assertEquals(null, field1.get(1));
    assertEquals(2, field1.size());

    // stringify the document and recreate it from the string; confirm getStringList still returns array with two nulls
    assertEquals("{\"id\":\"doc\",\"field1\":[null,null]}", document.toString());
    document = Document.fromJsonString(document.toString());
    field1 = document.getStringList("field1");
    assertEquals(null, field1.get(0));
    assertEquals(null, field1.get(1));
    assertEquals(2, field1.size());
  }

  @Test
  public void testRenameField() {
    Document document = new Document("doc");
    document.addToField("initial", "first");
    document.addToField("initial", "second");
    document.renameField("initial", "final", UpdateMode.SKIP);
    List<String> values = document.getStringList("final");
    assertFalse(document.has("initial"));
    assertEquals(2, values.size());
    assertEquals("first", values.get(0));
    assertEquals("second", values.get(1));
    assertFalse(document.has("initial"));
  }

  @Test
  public void testRenameOverwrite() {
    Document document = new Document("document");
    document.setField("initial", "first");
    document.setField("final", "will be repalced");
    assertTrue(document.has("final"));
    document.renameField("initial", "final", UpdateMode.OVERWRITE);
    assertEquals("first", document.getString("final"));
  }

  @Test
  public void testRenameAppend() {
    Document document = new Document("document");
    document.addToField("final", "first");
    document.addToField("final", "second");
    document.addToField("initial", "third");
    document.addToField("initial", "fourth");
    document.renameField("initial", "final", UpdateMode.APPEND);
    assertEquals(4, document.getStringList("final").size());
    assertEquals("first", document.getStringList("final").get(0));
    assertEquals("second", document.getStringList("final").get(1));
    assertEquals("third", document.getStringList("final").get(2));
    assertEquals("fourth", document.getStringList("final").get(3));
  }

  @Test
  public void testRenamePreservesTypes() {
    Document document = new Document("document");
    document.setField("initial", 5);
    document.addToField("initial", 22);
    document.renameField("initial", "final", UpdateMode.OVERWRITE);
    Map<String, Object> map = document.asMap();
    List<Object> finalVals = (List<Object>) map.get("final");
    assertEquals(5, finalVals.get(0));
    assertNotEquals(5.0, finalVals.get(0));
    assertNotEquals("5", finalVals.get(0));
    assertEquals(22, finalVals.get(1));
  }
}
