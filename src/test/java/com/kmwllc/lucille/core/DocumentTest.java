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
    document.writeToField("field", StageUtils.WriteMode.APPEND, "hello there");
    assertEquals("hello there", document.getStringList("field").get(0));
    document.writeToField("field", StageUtils.WriteMode.APPEND, "some more text", "and some more");
    assertEquals(3, document.getStringList("field").size());
    assertEquals("hello there", document.getStringList("field").get(0));
    assertEquals("some more text", document.getStringList("field").get(1));
    assertEquals("and some more", document.getStringList("field").get(2));
    document.writeToField("field", StageUtils.WriteMode.OVERWRITE, "this is it now");
    assertEquals(1, document.getStringList("field").size());
    assertEquals("this is it now", document.getString("field"));
    document.writeToField("field", StageUtils.WriteMode.SKIP, "this won't be written");
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
  public void testRenameField() {
    Document document = new Document("doc");
    document.addToField("initial", "first");
    document.addToField("initial", "second");
    document.renameField("initial", "final", StageUtils.WriteMode.SKIP);
    List<String> values = document.getStringList("final");
    assertFalse(document.has("initial"));
    assertEquals(2, values.size());
    assertEquals("first", values.get(0));
    assertEquals("second", values.get(1));
    assertFalse(document.has("initial"));

    Document doc2 = new Document("doc2");
    doc2.setField("initial", "first");
    doc2.setField("final", "will be repalced");
    assertTrue(doc2.has("final"));
    doc2.renameField("initial", "final", StageUtils.WriteMode.OVERWRITE);
    assertEquals("first", doc2.getString("final"));

    Document doc3 = new Document("doc3");
    doc3.addToField("final", "first");
    doc3.addToField("final", "second");
    doc3.addToField("initial", "third");
    doc3.addToField("initial", "fourth");
    doc3.renameField("initial", "final", StageUtils.WriteMode.APPEND);
    assertEquals(4, doc3.getStringList("final").size());
    assertEquals("first", doc3.getStringList("final").get(0));
    assertEquals("second", doc3.getStringList("final").get(1));
    assertEquals("third", doc3.getStringList("final").get(2));
    assertEquals("fourth", doc3.getStringList("final").get(3));

    Document doc4 = new Document("doc4");
    doc4.setField("initial", 5);
    doc4.addToField("initial", 22);
    doc4.renameField("initial", "final", StageUtils.WriteMode.OVERWRITE);
    Map<String, Object> map = doc4.asMap();
    List<Object> finalVals = (List<Object>) map.get("final");
    assertEquals(5, finalVals.get(0));
    assertNotEquals(5.0, finalVals.get(0));
    assertNotEquals("5", finalVals.get(0));
    assertEquals(22, finalVals.get(1));
  }
}
