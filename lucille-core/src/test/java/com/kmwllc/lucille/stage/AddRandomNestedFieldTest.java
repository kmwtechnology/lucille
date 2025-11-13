package com.kmwllc.lucille.stage;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class AddRandomNestedFieldTest {

  private final StageFactory factory = StageFactory.of(AddRandomNestedField.class);

  @Test
  public void testValidScalarSingle() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/valid_scalar.conf");
    Document d = Document.create("d1");
    d.setField("name", "Alice");
    d.setField("age", 30);

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertTrue(out.isArray());
    assertEquals(1, out.size());
    JsonNode obj = out.get(0);
    assertEquals("Alice", obj.at("/user/name").asText());
    assertEquals(30, obj.at("/user/age").asInt());
  }

  @Test
  public void testNumObjectsFixedTwo() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/num_objects_2.conf");
    Document d = Document.create("d2");
    d.setField("city", "Boston");

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertTrue(out.isArray());
    assertEquals(2, out.size());
    assertEquals("Boston", out.get(0).at("/location/city").asText());
    assertEquals("Boston", out.get(1).at("/location/city").asText());
  }

  @Test
  public void testRangeMinMaxThree() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/range_minmax_3.conf");
    Document d = Document.create("d3");
    d.setField("title", "Engineer");

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertTrue(out.isArray());
    assertEquals(3, out.size());
    for (JsonNode obj : out) {
      assertEquals("Engineer", obj.at("/job/title").asText());
    }
  }

  /** Missing source and no generator configured -> should throw at process time. */
  @Test
  public void testMissingValueNoGenerator() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/missing_no_generator.conf");
    Document d = Document.create("d4");
    assertThrows(StageException.class, () -> s.processDocument(d));
  }

  @Test
  public void testGeneratorScalar_UsingAddRandomInt() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/generator_scalar_int.conf");
    Document d = Document.create("d5");

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertEquals(1, out.size());
    assertEquals(0, out.get(0).at("/user/id").asInt());
  }

  @Test
  public void testGeneratorList_UsingAddRandomString() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/generator_list_strings.conf");
    Document d = Document.create("d6");

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertEquals(1, out.size());
    JsonNode tags = out.get(0).get("tags");
    assertTrue(tags.isArray());
    assertEquals(2, tags.size());
    assertEquals("0", tags.get(0).asText());
    assertEquals("0", tags.get(1).asText());
  }

  @Test
  public void testInvalidNumObjectsZero() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomNestedFieldTest/invalid_num_zero.conf"));
  }

  @Test
  public void testInvalidOnlyMin() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomNestedFieldTest/invalid_only_min.conf"));
  }

  @Test
  public void testInvalidOnlyMax() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomNestedFieldTest/invalid_only_max.conf"));
  }

  @Test
  public void testInvalidNumAndRange() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomNestedFieldTest/invalid_num_and_range.conf"));
  }

  @Test
  public void testInvalidGeneratorMissingClass() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomNestedFieldTest/invalid_generator_missing_class.conf"));
  }

  @Test
  public void testInvalidEmptyEntries() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomNestedFieldTest/invalid_empty_entries.conf"));
  }

  @Test
  public void testNullSourceSkipped() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/null_source_skipped.conf");
    Document d = Document.create("d7");
    d.setField("deleted_src", NullNode.instance);

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertTrue(out.isArray());
    assertEquals(0, out.size());
  }

  @Test
  public void testSourceWinsOverGenerator() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/source_wins_over_generator.conf");
    Document d = Document.create("d8");
    d.setField("intGen", 42);

    s.processDocument(d);

    JsonNode out = d.getJson("out");
    assertEquals(1, out.size());
    assertEquals(42, out.get(0).at("/user/id").asInt());
  }

  @Test
  public void testGeneratorTempFieldIsCleaned() throws StageException {
    Stage s = factory.get("AddRandomNestedFieldTest/generator_scalar_int.conf");
    Document d = Document.create("d9");

    s.processDocument(d);

    assertFalse(d.has(".arnf_gen.intGen"));

    JsonNode out = d.getJson("out");
    assertEquals(1, out.size());
    assertEquals(0, out.get(0).at("/user/id").asInt());
  }
}