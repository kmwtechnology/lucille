package com.kmwllc.lucille.util;

import static com.kmwllc.lucille.core.HashMapDocument.SUPPORTED_TYPES;
import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.junit.Before;
import org.junit.Test;

public class LinkedMultiMapTest {

  private static final Instant NOW = Instant.now();

  int mapSize;
  MultiMap empty;
  MultiMap map;

  @Before
  public void setup() throws JsonProcessingException {
    empty = new LinkedMultiMap(SUPPORTED_TYPES);

    map = new LinkedMultiMap(SUPPORTED_TYPES);
    map.putOne("string", "a");
    map.putOne("null", null);
    map.putOne("integer", 1);
    map.putOne("double", 1.1);
    map.putOne("long", 1L);
    map.putOne("boolean", true);
    map.putOne("json", makeJson("{\"a\":1, \"b\":2}"));
    map.putOne("instant", NOW);

    // todo add list of things
    map.putMany("list-empty", new ArrayList<>());
    map.putMany("list-string", List.of("a", "b", "c"));
    map.putMany("list-integer", List.of(1, 2, 3, 2, 2, 1, 3, 5));

    mapSize = map.size();
  }

  @Test
  public void testIsEmpty() {
    assertTrue(empty.isEmpty());
    assertFalse(map.isEmpty());
  }

  @Test
  public void testContains() {

    exception(() -> empty.contains(null));
    exception(() -> map.contains(null));

    assertFalse(empty.contains("string"));
    assertFalse(map.contains("string2"));

    assertTrue(map.contains("string"));
    map.remove("string");
    assertFalse(map.contains("string"));
  }

  @Test
  public void testIsMultiValued() {
    // todo
  }

  @Test
  public void testSize() {
    assertEquals(0, empty.size());
    assertEquals(11, map.size());
  }

  @Test
  public void testLength() {

    exception(() -> empty.length(null));
    exception(() -> empty.length("hi"));

    assertEquals(1, map.length("string"));
    assertEquals(0, map.length("list-empty"));
    assertEquals(3, map.length("list-string"));
    assertEquals(8, map.length("list-integer"));
  }

  @Test
  public void testGetKeys() {
    assertEquals(new HashSet<>(), empty.getKeys());
    assertEquals(
        new HashSet<>(
            List.of(
                "string",
                "null",
                "integer",
                "double",
                "long",
                "boolean",
                "json",
                "instant",
                "list-empty",
                "list-string",
                "list-integer")),
        map.getKeys());

    map.rename("string", "newString");

    assertEquals(
        new HashSet<>(
            List.of(
                "newString",
                "null",
                "integer",
                "double",
                "long",
                "boolean",
                "json",
                "instant",
                "list-empty",
                "list-string",
                "list-integer")),
        map.getKeys());
  }

  @Test
  public void testGetSingleKeys() {
    assertEquals(new HashSet<>(), empty.getSingleKeys());
    assertEquals(
        new HashSet<>(
            List.of("string", "null", "integer", "double", "long", "boolean", "json", "instant")),
        map.getSingleKeys());

    map.rename("string", "newString");

    assertEquals(
        new HashSet<>(
            List.of(
                "newString", "null", "integer", "double", "long", "boolean", "json", "instant")),
        map.getSingleKeys());
  }

  @Test
  public void testGetMultiKeys() {
    assertEquals(new HashSet<>(), empty.getMultiKeys());
    assertEquals(
        new HashSet<>(List.of("list-empty", "list-string", "list-integer")), map.getMultiKeys());

    map.rename("list-empty", "list-empty2");

    assertEquals(
        new HashSet<>(List.of("list-empty2", "list-string", "list-integer")), map.getMultiKeys());
  }

  @Test
  public void testGetSingleValued() {
    assertEquals(new HashMap<>(), empty.getSingleValued());
    assertEquals(8, map.getSingleValued().size());
    assertEquals("a", map.getSingleValued().get("string"));
  }

  @Test
  public void testGetMultiValued() {
    assertEquals(new HashMap<>(), empty.getMultiValued());
    assertEquals(3, map.getMultiValued().size());
    assertEquals(List.of(), map.getMultiValued().get("list-empty"));
    assertEquals(List.of("a", "b", "c"), map.getMultiValued().get("list-string"));
  }

  @Test
  public void testGetType() {
    // todo
  }

  @Test
  public void testGetTypes() {
    // todo
  }

  @Test
  public void testGetOne() throws JsonProcessingException {
    exception(() -> empty.getOne(null));
    exception(() -> empty.getOne("hi"));
    exception(() -> map.getOne(null));
    exception(() -> map.getOne("hi"));

    // verifies cannot get from an empty list
    exception(() -> map.getOne("list-empty"));

    assertEquals("a", map.getOne("string"));
    assertNull(map.getOne("null"));
    assertEquals(1, map.getOne("integer"));
    assertEquals(1.1, map.getOne("double"));
    assertEquals(1L, map.getOne("long"));
    assertEquals(true, map.getOne("boolean"));
    assertEquals(NOW, map.getOne("instant"));
    assertEquals(makeJson("{\"a\":1, \"b\":2}"), map.getOne("json"));

    // verifies returns the first in the list
    assertEquals("a", map.getOne("list-string"));
    assertEquals(1, map.getOne("list-integer"));
  }

  @Test
  public void testGetMany() {
    exception(() -> empty.getMany(null));
    exception(() -> empty.getMany("hi"));
    exception(() -> map.getMany(null));
    exception(() -> map.getMany("hi"));

    // verifies that single value is returned as a list
    assertEquals(List.of("a"), map.getMany("string"));

    assertEquals(List.of(), map.getMany("list-empty"));
    assertEquals(List.of("a", "b", "c"), map.getMany("list-string"));
    assertEquals(List.of(1, 2, 3, 2, 2, 1, 3, 5), map.getMany("list-integer"));
  }

  @Test
  public void testPut() {
    exception(() -> empty.putOne(null, "hi"));
    exception(() -> map.putOne(null, "hi"));

    // verifies illegal type
    exception(() -> empty.putOne("hi", Byte.valueOf("1")));

    assertEquals(0, empty.size());
    assertFalse(empty.contains("hi"));
    empty.putOne("hi", "there");
    assertEquals(1, empty.size());
    assertTrue(empty.contains("hi"));
    assertEquals("there", empty.getOne("hi"));
    assertEquals(1, empty.size());
    empty.putOne("hello", null);
    assertEquals(2, empty.size());

    // override
    assertEquals("a", map.getOne("string"));
    map.putOne("string", "b");
    assertEquals("b", map.getOne("string"));

    // override list with single value
    assertTrue(map.isMultiValued("list-string"));
    assertEquals(List.of("a", "b", "c"), map.getMany("list-string"));
    map.putOne("list-string", "d");
    assertFalse(map.isMultiValued("list-string"));
    assertEquals(List.of("d"), map.getMany("list-string"));
  }

  @Test
  public void testPutMany() {
    exception(() -> empty.putMany(null, List.of("hi")));
    exception(() -> empty.putMany("hi", null));

    // verifies valid types and consistency
    exception(() -> empty.putMany("hi", List.of("hi", 1)));
    exception(() -> empty.putMany("hi", List.of(Byte.valueOf("1"))));
    exception(() -> empty.putMany("hi", Arrays.asList(null, 1, null, null, "hello", null)));

    empty.putMany("string", List.of("a", "b", "c"));
    assertEquals(List.of("a", "b", "c"), empty.getMany("string"));

    empty.putMany("null", Collections.singletonList(null));
    assertNull(empty.getOne("null"));
    assertEquals(Collections.singletonList(null), empty.getMany("null"));
    
    empty.putMany("null-list", Arrays.asList(null, null, null));
    assertEquals(Arrays.asList(null, null, null), empty.getMany("null-list"));
    assertNull(empty.getOne("null-list"));
  }

  @Test
  public void testDeepCopy() {
    // todo
  }

  @Test
  public void testAdd() {
    // todo
  }

  @Test
  public void testAddAll() {
    // todo
  }

  @Test
  public void testClear() {
    // todo
  }

  @Test
  public void testRename() {
    exception(() -> empty.rename(null, "string"));
    exception(() -> empty.rename("string", null));
    exception(() -> map.rename(null, "string"));
    exception(() -> map.rename("string", null));

    exception(() -> map.rename("string", "string"));
    exception(() -> map.rename("string", "integer"));

    map.rename("string", "string2");
    assertFalse(map.contains("string"));
    assertTrue(map.contains("string2"));

    map.rename("list-string", "list-string2");
    assertFalse(map.contains("list-string"));
    assertTrue(map.contains("list-string2"));
  }

  @Test
  public void testRemove() {
    exception(() -> empty.remove(null));
    exception(() -> empty.remove("hi"));

    exception(() -> map.remove(null));
    exception(() -> map.remove("hi"));

    assertEquals(11, map.size());
    assertTrue(map.contains("string"));
    map.remove("string");
    assertEquals(10, map.size());
    assertFalse(map.contains("string"));

    assertTrue(map.contains("list-empty"));
    map.remove("list-empty");
    assertEquals(9, map.size());
    assertFalse(map.contains("list-empty"));
  }

  @Test
  public void testRemoveFromArray() {

    exception(() -> map.removeFromArray(null, 0));
    exception(() -> map.removeFromArray("hi", 0));
    exception(() -> map.removeFromArray("list-empty", 0));
    exception(() -> map.removeFromArray("list-string", -1));
    exception(() -> map.removeFromArray("list-string", 3));

    map.removeFromArray("list-string", 1);
    assertEquals(List.of("a", "c"), map.getMany("list-string"));

    assertEquals(List.of(1, 2, 3, 2, 2, 1, 3, 5), map.getMany("list-integer"));
    map.removeFromArray("list-integer", 2);
    assertEquals(List.of(1, 2, 2, 2, 1, 3, 5), map.getMany("list-integer"));
  }

  @Test
  public void testRemoveDuplicates() {
    exception(() -> map.removeDuplicates(null));
    exception(() -> map.removeDuplicates("hi"));
    exception(() -> map.removeDuplicates("string"));

    assertEquals(List.of(1, 2, 3, 2, 2, 1, 3, 5), map.getMany("list-integer"));
    map.removeDuplicates("list-integer");
    assertEquals(List.of(1, 2, 3, 5), map.getMany("list-integer"));
  }

  @Test
  public void testException() {
    exception(
        () -> {
          throw new RuntimeException();
        },
        RuntimeException.class);
  }

  private void exception(Runnable r) {
    exception(r, IllegalArgumentException.class);
  }

  private void exception(Runnable r, Class<?> eClass) {
    try {
      r.run();
      fail("should have thrown " + eClass.getName());
    } catch (Exception e) {
      assertEquals(eClass, e.getClass());
    }
  }

  private static ObjectNode makeJson(String json) throws JsonProcessingException {
    return (ObjectNode) new ObjectMapper().readTree(json);
  }
}
