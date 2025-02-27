package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.List;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrUtilsTest {

  private static final Logger log = LoggerFactory.getLogger(SolrUtilsTest.class);

  @Test
  public void requireAuthTest() throws Exception {
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:SolrUtilsTest/auth.conf"));
    assertTrue(SolrUtils.requiresAuth(config));
  }

  @Test
  public void getHttpClientTest() throws Exception {
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:SolrUtilsTest/auth.conf"));
    Http2SolrClient client = SolrUtils.getHttpClient(config);
    // would like to inspect the solr client to confirm credentials are configured, but can’t do that so just checking it’s non-null
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testToDocument() throws DocumentException {
    Tuple noId = new Tuple();

    assertThrows(DocumentException.class, () -> {
      SolrUtils.toDocument(noId);
    });

    Tuple justId = new Tuple(Document.ID_FIELD, "foo");
    Tuple longTuple = new Tuple(Document.ID_FIELD, "foo1", "long", 1L);
    Tuple doubleTuple = new Tuple(Document.ID_FIELD, "foo2", "double", 2.0);
    Tuple booleanTuple = new Tuple(Document.ID_FIELD, "foo3", "boolean", true);
    Tuple stringTuple = new Tuple(Document.ID_FIELD, "foo4", "string", "bar");
    Tuple longListTuple = new Tuple(Document.ID_FIELD, "foo5", "longList", List.of(1L, 2L));
    Tuple doubleListTuple = new Tuple(Document.ID_FIELD, "foo6", "doubleList", List.of(3.0, 4.0));
    Tuple booleanListTuple = new Tuple(Document.ID_FIELD, "foo7", "booleanList", List.of(false, true));
    Tuple stringListTuple = new Tuple(Document.ID_FIELD, "foo8", "stringList", List.of("foo", "bar"));
    Tuple mapTuple = new Tuple(Document.ID_FIELD, "foo9", "map", new HashMap<>());
    Tuple mixedTuple = new Tuple(Document.ID_FIELD, "foo9", "mixed", List.of(1, "string"));
    Tuple unsupportedList = new Tuple(Document.ID_FIELD, "foo9", "bad", List.of(new HashMap<>()));
    Tuple emptyList = new Tuple(Document.ID_FIELD, "foo9", "empty", List.of());
    Tuple stringArray = new Tuple(Document.ID_FIELD, "foo10", "stringArray", new String[] {"one", "two"});

    Document justIdDoc = SolrUtils.toDocument(justId);
    Document longTupleDoc = SolrUtils.toDocument(longTuple);
    Document doubleTupleDoc = SolrUtils.toDocument(doubleTuple);
    Document booleanTupleDoc = SolrUtils.toDocument(booleanTuple);
    Document stringTupleDoc = SolrUtils.toDocument(stringTuple);
    Document longListTupleDoc = SolrUtils.toDocument(longListTuple);
    Document doubleListTupleDoc = SolrUtils.toDocument(doubleListTuple);
    Document booleanListTupleDoc = SolrUtils.toDocument(booleanListTuple);
    Document stringListTupleDoc = SolrUtils.toDocument(stringListTuple);
    Document emptyListDoc = SolrUtils.toDocument(emptyList);
    Document stringArrayDoc = SolrUtils.toDocument(stringArray);
    Document mixedListDoc = SolrUtils.toDocument(mixedTuple);

    assertThrows(DocumentException.class, () -> {
      SolrUtils.toDocument(mapTuple);
    });
    assertThrows(DocumentException.class, () -> {
      SolrUtils.toDocument(unsupportedList);
    });

    assertEquals(1, justIdDoc.getFieldNames().size());
    assertEquals("foo", justIdDoc.getString(Document.ID_FIELD));

    assertEquals(2, longTupleDoc.getFieldNames().size());
    assertEquals("foo1", longTupleDoc.getString(Document.ID_FIELD));
    assertEquals((Long) 1L, longTupleDoc.getLong("long"));

    assertEquals(2, doubleTupleDoc.getFieldNames().size());
    assertEquals("foo2", doubleTupleDoc.getString(Document.ID_FIELD));
    assertEquals((Double) 2.0, doubleTupleDoc.getDouble("double"));

    assertEquals(2, booleanTupleDoc.getFieldNames().size());
    assertEquals("foo3", booleanTupleDoc.getString(Document.ID_FIELD));
    assertEquals(true, booleanTupleDoc.getBoolean("boolean"));

    assertEquals(2, stringTupleDoc.getFieldNames().size());
    assertEquals("foo4", stringTupleDoc.getString(Document.ID_FIELD));
    assertEquals("bar", stringTupleDoc.getString("string"));

    assertEquals(2, longListTupleDoc.getFieldNames().size());
    assertEquals("foo5", longListTupleDoc.getString(Document.ID_FIELD));
    assertEquals(List.of(1L, 2L), longListTupleDoc.getLongList("longList"));

    assertEquals(2, doubleListTupleDoc.getFieldNames().size());
    assertEquals("foo6", doubleListTupleDoc.getString(Document.ID_FIELD));
    assertEquals(List.of(3.0, 4.0), doubleListTupleDoc.getDoubleList("doubleList"));

    assertEquals(2, booleanListTupleDoc.getFieldNames().size());
    assertEquals("foo7", booleanListTupleDoc.getString(Document.ID_FIELD));
    assertEquals(List.of(false, true), booleanListTupleDoc.getBooleanList("booleanList"));

    assertEquals(2, stringListTupleDoc.getFieldNames().size());
    assertEquals("foo8", stringListTupleDoc.getString(Document.ID_FIELD));
    assertEquals(List.of("foo", "bar"), stringListTupleDoc.getStringList("stringList"));

    assertEquals(1, emptyListDoc.getFieldNames().size());
    assertEquals("foo9", emptyListDoc.getString(Document.ID_FIELD));

    assertEquals(2, stringArrayDoc.getFieldNames().size());
    assertEquals("foo10", stringArrayDoc.getString(Document.ID_FIELD));
    assertEquals(List.of("one", "two"), stringArrayDoc.getStringList("stringArray"));

    assertEquals(2, mixedListDoc.getFieldNames().size());
    assertEquals("foo9", mixedListDoc.getString(Document.ID_FIELD));
    assertEquals(List.of("1", "string"), mixedListDoc.getStringList("mixed"));
  }
}
