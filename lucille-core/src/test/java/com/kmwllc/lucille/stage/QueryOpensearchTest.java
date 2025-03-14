package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class QueryOpensearchTest {

  private final StageFactory factory = StageFactory.of(QueryOpensearch.class);

  @Test
  public void testOpensearchQuery() throws Exception {
    Stage stage = factory.get("QueryOpensearchTest/opensearchQuery.conf");

    Document testDoc = Document.create("test_doc");
    stage.processDocument(testDoc);

    System.out.println(testDoc.getString("response"));
  }

  @Test
  public void testDocumentQueryField() throws Exception {
    Stage stage = factory.get("QueryOpensearchTest/documentQuery.conf");

    Document hasQuery = Document.create("test_doc");
    hasQuery.setField("query", """
        {
          "query": {
            "match_phrase": {
              "park_name": "Neck Creek Preserve"
            }
          }
        }""");

    Document noQuery = Document.create("test_doc_no_query");

    stage.processDocument(hasQuery);
    stage.processDocument(noQuery);

    assertTrue(hasQuery.has("response"));
    assertFalse(noQuery.has("response"));
  }

  @Test
  public void testOpensearchAndDocumentQuery() throws Exception {
    Stage stage = factory.get("QueryOpensearchTest/twoQueries.conf");

    Document hasQuery = Document.create("test_doc");
    hasQuery.setField("query", """
        {
          "query": {
            "match_phrase": {
              "park_name": "Neck Creek Preserve"
            }
          }
        }""");

    Document noQuery = Document.create("test_doc_no_query");

    stage.processDocument(hasQuery);
    stage.processDocument(noQuery);

    assertTrue(hasQuery.has("response"));
    assertTrue(noQuery.has("response"));

    System.out.println(hasQuery.getString("response"));
    System.out.println(noQuery.getString("response"));
  }

  @Test
  public void testDestinationField() throws Exception {
    Stage stage = factory.get("QueryOpensearchTest/specialDestination.conf");

    Document testDoc = Document.create("test_doc");
    stage.processDocument(testDoc);

    System.out.println(testDoc.getString("special_destination"));
  }

  @Test
  public void testDefaultResponseField() throws Exception {
    Stage stage = factory.get("QueryOpensearchTest/defaultResponseField.conf");

    Document testDoc = Document.create("test_doc");
    stage.processDocument(testDoc);

    System.out.println(testDoc.getString("response"));
  }

  @Test
  public void testBadConfs() {
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noOpensearch.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noQueries.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/partialOpensearch.conf"));
  }
}
