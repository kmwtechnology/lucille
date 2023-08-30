package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Set;

import static junit.framework.TestCase.*;


@RunWith(JUnit4.class)
public class MatchQueryTest {

  private StageFactory factory = StageFactory.of(MatchQuery.class);

  @Test
  public void testMatchQueryStage() throws Exception {
    Stage matchQueryStage = factory.get("MatchQueryTest/config.conf");
    String matchedQueriesField = matchQueryStage.getConfig().getString(MatchQuery.MATCHEDQUERIES_PARAM);

    Document d1 = Document.create("d1");
    d1.setField("content", "giraffe");
    matchQueryStage.processDocument(d1);
    assertTrue("One query should have matched document d1. ",
        d1.has(matchedQueriesField) && d1.getStringList(matchedQueriesField).size() == 1);
    assertTrue("d1 should match only query2", d1.getStringList(matchedQueriesField).contains("query2"));

    Document d2 = Document.create("d2");
    d2.setField("content", "giraffe test foo");
    matchQueryStage.processDocument(d2);
    assertTrue("Two queries should have matched document d2. ",
        d2.has(matchedQueriesField) && d2.getStringList(matchedQueriesField).size() == 2);
    assertTrue("d2 should match query1", d2.getStringList(matchedQueriesField).contains("query1"));
    assertTrue("d2 should match query2", d2.getStringList(matchedQueriesField).contains("query2"));

    Document d3 = Document.create("d3");
    d3.setField("content", "test");
    d3.setField("table", "geotrans");
    matchQueryStage.processDocument(d3);
    assertTrue("Two queries should have matched document d3. ",
        d3.has(matchedQueriesField) && d3.getStringList(matchedQueriesField).size() == 2);
    assertTrue("d3 should match query2", d3.getStringList(matchedQueriesField).contains("query2"));
    assertTrue("d3 should match not_asia", d3.getStringList(matchedQueriesField).contains("not_asia"));

    Document d4 = Document.create("d4");
    d4.setField("content", "foobar");
    matchQueryStage.processDocument(d4);
    assertFalse("No queries should have matched document d4. ", d4.has(matchedQueriesField));

    Document d5 = Document.create("d5");
    d5.setField("table", "geotrans");
    d5.setField("country", "japan");
    matchQueryStage.processDocument(d5);
    assertTrue("One query should have matched document d5. ",
        d5.has(matchedQueriesField) && d5.getStringList(matchedQueriesField).size() == 1);
    assertTrue("d5 should match japan", d5.getStringList(matchedQueriesField).contains("japan"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("MatchQueryTest/config.conf");
    assertEquals(
        Set.of("matchedQueriesField", "name", "fields", "conditions", "queries", "class"),
        stage.getLegalProperties());
  }
}
