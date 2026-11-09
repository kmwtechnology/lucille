package com.kmwllc.lucille.examples.elasticsearchsmoke;

import static com.kmwllc.lucille.examples.elasticsearchsmoke.ElasticsearchSmokeClient.MATCH_ALL;
import static com.kmwllc.lucille.examples.elasticsearchsmoke.ElasticsearchSmokeClient.count;
import static com.kmwllc.lucille.examples.elasticsearchsmoke.ElasticsearchSmokeClient.documentStatus;
import static com.kmwllc.lucille.examples.elasticsearchsmoke.ElasticsearchSmokeClient.refresh;
import static com.kmwllc.lucille.examples.elasticsearchsmoke.ElasticsearchSmokeClient.termQuery;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Run after the delete-by-id run (data/delete_by_id.csv): document 3 was deleted by its id, and nothing else changed.
 */
public class VerifyAfterDeleteById {

  @Test
  public void testDocumentDeletedById() throws Exception {
    refresh();
    assertEquals(404, documentStatus("3"));
    assertEquals(200, documentStatus("4"));
    assertEquals(19, count(MATCH_ALL));
    assertEquals(13, count(termQuery("group", "keep")));
  }
}
