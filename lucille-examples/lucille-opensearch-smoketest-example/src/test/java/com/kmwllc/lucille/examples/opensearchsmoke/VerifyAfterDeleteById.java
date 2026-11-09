package com.kmwllc.lucille.examples.opensearchsmoke;

import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.MATCH_ALL;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.count;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.documentStatus;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.refresh;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.termQuery;
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
