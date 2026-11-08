package com.kmwllc.lucille.examples.opensearchsmoke;

import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.MATCH_ALL;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.count;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.refresh;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.termQuery;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Run after the delete-by-query run (data/delete_by_query.csv): every document with group=obsolete was deleted by the
 * query, and the group=keep documents that were left are untouched.
 */
public class VerifyAfterDeleteByQuery {

  @Test
  public void testObsoleteDocumentsDeletedByQuery() throws Exception {
    refresh();
    assertEquals(0, count(termQuery("group", "obsolete")));
    assertEquals(13, count(termQuery("group", "keep")));
    assertEquals(13, count(MATCH_ALL));
  }
}
