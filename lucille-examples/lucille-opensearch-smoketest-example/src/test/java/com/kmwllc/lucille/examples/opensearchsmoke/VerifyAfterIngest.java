package com.kmwllc.lucille.examples.opensearchsmoke;

import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.MATCH_ALL;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.count;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.refresh;
import static com.kmwllc.lucille.examples.opensearchsmoke.OpenSearchSmokeClient.termQuery;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Run after the ingest run (data/ingest.csv): every bulk request landed, so all 20 documents are in the index. The
 * documents with group=keep are 1 to 14 and the ones with group=obsolete are 15 to 20.
 */
public class VerifyAfterIngest {

  @Test
  public void testAllDocumentsIndexed() throws Exception {
    refresh();
    assertEquals(20, count(MATCH_ALL));
    assertEquals(14, count(termQuery("group", "keep")));
    assertEquals(6, count(termQuery("group", "obsolete")));
  }
}
