package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class HashFieldValueToBucketTest {

  private final StageFactory factory = StageFactory.of(HashFieldValueToBucket.class);

  @Test
  public void testHashDocIdStage() throws StageException {
    // Validate that the value from the id field properly hashes to the bucket called "shard4" 
    Stage stage = factory.get("HashFieldValueToBucketTest/hashdocid.conf");
    Document doc = Document.create("id");
    stage.processDocument(doc);
    assertEquals("shard4", doc.getString("bucket"));
  }

}
