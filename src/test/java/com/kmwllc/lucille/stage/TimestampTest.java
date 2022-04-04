package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertTrue;

public class TimestampTest {

  private StageFactory factory = StageFactory.of(Timestamp.class);

  @Test
  public void testTimestamp() throws Exception {
    Stage stage = factory.get("TimestampTest/config.conf");

    Document doc = new Document("doc1");
    stage.processDocument(doc);

    assertTrue(doc.has("timestamp"));
    Instant timestamp = Instant.parse(doc.getString("timestamp"));
    Instant now = Instant.now();

    // timestamp should be at most 2 seconds before now
    assertTrue(ChronoUnit.SECONDS.between(timestamp, now) <= 2);

    // timestamp should not be after now; but allow for it to be the same as now
    assertTrue(ChronoUnit.SECONDS.between(timestamp, now) >= 0);
  }

}
