package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageFactory;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimestampTest {

  private StageFactory factory = StageFactory.of(Timestamp.class);

  @Test
  public void testTimestamp() throws Exception {
    Stage stage = factory.get("TimestampTest/config.conf");

    Document doc = Document.create("doc1");
    stage.processDocument(doc);

    assertTrue(doc.has("timestamp"));
    Instant timestamp = Instant.parse(doc.getString("timestamp"));
    Instant now = Instant.now();

    // timestamp should be at most 2 seconds before now
    assertTrue(ChronoUnit.SECONDS.between(timestamp, now) <= 2);

    // timestamp should not be after now; but allow for it to be the same as now
    assertTrue(ChronoUnit.SECONDS.between(timestamp, now) >= 0);
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("TimestampTest/config.conf");
    assertEquals(Set.of("dest_field", "name", "conditions", "class"), stage.getLegalProperties());
  }
}
