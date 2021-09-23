package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;

import static org.junit.Assert.*;

public class TimestampTest {

  private StageFactory factory = StageFactory.of(Timestamp.class);

  @Test
  public void testTimestamp() throws Exception {
    Stage stage = factory.get("TimestampTest/config.conf");

    Document doc = new Document("doc1");
    stage.processDocument(doc);

    assertTrue(doc.has("timestamp"));
    Instant timestamp = Instant.parse(doc.getString("timestamp"));
    assertTrue(timestamp.isBefore(Instant.now()));
  }

}
