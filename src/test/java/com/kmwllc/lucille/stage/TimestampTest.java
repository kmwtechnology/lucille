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

  @Test
  public void testTimestamp() throws Exception {
    Config config = ConfigFactory.load("TimestampTest/config.conf");
    Stage stage = new Timestamp(config);

    Document doc = new Document("doc1");
    stage.processDocument(doc);

    assertTrue(doc.has("timestamp"));
    Instant timestamp = Instant.parse(doc.getString("timestamp"));
    assertTrue(timestamp.isBefore(Instant.now()));
  }

}
