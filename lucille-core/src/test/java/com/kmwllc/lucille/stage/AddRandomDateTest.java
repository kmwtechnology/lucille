package com.kmwllc.lucille.stage;

import java.util.Map;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import org.junit.Test;

public class AddRandomDateTest {

  private final StageFactory factory = StageFactory.of(AddRandomDate.class);

  /**
   *
   */
  @Test( expected = StageException.class )
  public void testInvalidStartEnd() throws StageException {
    Stage stage = factory.get("AddRandomDateTest/invalid.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);
  }

  /**
   * Validates that random dates are produced within the default Range
   */
  @Test
  public void testDefaultDateRange() throws StageException {
    Stage stage = factory.get();
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    Date startDate = Date.from(Instant.ofEpochMilli(0));
    Date endDate = Date.from(Instant.now());

    assertTrue(startDate.toInstant().isBefore(doc1.getInstant("data")) && endDate.toInstant().isAfter(doc1.getInstant("data")));
    assertTrue(startDate.toInstant().isBefore(doc2.getInstant("data")) && endDate.toInstant().isAfter(doc2.getInstant("data")));
    assertTrue(startDate.toInstant().isBefore(doc3.getInstant("data")) && endDate.toInstant().isAfter(doc3.getInstant("data")));
  }

  /**
   * Validates that random dates are produced within a custom Range
   */
  @Test
  public void testCustomDateRange() throws StageException {
    Stage stage = factory.get("AddRandomDateTest/customrange.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    Date startDate = Date.from(
        LocalDate.of(2024, 11, 20)
            .atStartOfDay().toInstant(ZoneOffset.UTC));
    Date endDate = Date.from(
        LocalDate.of(2024, 11, 30)
            .atStartOfDay().toInstant(ZoneOffset.UTC));

    assertTrue(startDate.toInstant().isBefore(doc1.getInstant("randomDate")) && endDate.toInstant().isAfter(doc1.getInstant("randomDate")));
    assertTrue(startDate.toInstant().isBefore(doc2.getInstant("randomDate")) && endDate.toInstant().isAfter(doc2.getInstant("randomDate")));
    assertTrue(startDate.toInstant().isBefore(doc3.getInstant("randomDate")) && endDate.toInstant().isAfter(doc3.getInstant("randomDate")));
  }

  @Test
  public void testEqualBoundsFailAtStartup() {
    assertThrows(StageException.class, () -> factory.get(Map.of("rangeStartDate", "2024-12-18", "rangeEndDate", "2024-12-18")));
  }
}
