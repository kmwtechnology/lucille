package com.kmwllc.lucille.stage;

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

    assertTrue(startDate.before(doc1.getDate("data")) && endDate.after(doc1.getDate("data")));
    assertTrue(startDate.before(doc2.getDate("data")) && endDate.after(doc2.getDate("data")));
    assertTrue(startDate.before(doc3.getDate("data")) && endDate.after(doc3.getDate("data")));
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

    assertTrue(startDate.before(doc1.getDate("randomDate")) && endDate.after(doc1.getDate("randomDate")));
    assertTrue(startDate.before(doc2.getDate("randomDate")) && endDate.after(doc2.getDate("randomDate")));
    assertTrue(startDate.before(doc3.getDate("randomDate")) && endDate.after(doc3.getDate("randomDate")));
  }
}
