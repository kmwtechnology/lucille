import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.parquet.connector.ParquetFileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;

public class ParquetFileHandlerTest {

  File exampleFile = new File("src/test/resources/ParquetFileHandlerTest/example.parquet");

  Config defaultConfig = ConfigFactory.parseMap(Map.of("idField", "id"));
  Config limitConfig = ConfigFactory.parseMap(Map.of(
      "idField", "id",
      "limit", 3L));
  Config startConfig = ConfigFactory.parseMap(Map.of(
      "idField", "id",
      "start", 3L));

  @Test
  public void testProcessFile() throws FileHandlerException, IOException {
    ParquetFileHandler handler = new ParquetFileHandler(defaultConfig);

    Iterator<Document> docIterator = handler.processFile(new FileInputStream(exampleFile), "");

    for (int i = 1; i <= 6; i++) {
      assertTrue(docIterator.hasNext());
      Document doc = docIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(docIterator.hasNext());

    // Processing on the same handler. Limit is specific to each document.
    docIterator = handler.processFile(new FileInputStream(exampleFile), "");

    for (int i = 1; i <= 6; i++) {
      assertTrue(docIterator.hasNext());
      Document doc = docIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(docIterator.hasNext());
  }

  @Test
  public void testLimit() throws FileHandlerException, IOException {
    ParquetFileHandler handler = new ParquetFileHandler(limitConfig);
    Iterator<Document> docIterator = handler.processFile(new FileInputStream(exampleFile), "");

    for (int i = 1; i <= 3; i++) {
      assertTrue(docIterator.hasNext());
      Document doc = docIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(docIterator.hasNext());
  }

  @Test
  public void testStart() throws FileHandlerException, IOException {
    ParquetFileHandler handler = new ParquetFileHandler(startConfig);
    Iterator<Document> docIterator = handler.processFile(new FileInputStream(exampleFile), "");

    for (int i = 4; i <= 6; i++) {
      assertTrue(docIterator.hasNext());
      Document doc = docIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(docIterator.hasNext());
  }
}
