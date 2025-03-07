import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.parquet.core.fileHandler.ParquetFileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;

public class ParquetFileHandlerTest {

  File exampleFile = new File("src/test/resources/ParquetFileHandlerTest/example.parquet");

  Config defaultConfig = ConfigFactory.parseMap(Map.of("idField", "id"));

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
    ParquetFileHandler handler = new ParquetFileHandler(ConfigFactory.parseMap(Map.of(
        "idField", "id",
        "limit", 3L)));
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
    ParquetFileHandler handler = new ParquetFileHandler(ConfigFactory.parseMap(Map.of(
        "idField", "id",
        "numToSkip", 3L)));
    Iterator<Document> docIterator = handler.processFile(new FileInputStream(exampleFile), "");

    for (int i = 4; i <= 6; i++) {
      assertTrue(docIterator.hasNext());
      Document doc = docIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(docIterator.hasNext());
  }

  @Test
  public void testBadProcessing() throws IOException {
    ParquetFileHandler handler = new ParquetFileHandler(defaultConfig);

    InputStream ioExceptionStream = mock(InputStream.class);
    when(ioExceptionStream.readAllBytes()).thenThrow(new IOException("Mock Exception"));

    assertThrows(FileHandlerException.class, () -> handler.processFile(ioExceptionStream, "testFile"));
  }

  @Test
  public void testNoIDFieldInSchema() {
    ParquetFileHandler badIDHandler = new ParquetFileHandler(ConfigFactory.parseMap(Map.of("idField", "abcDEFghi")));
    assertThrows(IllegalArgumentException.class, () -> badIDHandler.processFile(new FileInputStream(exampleFile), ""));
  }
}
