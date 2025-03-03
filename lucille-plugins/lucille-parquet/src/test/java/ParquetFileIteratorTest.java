import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.parquet.connector.ByteArrayInputFile;
import com.kmwllc.lucille.parquet.connector.ParquetFileIterator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.junit.Test;

public class ParquetFileIteratorTest {
  Path examplePath = Paths.get("src/test/resources/ParquetFileIteratorTest/example.parquet");
  Path exampleWithRowsPath = Paths.get("src/test/resources/ParquetFileIteratorTest/example_with_rows.parquet");
  Path exampleNonPrimitivePath = Paths.get("src/test/resources/ParquetFileIteratorTest/example_with_non_primitive.parquet");
  Path exampleNonPrimitiveWithRowsPath = Paths.get("src/test/resources/ParquetFileIteratorTest/example_with_rows_and_non_primitive.parquet");

  byte[] exampleContents = Files.readAllBytes(examplePath);
  byte[] exampleWithRowsContents = Files.readAllBytes(exampleWithRowsPath);
  byte[] exampleNonPrimitiveContents = Files.readAllBytes(exampleNonPrimitivePath);
  byte[] exampleNonPrimitiveWithRowsContents = Files.readAllBytes(exampleNonPrimitiveWithRowsPath);

  // need to declare the IOException here so we can call readAllBytes above
  public ParquetFileIteratorTest() throws IOException {}

  /*
  The Parquet file is this:

  {
    'name': ['Oliver', 'Mia', 'Jasper', 'Mr. Meow', 'Elijah', 'Spot'],
    'age': [20, 35, 46, 8, 7, 3],
    'net_worth': [10.0, 12.5, 7.5, 0.0, 15.3, -2.0],
    'species': ['Human', 'Human', 'Human', 'Cat', 'Human', 'Dog'],
    'id': ['1', '2', '3', '4', '5', '6']

    (for the non-primitive file, also:)
    'hobbies': [['Reading', 'Running'], ['Cooking', 'Painting'], ['Gaming', 'Reading'],
                 ['Sleeping', 'Chasing mice'], ['Drawing', 'Coding'], ['Playing', 'Sleeping']]
  }

  The "with_rows" file has three pages (row groups), each with two rows to read. (1 and 2, 3 and 4, 5 and 6...)
   */

  @Test
  public void testIterator() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    assertEquals(6, reader.getRecordCount());

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, -1);

    for (int i = 1; i <= reader.getRecordCount(); i++) {
      assertTrue(testIterator.hasNext());
      Document doc = testIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(testIterator.hasNext());
  }

  @Test
  public void testIteratorLimit() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, 2);

    assertTrue(testIterator.hasNext());
    Document doc = testIterator.next();
    assertEquals("1", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("2", doc.getId());

    assertFalse(testIterator.hasNext());

    // We throw an actual exception when the limit has been reached.
    assertThrows(NoSuchElementException.class,
        () -> testIterator.next());
  }

  @Test
  public void testIteratorStart() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    // There are 6 records and start is set to "6". So this document gets skipped
    // entirely...
    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 6, -1);
    assertFalse(testIterator.hasNext());

    // This skips the first three records.
    reader = ParquetFileReader.open(byteInputFile);
    testIterator = new ParquetFileIterator(reader, "id", 3, -1);

    for (int i = 4; i <= reader.getRecordCount(); i++) {
      assertTrue(testIterator.hasNext());
      Document doc = testIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(testIterator.hasNext());
  }

  @Test
  public void testIteratorStartAndLimit() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    // Skip the first three documents. Only return up to two documents.
    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 3, 2);

    assertTrue(testIterator.hasNext());
    Document doc = testIterator.next();
    assertEquals("4", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("5", doc.getId());

    assertFalse(testIterator.hasNext());
  }

  // For the "paged" tests - this file splits the data into three row groups. So the iterator has to go back to
  // "readNewPages" after reading two records.
  @Test
  public void testPagedFile() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleWithRowsContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    assertEquals(3, reader.getRowGroups().size());

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, -1);

    for (int i = 1; i <= reader.getRecordCount(); i++) {
      assertTrue(testIterator.hasNext());
      Document doc = testIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(testIterator.hasNext());
  }

  @Test
  public void testPagedAndLimited() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleWithRowsContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, 2);

    assertTrue(testIterator.hasNext());
    Document doc = testIterator.next();
    assertEquals("1", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("2", doc.getId());

    assertFalse(testIterator.hasNext());

    // We throw an actual exception when the limit has been reached.
    assertThrows(NoSuchElementException.class,
        () -> testIterator.next());
  }

  @Test
  public void testPagedAndLimitedOnBound() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleWithRowsContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, 3);

    assertTrue(testIterator.hasNext());
    Document doc = testIterator.next();
    assertEquals("1", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("2", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("3", doc.getId());

    assertFalse(testIterator.hasNext());

    // We throw an actual exception when the limit has been reached.
    assertThrows(NoSuchElementException.class,
        () -> testIterator.next());
  }

  @Test
  public void testPagedAndStart() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleWithRowsContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 6, -1);
    assertFalse(testIterator.hasNext());

    // This skips the first two records.
    reader = ParquetFileReader.open(byteInputFile);
    testIterator = new ParquetFileIterator(reader, "id", 2, -1);

    for (int i = 3; i <= reader.getRecordCount(); i++) {
      assertTrue(testIterator.hasNext());
      Document doc = testIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(testIterator.hasNext());
  }

  @Test
  public void testPagedAndStartAndLimited() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleWithRowsContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 3, 2);

    assertTrue(testIterator.hasNext());
    Document doc = testIterator.next();
    assertEquals("4", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("5", doc.getId());

    assertFalse(testIterator.hasNext());
  }

  @Test
  public void testNonPrimitiveFields() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleNonPrimitiveContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);
    assertEquals(6, reader.getRecordCount());

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, -1);

    while (testIterator.hasNext()) {
      Document d = testIterator.next();

      // The String list is a non-primitive field. Want to make sure it's parsed correctly.
      assertTrue(d.has("hobbies"));
      assertEquals(2, d.getStringList("hobbies").size());
    }
  }

  @Test
  public void testNonPrimitiveFieldsAndRows() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleNonPrimitiveWithRowsContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);
    assertEquals(3, reader.getRowGroups().size());

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 0, -1);

    while (testIterator.hasNext()) {
      Document d = testIterator.next();
      assertTrue(d.has("hobbies"));
      assertEquals(2, d.getStringList("hobbies").size());
    }
  }

  @Test
  public void testMissingIDField() throws IOException {
    InputFile byteInputFile = new ByteArrayInputFile(exampleContents);
    ParquetFileReader reader = ParquetFileReader.open(byteInputFile);

    assertThrows(IllegalArgumentException.class, () -> new ParquetFileIterator(reader, "ABCDEFGHI", 0, -1));
  }
}
