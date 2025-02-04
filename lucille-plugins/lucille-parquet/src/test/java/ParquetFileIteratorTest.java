import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.parquet.connector.ParquetFileIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.Test;

public class ParquetFileIteratorTest {

  Path exampleFilePath = new Path("src/test/resources/ParquetFileIteratorTest/example.parquet");
  Path exampleFilePathWithRows = new Path("src/test/resources/ParquetFileIteratorTest/example_with_rows.parquet");
  /*
  The Parquet file is this:

  {
    'name': ['Oliver', 'Mia', 'Jasper', 'Mr. Meow', 'Elijah', 'Spot'],
    'age': [20, 35, 46, 8, 7, 3],
    'net_worth': [10.0, 12.5, 7.5, 0.0, 15.3, -2.0],
    'species': ['Human', 'Human', 'Human', 'Cat', 'Human', 'Dog'],
    'id': ['1', '2', '3', '4', '5', '6']
  }

  The "with_rows" file has three pages, each with two rows to read. (1 and 2, 3 and 4, 5 and 6...)
   */

  @Test
  public void testIterator() throws IOException {
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

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
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

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
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

    // There are 6 records and start is set to "6". So this document gets skipped
    // entirely...
    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 6, -1);
    assertFalse(testIterator.hasNext());

    // This skips the first two records.
    reader = ParquetFileReader.open(hadoopFile);
    testIterator = new ParquetFileIterator(reader, "id", 2, -1);

    for (int i = 3; i <= reader.getRecordCount(); i++) {
      assertTrue(testIterator.hasNext());
      Document doc = testIterator.next();
      assertEquals("" + i, doc.getId());
    }

    assertFalse(testIterator.hasNext());
  }

  @Test
  public void testIteratorStartAndLimit() throws IOException {
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

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
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePathWithRows, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

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
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePathWithRows, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

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
  public void testPagedAndStart() throws IOException {
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePathWithRows, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 6, -1);
    assertFalse(testIterator.hasNext());

    // This skips the first two records.
    reader = ParquetFileReader.open(hadoopFile);
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
    HadoopInputFile hadoopFile = HadoopInputFile.fromPath(exampleFilePathWithRows, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 3, 2);

    assertTrue(testIterator.hasNext());
    Document doc = testIterator.next();
    assertEquals("4", doc.getId());

    assertTrue(testIterator.hasNext());
    doc = testIterator.next();
    assertEquals("5", doc.getId());

    assertFalse(testIterator.hasNext());
  }
}
