package com.kmwllc.lucille.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.Test;

public class ParquetFileIteratorTest {
  Path examplePath = new Path("src/test/resources/ParquetFileIteratorTest/example.parquet");
  Path exampleWithRowsPath = new Path("src/test/resources/ParquetFileIteratorTest/example_with_rows.parquet");

   /*
   The Parquet file is this:
   {
     'name': ['Oliver', 'Mia', 'Jasper', 'Mr. Meow', 'Elijah', 'Spot'],
     'age': [20, 35, 46, 8, 7, 3],
     'net_worth': [10.0, 12.5, 7.5, 0.0, 15.3, -2.0],
     'species': ['Human', 'Human', 'Human', 'Cat', 'Human', 'Dog'],
     'id': ['1', '2', '3', '4', '5', '6']
     'hobbies': [['Reading', 'Running'], ['Cooking', 'Painting'], ['Gaming', 'Reading'],
                  ['Sleeping', 'Chasing mice'], ['Drawing', 'Coding'], ['Playing', 'Sleeping']]
   }
   The "with_rows" file has the same data in three pages (row groups), each with two rows to read. (1 and 2, 3 and 4, 5 and 6...)
    */

  @Test
  public void testIterator() throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(examplePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
    InputFile inputFile = HadoopInputFile.fromPath(examplePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
    InputFile inputFile = HadoopInputFile.fromPath(examplePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

    // There are 6 records and start is set to "6". So this document gets skipped
    // entirely...
    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 6, -1);
    assertFalse(testIterator.hasNext());

    // This skips the first three records.
    reader = ParquetFileReader.open(inputFile);
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
    InputFile inputFile = HadoopInputFile.fromPath(examplePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
    InputFile inputFile = HadoopInputFile.fromPath(exampleWithRowsPath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
    InputFile inputFile = HadoopInputFile.fromPath(exampleWithRowsPath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
    InputFile inputFile = HadoopInputFile.fromPath(exampleWithRowsPath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
    InputFile inputFile = HadoopInputFile.fromPath(exampleWithRowsPath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

    ParquetFileIterator testIterator = new ParquetFileIterator(reader, "id", 6, -1);
    assertFalse(testIterator.hasNext());

    // This skips the first two records.
    reader = ParquetFileReader.open(inputFile);
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
    InputFile inputFile = HadoopInputFile.fromPath(exampleWithRowsPath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

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
  public void testMissingIDField() throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(examplePath, new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(inputFile);

    assertThrows(IllegalArgumentException.class, () -> new ParquetFileIterator(reader, "ABCDEFGHI", 0, -1));
  }
}