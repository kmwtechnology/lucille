package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.util.BoundedLineReader.RecordTooLargeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import org.junit.Test;

public class BoundedLineReaderTest {

  @Test
  public void testLineTerminatorsMatchBufferedReader() throws IOException {
    String input = "a\nb\r\nc\rd\n\n\r\ne\r";

    try (BufferedReader expected = new BufferedReader(new StringReader(input));
        BoundedLineReader actual = new BoundedLineReader(new StringReader(input), 100)) {
      String line;
      do {
        line = expected.readLine();
        assertEquals(line, actual.readLine());
      } while (line != null);
    }
  }

  @Test
  public void testLastLineWithoutTerminator() throws IOException {
    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("abc\ndef"), 10)) {
      assertEquals("abc", reader.readLine());
      reader.startRecord();
      assertEquals("def", reader.readLine());
      assertNull(reader.readLine());
    }
  }

  @Test
  public void testRecordAtLimitIsAllowed() throws IOException {
    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("12345\n"), 5)) {
      assertEquals("12345", reader.readLine());
      assertNull(reader.readLine());
    }
  }

  @Test
  public void testRecordOverLimitThrows() throws IOException {
    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("ok\n123456\n"), 5)) {
      assertEquals("ok", reader.readLine());
      reader.startRecord();
      RecordTooLargeException e = assertThrows(RecordTooLargeException.class, reader::readLine);
      assertTrue(e.getMessage().contains("5 bytes"));
    }
  }

  @Test
  public void testCountsUtf8Bytes() throws IOException {
    // "é" is 2 bytes, "€" is 3, and "😀" (a surrogate pair) is 4
    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("é€😀\n"), 9)) {
      assertEquals("é€😀", reader.readLine());
    }
    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("é€😀\n"), 8)) {
      assertThrows(RecordTooLargeException.class, reader::readLine);
    }
  }

  @Test
  public void testRecordSpansLinesUntilStartRecord() throws IOException {
    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("123\n456\n789\n"), 6)) {
      // without startRecord, the lines count toward the same record
      assertEquals("123", reader.readLine());
      assertEquals("456", reader.readLine());
      assertThrows(RecordTooLargeException.class, reader::readLine);
    }

    try (BoundedLineReader reader = new BoundedLineReader(new StringReader("123\n456\n789\n"), 6)) {
      assertEquals("123", reader.readLine());
      assertEquals("456", reader.readLine());
      reader.startRecord();
      assertEquals("789", reader.readLine());
    }
  }
}
