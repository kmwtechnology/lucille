import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.parquet.connector.SeekableByteArrayInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SeekableByteArrayInputStreamTest {

  SeekableByteArrayInputStream testStream;
  int helloWorldLength = "Hello, world!".getBytes().length;

  @Before
  public void setup() throws FileNotFoundException {
    testStream = new SeekableByteArrayInputStream("Hello, world!".getBytes());
  }

  @After
  public void shutdown() throws IOException {
    testStream.close();
    testStream = null;
  }

  @Test
  public void testRead() throws IOException {
    List<Integer> bytes = new ArrayList<>();

    int byteRead;
    while ((byteRead = testStream.read()) != -1) {
      bytes.add(byteRead);
    }

    byte[] byteArray = listToArray(bytes);
    assertEquals("Hello, world!", new String(byteArray));
  }

  @Test
  public void testReadFully() throws IOException {
    // nothing wrong with passing in 0 capacity list...
    byte[] emptyList = new byte[0];
    testStream.readFully(emptyList);

    byte[] fullList = new byte[helloWorldLength];
    testStream.readFully(fullList);
    assertEquals("Hello, world!", new String(fullList));

    testStream.seek(0);

    byte[] halfList = new byte[6];
    testStream.readFully(halfList);
    assertEquals("Hello,", new String(halfList));

    testStream.seek(0);

    byte[] tooLong = new byte[100];
    assertThrows(IOException.class, () -> testStream.readFully(tooLong));
  }

  @Test
  public void testReadFullyOffset() throws IOException {
    byte[] fullList = new byte[helloWorldLength];
    testStream.readFully(fullList, 0, helloWorldLength);
    assertEquals("Hello, world!", new String(fullList));

    testStream.seek(0);

    fullList = new byte[6];
    testStream.readFully(fullList, 0, 6);
    assertEquals("Hello,", new String(fullList));

    testStream.seek(0);

    fullList = new byte[helloWorldLength + 1];
    testStream.readFully(fullList, 1, helloWorldLength);
    assertEquals("\u0000Hello, world!", new String(fullList));

    testStream.seek(0);

    fullList = new byte[helloWorldLength];
    testStream.readFully(fullList, 2, helloWorldLength - 2);
    assertEquals("\u0000\u0000Hello, worl", new String(fullList));
  }

  @Test
  public void testSeekAndGetPosition() throws IOException {
    assertEquals(0, testStream.getPos());
    assertEquals('H', (char) testStream.read());
    assertEquals(1, testStream.getPos());

    // seeking forwards
    testStream.seek(3);
    assertEquals(3, testStream.getPos());
    assertEquals('l', (char) testStream.read());
    assertEquals(4, testStream.getPos());
    assertEquals('o', (char) testStream.read());
    assertEquals(5, testStream.getPos());

    // seeking backwards
    testStream.seek(3);
    assertEquals(3, testStream.getPos());
    assertEquals('l', (char) testStream.read());
    assertEquals(4, testStream.getPos());
    assertEquals('o', (char) testStream.read());
    assertEquals(5, testStream.getPos());

    byte[] remainingList = new byte[8];
    testStream.readFully(remainingList);
    assertEquals(", world!", new String(remainingList));
    assertEquals(13, testStream.getPos());
  }

  @Test
  public void testReadByteBuffer() throws IOException {
    ByteBuffer fullBuffer = ByteBuffer.allocate(13);

    // Read method is *fully* reading and returns number read.
    assertEquals(13, testStream.read(fullBuffer));
    assertEquals("Hello, world!", new String(fullBuffer.array()));

    ByteBuffer partialBuffer = ByteBuffer.allocate(6);
    testStream.seek(0);
    testStream.readFully(partialBuffer);
    assertEquals("Hello,", new String(partialBuffer.array()));

    partialBuffer = ByteBuffer.allocate(6);
    testStream.seek(7);
    assertEquals(6, testStream.read(partialBuffer));
    assertEquals("world!", new String(partialBuffer.array()));
  }

  @Test
  public void testReadFullyByteBuffer() throws IOException {
    ByteBuffer fullBuffer = ByteBuffer.allocate(13);

    testStream.readFully(fullBuffer);
    assertEquals("Hello, world!", new String(fullBuffer.array()));

    ByteBuffer partialBuffer = ByteBuffer.allocate(6);

    testStream.seek(0);
    testStream.readFully(partialBuffer);
    assertEquals("Hello,", new String(partialBuffer.array()));

    partialBuffer = ByteBuffer.allocate(6);
    testStream.seek(7);
    assertEquals(6, testStream.read(partialBuffer));
    assertEquals("world!", new String(partialBuffer.array()));
  }

  private byte[] listToArray(List<Integer> byteList) {
    byte[] result = new byte[byteList.size()];

    for (int i = 0; i < byteList.size(); i++) {
      result[i] = byteList.get(i).byteValue();
    }

    return result;
  }
}
