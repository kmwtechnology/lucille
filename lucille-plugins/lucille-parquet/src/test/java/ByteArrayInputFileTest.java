import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.parquet.ByteArrayInputFile;
import java.io.IOException;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.Test;

public class ByteArrayInputFileTest {

  byte[] helloWorld = "Hello, world!".getBytes();

  @Test
  public void testGetLength() {
    ByteArrayInputFile inputFile = new ByteArrayInputFile(helloWorld);

    assertEquals(13, inputFile.getLength());
  }

  @Test
  public void testNewStream() throws IOException {
    ByteArrayInputFile inputFile = new ByteArrayInputFile(helloWorld);
    SeekableInputStream inputStream = inputFile.newStream();

    byte[] result = new byte[13];

    for (int i = 0; i < 13; i++) {
      result[i] = (byte) inputStream.read();
    }

    assertEquals("Hello, world!", new String(result));
  }
}
