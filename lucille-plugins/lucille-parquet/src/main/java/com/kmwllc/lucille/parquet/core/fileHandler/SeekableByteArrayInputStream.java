package com.kmwllc.lucille.parquet.core.fileHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;

public class SeekableByteArrayInputStream extends SeekableInputStream {
  private final ByteArrayInputStream stream;
  private final byte[] source;
  private long position;

  public SeekableByteArrayInputStream(byte[] source) {
    this.source = source;
    this.stream = new ByteArrayInputStream(source);
    this.position = 0;
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void seek(long target) throws IOException {
    if (target < 0 || target >= source.length) {
      throw new IOException("Invalid position to seek to.");
    }

    stream.reset();
    stream.skip(target);
    position = target;
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    int numRead = 0;
    int numToRead = bytes.length;

    while (numRead < numToRead) {
      int bytesRemaining = numToRead - numRead;
      int bytesRead = read(bytes, numRead, bytesRemaining);

      if (bytesRead == -1) {
        throw new IOException("Unexpected end of stream while reading " + numToRead + " bytes.");
      }

      numRead += bytesRead;
    }
  }

  @Override
  public void readFully(byte[] bytes, int offset, int length) {
    int numRead = stream.read(bytes, offset, length);

    if (numRead != -1) {
      position += numRead;
    }
  }

  @Override
  public int read() {
    int byteRead = stream.read();

    if (byteRead != -1) {
      position++;
    }

    return byteRead;
  }

  @Override
  public int read(ByteBuffer byteBuffer) {
    int maxToRead = byteBuffer.remaining();

    if (maxToRead == 0) {
      return 0;
    }

    byte[] temp = new byte[maxToRead];

    int numRead = stream.read(temp, 0, maxToRead);

    if (numRead > 0) {
      byteBuffer.put(temp, 0, numRead);
      position += numRead;
    }

    return numRead;
  }

  @Override
  public void readFully(ByteBuffer byteBuffer) throws IOException {
    int maxToRead = byteBuffer.remaining();
    if (maxToRead == 0) {
      return;
    }

    byte[] temp = new byte[maxToRead];
    int numReadTotal = 0;

    while (numReadTotal < maxToRead) {
      int currentBytesRead = stream.read(temp, numReadTotal, maxToRead - numReadTotal);

      if (currentBytesRead == -1) {
        throw new IOException("EOF reached while reading a buffer.");
      }

      numReadTotal += currentBytesRead;
    }

    byteBuffer.put(temp);
    position += numReadTotal;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
