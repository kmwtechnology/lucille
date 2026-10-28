package com.kmwllc.lucille.core.filehandler;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

// extended from example at:
// https://bytes.com/topic/net/answers/818268-java-sax-parser-how-get-raw-xml-code-currently-parsingevent
class RecordingInputStream extends FilterInputStream {

  protected ByteArrayOutputStream sink;
  private String encoding;
  // 0 means no limit
  private final int maxBufferSize;

  public RecordingInputStream(InputStream in) {
    this(in, new ByteArrayOutputStream(), 0);
  }

  /**
   * @param in The InputStream to read from.
   * @param maxBufferSize The maximum number of bytes that may be recorded before an IOException is thrown. Recorded bytes
   *                      are discarded by {@link #clearUpTo(String)} and {@link #returnUpTo(String)}.
   */
  public RecordingInputStream(InputStream in, int maxBufferSize) {
    this(in, new ByteArrayOutputStream(), maxBufferSize);
  }

  RecordingInputStream(InputStream in, ByteArrayOutputStream sink) {
    this(in, sink, 0);
  }

  private RecordingInputStream(InputStream in, ByteArrayOutputStream sink, int maxBufferSize) {
    super(in);

    if (maxBufferSize < 0) {
      throw new IllegalArgumentException("maxBufferSize cannot be negative, got " + maxBufferSize);
    }

    this.sink = sink;
    this.maxBufferSize = maxBufferSize;
  }

  @Override
  public synchronized int read() throws IOException {
    int i = in.read();
    sink.write(i);
    checkBufferSize();
    return i;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    int l = in.read(buf, off, len);
    if (l == -1) {
      return -1;
    }
    sink.write(buf, off, l);
    checkBufferSize();
    return l;
  }

  private void checkBufferSize() throws IOException {
    if (maxBufferSize > 0 && sink.size() > maxBufferSize) {
      throw new BufferLimitExceededException(maxBufferSize);
    }
  }

  @Override
  public synchronized int read(byte[] buf) throws IOException {
    return read(buf, 0, buf.length);
  }

  @Override
  public synchronized long skip(long len) throws IOException {
    long l = 0;
    int i = 0;
    byte[] buf = new byte[1024];
    while (l < len) {
      i = read(buf, 0, (int) Math.min((long) buf.length, len - l));
      if (i == -1) {
        break;
      }
      l += i;
    }
    return l;
  }

  public void clearUpTo(String string) throws IOException {
    String tempStr = sink.toString(this.encoding);
    clearUpTo(string, tempStr);
  }

  private int clearUpTo(String string, String tempStr) throws IOException {
    sink.reset();
    // TODO: avoid the byte to string conversions or make this more efficient.
    int start = tempStr.indexOf(string);
    if (start != -1) {
      sink.write(tempStr.substring(start).getBytes(this.encoding));
    }
    return start;
  }

  public String returnUpTo(String string) throws IOException {
    String tempStr = sink.toString(this.encoding);
    int start = clearUpTo(string, tempStr);
    int offset = start + string.length();
    return tempStr.substring(0, offset);
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * Thrown when more bytes have been recorded than the maximum buffer size allows.
   */
  public static class BufferLimitExceededException extends IOException {

    public BufferLimitExceededException(int maxBufferSize) {
      super("Recorded XML exceeds the maximum size of " + maxBufferSize + " bytes.");
    }
  }
}
