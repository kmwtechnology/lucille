package com.kmwllc.lucille.core.filehandler;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

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
    if (i != -1) {
      // -1 is end of stream, not a byte to record
      sink.write(i);
      checkBufferSize();
    }
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
    if (!asciiCompatible()) {
      clearUpToText(string);
      return;
    }
    clearUpTo(sink.toByteArray(), string.getBytes(this.encoding));
  }

  // The text path, for encodings where the tag's bytes are not the same as its ASCII bytes (UTF-16 and UTF-32). It decodes
  // the whole buffer, so it can still corrupt text when a buffer ends partway through a character.
  private void clearUpToText(String string) throws IOException {
    String tempStr = sink.toString(this.encoding);
    sink.reset();
    int start = tempStr.indexOf(string);
    if (start != -1) {
      sink.write(tempStr.substring(start).getBytes(this.encoding));
    }
  }

  /**
   * Whether the tags' ASCII bytes are their bytes in the encoding, as they are in UTF-8. False for UTF-16 and UTF-32, where
   * the text must be decoded to be searched. Other ASCII-compatible encodings have not been checked.
   */
  private boolean asciiCompatible() throws IOException {
    return Arrays.equals("</>".getBytes(this.encoding), new byte[] {'<', '/', '>'});
  }

  // Works on the raw bytes, never on decoded text. Decoding a buffer that ends partway through a multi-byte character
  // replaces that character with U+FFFD, and writing the result back corrupts the recorded XML. The tags are ASCII, and
  // in an ASCII-compatible encoding (such as UTF-8) their bytes never occur inside a multi-byte character.
  private int clearUpTo(byte[] recorded, byte[] tag) {
    sink.reset();
    int start = indexOf(recorded, tag);
    if (start != -1) {
      sink.write(recorded, start, recorded.length - start);
    }
    return start;
  }

  /**
   * Returns the recorded XML up to and including the first occurrence of the given string, and discards what comes before
   * it. The returned text is decoded from exactly the bytes that were recorded, so it is correct even when the record's
   * bytes were read in pieces.
   *
   * @throws IOException if the string is not in the recorded bytes.
   */
  public String returnUpTo(String string) throws IOException {
    if (!asciiCompatible()) {
      String tempStr = sink.toString(this.encoding);
      int start = tempStr.indexOf(string);
      if (start == -1) {
        throw new IOException("Did not find " + string + " in the recorded XML.");
      }
      clearUpToText(string);
      return tempStr.substring(0, start + string.length());
    }
    byte[] recorded = sink.toByteArray();
    byte[] tag = string.getBytes(this.encoding);
    int start = clearUpTo(recorded, tag);
    if (start == -1) {
      throw new IOException("Did not find " + string + " in the recorded XML.");
    }
    int offset = start + tag.length;
    return new String(recorded, 0, offset, this.encoding);
  }

  private static int indexOf(byte[] haystack, byte[] needle) {
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      int j = 0;
      while (j < needle.length && haystack[i + j] == needle[j]) {
        j++;
      }
      if (j == needle.length) {
        return i;
      }
    }
    return -1;
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
