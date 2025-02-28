package com.kmwllc.lucille.parquet.connector;

import java.io.IOException;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ByteArrayInputFile implements InputFile {

  private final byte[] source;

  public ByteArrayInputFile(byte[] source) {
    this.source = source;
  }

  @Override
  public long getLength() throws IOException {
    return source.length;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new SeekableByteArrayInputStream(source);
  }
}
