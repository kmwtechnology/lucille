package com.kmwllc.lucille.parquet.core.fileHandler;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ByteArrayInputFile implements InputFile {

  private final byte[] source;

  public ByteArrayInputFile(byte[] source) {
    this.source = source;
  }

  @Override
  public long getLength() {
    return source.length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new SeekableByteArrayInputStream(source);
  }
}
