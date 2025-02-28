package com.kmwllc.lucille.parquet.connector;

import java.io.IOException;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ByteArrayInputFile implements InputFile {

  private final byte[] source;

  public ByteArrayInputFile(byte[] source) {
//    this.source = ArrayUtils.addAll(ParquetFileWriter.MAGIC, source);

    this.source = source;
    System.out.print("[");
    for (byte b : this.source) {
      System.out.print(b);
    }
    System.out.println("]");

    System.out.print("[");
    for (byte b : ParquetFileWriter.MAGIC) {
      System.out.print(b);
    }
    System.out.println("]");
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
