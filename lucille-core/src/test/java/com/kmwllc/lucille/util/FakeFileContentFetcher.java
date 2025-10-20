package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.io.IOUtils;

public class FakeFileContentFetcher implements FileContentFetcher {

  public FakeFileContentFetcher(Config config) {

  }

  @Override
  public void startup() throws IOException {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public InputStream getInputStream(String path) throws IOException {
    return IOUtils.toInputStream("Test.", "UTF-8");
  }

  @Override
  public InputStream getInputStream(String path, Document doc) throws IOException {
    return IOUtils.toInputStream("Test.", "UTF-8");
  }

  @Override
  public BufferedReader getReader(String path) throws IOException {
    return new BufferedReader(new InputStreamReader(IOUtils.toInputStream("Test.", "UTF-8")));
  }

  @Override
  public BufferedReader getReader(String path, Document doc) throws IOException {
    return new BufferedReader(new InputStreamReader(IOUtils.toInputStream("Test.", "UTF-8")));
  }

  @Override
  public BufferedReader getReader(String path, String encoding) throws IOException {
    return new BufferedReader(new InputStreamReader(IOUtils.toInputStream("Test.", encoding)));
  }

  @Override
  public BufferedReader getReader(String path, String encoding, Document doc) throws IOException {
    return new BufferedReader(new InputStreamReader(IOUtils.toInputStream("Test.", encoding)));
  }

  @Override
  public int countLines(String path) throws IOException {
    return 0;
  }

  @Override
  public int countLines(String path, Document doc) throws IOException {
    return 0;
  }
}
