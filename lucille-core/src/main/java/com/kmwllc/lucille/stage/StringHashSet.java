package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringHashSet implements LookupCollection<String> {

  private final static Logger log = LoggerFactory.getLogger(StringHashSet.class);
  private final Set<String> set;

  public StringHashSet(String file, boolean ignoreCase) throws StageException {

    // count lines and create a set with correct capacity
    int lineCount = countLines(file);
    set = new HashSet<>((int) Math.ceil(lineCount / 0.75) + 1);

    // read file into set
    try (BufferedReader reader = new BufferedReader(getFileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        set.add(ignoreCase ? line.toLowerCase() : line);
      }

      log.info("Loaded {} values from {}", set.size(), file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean add(String value) {
    return set.add(value);
  }

  @Override
  public boolean contains(String value) {
    return set.contains(value);
  }

  @Override
  public boolean remove(String value) {
    return set.remove(value);
  }

  private static int countLines(String filename) throws StageException {
    try (BufferedReader reader = new BufferedReader(getFileReader(filename))) {
      int lines = 0;
      while (reader.readLine() != null) {
        lines++;
      }
      return lines;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Reader getFileReader(String path) throws StageException {
    try {
      return FileUtils.getReader(path);
    } catch (NullPointerException | IOException e) {
      throw new StageException("File does not exist: " + path);
    }
  }
}
