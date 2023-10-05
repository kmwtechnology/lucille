package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetLookup extends Stage {

  private final static Logger log = LoggerFactory.getLogger(SetLookup.class);
  private final String file;
  private final String source;
  private final String destination;
  private final boolean ignoreMissingSource;
  private final boolean ignoreCase;

  private Set<String> values;

  public SetLookup(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("file_path", "source")
        .withOptionalProperties("destination", "ignore_missing_source", "ignore_case")
    );

    // todo is it possible to abstract set type, have string by default and converters otherwise?

    // required
    file = config.getString("file_path");
    source = config.getString("source");

    // optional
    destination = ConfigUtils.getOrDefault(config, "destination", "setContains");
    ignoreMissingSource = ConfigUtils.getOrDefault(config, "ignore_missing_source", false);

    // todo not sure if this is necessary. DictionaryLookup has similar logic
    ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
  }

  @Override
  public void start() throws StageException {

    // count lines and create a set with correct capacity
    int lineCount = countLines(file);
    values = new HashSet<>((int) Math.ceil(lineCount / 0.75) + 1);

    // read file into set
    try (BufferedReader reader = new BufferedReader(getFileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        values.add(ignoreCase ? line.toLowerCase() : line);
      }

      log.info("Loaded {} values from {}", values.size(), file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {


    if (!doc.has(source)) {
      doc.setField(destination, ignoreMissingSource);
      return null;
    }

    String value = doc.getString(source);
    if (ignoreCase) {
      value = value.toLowerCase();
    }

    doc.setField(destination, values.contains(value));

    return null;
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

  // todo what would be a good place to put this method to be used across Lucille?
  private static Reader getFileReader(String path) throws StageException {
    try {
      return FileUtils.getReader(path);
    } catch (NullPointerException | IOException e) {
      throw new StageException("File does not exist: " + path);
    }
  }
}
