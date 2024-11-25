package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.IOException;
import java.io.Reader;
import java.util.function.UnaryOperator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONConnector extends AbstractConnector implements FileHandler {
  private static final Logger log = LoggerFactory.getLogger(JSONConnector.class);
  private final String path;

  private final UnaryOperator<String> idUpdater;

  public JSONConnector(Config config) {
    super(config);
    this.path = config.getString("jsonPath");
    this.idUpdater = (id) -> createDocId(id);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {

    try (Reader reader = FileUtils.getReader(path)) {
      LineIterator it = IOUtils.lineIterator(reader);
      while (it.hasNext()) {
        String line = it.nextLine();
        publisher.publish(Document.createFromJson(line, idUpdater));
      }
    } catch (IOException e) {
      throw new ConnectorException("Error reading file: ", e);
    } catch (Exception e) {
      throw new ConnectorException("Error creating or publishing document", e);
    }
  }

  @Override
  public Iterator<Document> processFile(Path path) throws Exception{
    // reader will be closed when the LineIterator is closed in getDocumentIterator
    Reader reader = FileUtils.getReader(String.valueOf(path));
    return getDocumentIterator(reader);
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent) throws Exception {
    // reader will be closed when the LineIterator is closed in getDocumentIterator
    Reader reader = new InputStreamReader(new ByteArrayInputStream(fileContent), StandardCharsets.UTF_8);
    return getDocumentIterator(reader);
  }

  private Iterator<Document> getDocumentIterator(Reader reader) {
    return new Iterator<Document>() {
      private final LineIterator it = IOUtils.lineIterator(reader);

      @Override
      public boolean hasNext() {
        boolean hasNext = it.hasNext();
        if (!hasNext) {
          IOUtils.closeQuietly(it);
        }
        return hasNext;
      }

      @Override
      public Document next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more lines to process");
        }

        String line = it.nextLine();
        try {
          return Document.createFromJson(line, idUpdater);
        } catch (Exception e) {
          throw new RuntimeException("Error creating document, make sure that you have 'id' key within each line of json", e);
        }
      }
    };
  }
}
