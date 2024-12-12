package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFileHandler implements FileHandler {
  private static final Logger log = LoggerFactory.getLogger(JsonFileHandler.class);

  private final String docIdPrefix;
  private final UnaryOperator<String> idUpdater;

  public JsonFileHandler(Config config) {
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
    this.idUpdater = (id) -> docIdPrefix + id;
  }


  @Override
  public Iterator<Document> processFile(Path path) throws Exception {
    // reader will be closed when the LineIterator is closed in getDocumentIterator
    // only works for path from local file system
    Reader reader;
    try {
      reader = FileUtils.getReader(path.toString());
    } catch (Exception e) {
      throw new ConnectorException("Error creating reader from path: " + path, e);
    }

    return getDocumentIterator(reader);
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws Exception {
    // reader will be closed when the LineIterator is closed in getDocumentIterator
    Reader reader;
    try {
      reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(fileContent), StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new ConnectorException("Error creating reader from file: " + pathStr, e);
    }

    return getDocumentIterator(reader);
  }

  private Iterator<Document> getDocumentIterator(Reader reader) {
    return new Iterator<Document>() {
      // closing LineIterator closes reader, which closes BufferedReader, InputStreamReader and the InputStream passed into InputStreamReader
      private final LineIterator it = IOUtils.lineIterator(reader);

      @Override
      public boolean hasNext() {
        boolean hasNext = it.hasNext();
        // Iterator closes when reader is done reading (successful job)
        if (!hasNext) {
          IOUtils.closeQuietly(it);
        }
        return hasNext;
      }

      @Override
      public Document next() {
        // additional safety check to ensure that the iterator has more lines to process, if hasNext returns false,
        // means we have also closed LineIterator, throw an exception
        if (!hasNext()) {
          throw new NoSuchElementException("No more lines to process");
        }

        String line = it.next();
        try {
          return Document.createFromJson(line, idUpdater);
        } catch (Exception e) {
          // any errors that occur during the process of creating a document, we close the LineIterator
          // cannot close iterator in finally, as we will called next() again if there are more elements.
          IOUtils.closeQuietly(it);
          throw new RuntimeException("Error creating document, make sure that you have 'id' key within each line of json", e);
        }
      }
    };
  }
}
