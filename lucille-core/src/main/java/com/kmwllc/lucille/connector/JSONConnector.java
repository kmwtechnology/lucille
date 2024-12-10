package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.IOException;
import java.io.Reader;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONConnector extends AbstractConnector {
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
        String line = it.next();
        publisher.publish(Document.createFromJson(line, idUpdater));
      }
    } catch (IOException e) {
      throw new ConnectorException("Error reading file: ", e);
    } catch (Exception e) {
      throw new ConnectorException("Error creating or publishing document", e);
    }
  }
}
