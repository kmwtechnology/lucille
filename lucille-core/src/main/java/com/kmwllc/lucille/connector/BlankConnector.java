package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.typesafe.config.Config;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Connector implementation that produces documents from the rows in a given CSV file.
 */
public class BlankConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(BlankConnector.class);
  private final int docNum;
  private final int docStartNum;

  public BlankConnector(Config config) {
    super(config);
    this.docNum = config.getInt("docNum");
    this.docStartNum = config.hasPath("docStartNum") ? config.getInt("docStartNum") : 0;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    for (int i = 0; i < docNum; i++) {
      Document doc = Document.create(createDocId(Integer.toString(i + docStartNum)));
      try {
        publisher.publish(doc);
      } catch (Exception e) {
        throw new ConnectorException("Error creating or publishing document", e);
      }
    }
  }
}
