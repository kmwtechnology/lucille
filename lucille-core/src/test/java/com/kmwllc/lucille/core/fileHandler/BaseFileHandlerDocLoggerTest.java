package com.kmwllc.lucille.core.fileHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the per-document DocLogger line in BaseFileHandler only touches the MDC when DocLogger is at INFO, and
 * that when it is enabled the line still carries the document id in its context.
 */
public class BaseFileHandlerDocLoggerTest {

  private static final String DOC_LOGGER = "com.kmwllc.lucille.core.DocLogger";
  private static final String FILE_PATH = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";

  private Logger logger;
  private Level originalLevel;
  private CapturingAppender appender;

  @Before
  public void setUp() {
    logger = (Logger) LogManager.getLogger(DOC_LOGGER);
    originalLevel = logger.getLevel();
    appender = new CapturingAppender();
    appender.start();
    logger.addAppender(appender);
  }

  @After
  public void tearDown() {
    logger.removeAppender(appender);
    appender.stop();
    logger.setLevel(originalLevel);
  }

  @Test
  public void testNoDocLoggerOutputWhenDisabled() throws Exception {
    logger.setLevel(Level.ERROR);
    publishFile();
    assertTrue(appender.docIds.isEmpty());
    assertTrue(appender.messages.isEmpty());
  }

  @Test
  public void testMdcCarriesIdWhenDocLoggerEnabled() throws Exception {
    logger.setLevel(Level.INFO);
    publishFile();
    assertEquals(List.of("1", "2", "3"), appender.docIds);
  }

  private void publishFile() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of()));
    FileHandler handler = FileHandler.create("json", config);
    try (FileInputStream in = new FileInputStream(new File(FILE_PATH))) {
      handler.processFileAndPublish(mock(Publisher.class), in, FILE_PATH);
    }
  }

  private static class CapturingAppender extends AbstractAppender {
    final List<String> messages = new ArrayList<>();
    final List<String> docIds = new ArrayList<>();

    CapturingAppender() {
      super("CapturingAppender", null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
      docIds.add(event.getContextData().getValue("id"));
    }
  }
}
