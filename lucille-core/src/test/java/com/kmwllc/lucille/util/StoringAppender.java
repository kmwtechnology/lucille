package com.kmwllc.lucille.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Log4J appender that can be used in a testing context to store a history of log events for inspection.
 *
 * Note that this is one of the few places where we have an explicit log4j dependency in the codebase
 * and this class should be used sparingly.
 */
public class StoringAppender extends AbstractAppender {

  private final List<String> messages = new ArrayList<>();

  public StoringAppender() {
    super("StoringAppender", null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
  }

  @Override
  public void append(LogEvent event) {
    messages.add(event.getMessage().getFormattedMessage());
  }

  public List<String> getMessages() {
    return new ArrayList<>(messages);
  }

  public void clear() {
    messages.clear();
  }
}
