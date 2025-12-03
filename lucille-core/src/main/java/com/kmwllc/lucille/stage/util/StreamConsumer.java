package com.kmwllc.lucille.stage.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes a given InputStream line by line, setting a flag once a designated message is found within a line.
 */
public class StreamConsumer extends Thread {

  private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

  private final InputStream inputStream;

  private final String message;

  private volatile boolean messageSeen = false;

  public StreamConsumer(InputStream inputStream, String message) {
    this.inputStream = inputStream;
    this.message = message;
  }

  public boolean isMessageSeen() {
    return messageSeen;
  }

  @Override
  public void run() {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        log.debug("{} -> {}", getName(), line);
        if ((!messageSeen) && (line.indexOf(message) != -1)) {
          messageSeen = true;
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
