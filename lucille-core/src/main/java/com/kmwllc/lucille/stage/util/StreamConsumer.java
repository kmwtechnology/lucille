package com.kmwllc.lucille.stage.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes a given InputStream line by line, setting a flag once a designated message is found within a line.
 * The most recent lines are kept and can be retrieved via getCapturedOutput(). Older lines are dropped so a long-running
 * process cannot grow this buffer without bound.
 */
public class StreamConsumer extends Thread {

  private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

  private static final int MAX_CAPTURED_LINES = 500;

  private final InputStream inputStream;

  private final String message;

  private volatile boolean messageSeen = false;

  private final Deque<String> capturedOutput = new ArrayDeque<>();

  public StreamConsumer(InputStream inputStream, String message) {
    this.inputStream = inputStream;
    this.message = message;
  }

  public boolean isMessageSeen() {
    return messageSeen;
  }

  public List<String> getCapturedOutput() {
    synchronized (capturedOutput) {
      return new ArrayList<>(capturedOutput);
    }
  }

  @Override
  public void run() {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        synchronized (capturedOutput) {
          capturedOutput.addLast(line);
          if (capturedOutput.size() > MAX_CAPTURED_LINES) {
            capturedOutput.removeFirst();
          }
        }
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
