package com.kmwllc.lucille.stage.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A general stream gobbler, useful when starting processes and handling their streams
 */
public class StreamGobbler extends Thread {

  private static final Logger log = LoggerFactory.getLogger(StreamGobbler.class);

  private final InputStream processOut;

  private volatile boolean startMessageSeen = false;

  /**
   * When we do not need to redirect the stream to another stream. E.g.
   * when we create a process, and are already redirecting std:out and std:err
   * 
   * @param name
   * @param processOut
   */
  public StreamGobbler(String name, InputStream processOut) {
    super(name);
    this.processOut = processOut;
  }

  public boolean isStartMessageSeen() {
    return startMessageSeen;
  }

  @Override
  public void run() {
    try (InputStreamReader isr = new InputStreamReader(processOut);
         BufferedReader br = new BufferedReader(isr)) {
      String line;
      while ((line = br.readLine()) != null) {
        log.info("{} -> {}", getName(), line);
        if (line.indexOf("Callback server port:") != -1) {
          startMessageSeen = true;
        }
      }
    } catch (IOException ioe) {
      log.info("{} gobbler leaving", getName());
    }
  }
}
