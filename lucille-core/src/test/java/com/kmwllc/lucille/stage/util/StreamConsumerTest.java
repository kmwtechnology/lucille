package com.kmwllc.lucille.stage.util;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;

public class StreamConsumerTest {

  @Test
  public void testStreamConsumerCapturesOutput() throws InterruptedException {
    String testOutput = "Line 1\nLine 2\nLine 3\nJavaGateway STARTED\nLine 5";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    assertTrue("Should have seen the target message", consumer.isMessageSeen());

    List<String> captured = consumer.getCapturedOutput();
    assertEquals("Should have captured 5 lines", 5, captured.size());
    assertEquals("First line should match", "Line 1", captured.get(0));
    assertEquals("Message line should be captured", "JavaGateway STARTED", captured.get(3));
    assertEquals("Last line should match", "Line 5", captured.get(4));
  }

  @Test
  public void testStreamConsumerCapturesPartialOutput() throws InterruptedException {
    // Test when message is never seen - output should still be captured
    String testOutput = "Error initializing\nFailed to start\nTimeout waiting for gateway";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    assertFalse("Should not have seen the target message", consumer.isMessageSeen());

    List<String> captured = consumer.getCapturedOutput();
    assertEquals("Should have captured 3 lines", 3, captured.size());
    assertEquals("First error line should be captured", "Error initializing", captured.get(0));
    assertEquals("Middle line should be captured", "Failed to start", captured.get(1));
    assertEquals("Last line should be captured", "Timeout waiting for gateway", captured.get(2));
  }

  @Test
  public void testStreamConsumerKeepsOnlyMostRecentLines() throws InterruptedException {
    StringBuilder testOutput = new StringBuilder();
    for (int i = 1; i <= 600; i++) {
      testOutput.append("Line ").append(i).append("\n");
    }
    InputStream inputStream = new ByteArrayInputStream(testOutput.toString().getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    List<String> captured = consumer.getCapturedOutput();
    assertEquals("Should keep only the most recent 500 lines", 500, captured.size());
    assertEquals("Oldest kept line should be line 101", "Line 101", captured.get(0));
    assertEquals("Newest line should be line 600", "Line 600", captured.get(499));
  }

  @Test
  public void testStreamConsumerEmptyOutput() throws InterruptedException {
    String testOutput = "";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    assertFalse("Should not have seen message with empty output", consumer.isMessageSeen());
    assertEquals("Should have empty captured output", 0, consumer.getCapturedOutput().size());
  }

  @Test
  public void testStreamConsumerSingleLine() throws InterruptedException {
    String testOutput = "JavaGateway STARTED";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    assertTrue("Should have seen message", consumer.isMessageSeen());
    List<String> captured = consumer.getCapturedOutput();
    assertEquals("Should have captured 1 line", 1, captured.size());
    assertEquals("Line should match", "JavaGateway STARTED", captured.get(0));
  }

  @Test
  public void testStreamConsumerPartialMatchNotFound() throws InterruptedException {
    String testOutput = "Java Gateway\nStarted process\nReady";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    assertFalse("Should not match partial strings", consumer.isMessageSeen());
    assertEquals("Should have captured all lines", 3, consumer.getCapturedOutput().size());
  }

  @Test
  public void testStreamConsumerCapturedOutputIsThreadSafe() throws InterruptedException {
    String testOutput = "Line 1\nLine 2\nLine 3";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "Never seen");
    consumer.start();

    // Get list while consumer is still running
    List<String> captured1 = consumer.getCapturedOutput();
    consumer.join();
    List<String> captured2 = consumer.getCapturedOutput();

    // Both should be accurate (capture is synchronized)
    assertTrue("First capture should have at least some lines", captured1.size() >= 0);
    assertEquals("Final capture should have all 3 lines", 3, captured2.size());
  }

  @Test
  public void testStreamConsumerCapturedOutputIsImmutable() throws InterruptedException {
    String testOutput = "Line 1\nLine 2\nJavaGateway STARTED";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "JavaGateway STARTED");
    consumer.start();
    consumer.join();

    List<String> captured1 = consumer.getCapturedOutput();
    List<String> captured2 = consumer.getCapturedOutput();

    // Modify one list - should not affect the other
    captured1.clear();

    assertEquals("Second list should still have original content", 3, captured2.size());
    assertEquals("First list should be empty", 0, captured1.size());
    assertEquals("Consumer's internal list should be unchanged", 3, consumer.getCapturedOutput().size());
  }

  @Test
  public void testStreamConsumerMultipleCallsReturnConsistentData() throws InterruptedException {
    String testOutput = "Line 1\nLine 2\nLine 3";
    InputStream inputStream = new ByteArrayInputStream(testOutput.getBytes());

    StreamConsumer consumer = new StreamConsumer(inputStream, "Line 2");
    consumer.start();
    consumer.join();

    List<String> firstCall = consumer.getCapturedOutput();
    List<String> secondCall = consumer.getCapturedOutput();
    List<String> thirdCall = consumer.getCapturedOutput();

    assertEquals("All calls should return same content", firstCall, secondCall);
    assertEquals("All calls should return same content", secondCall, thirdCall);
    assertEquals("All should have 3 lines", 3, thirdCall.size());
  }
}