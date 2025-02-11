package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ThreadNameUtilsTest {

  @Test
  public void testCreateName() {
    String nameUsingConvenience = ThreadNameUtils.createName("test");
    String nameUsingNull = ThreadNameUtils.createName("test", null);
    String nameWithRunId = ThreadNameUtils.createName("test", "runId");

    assertEquals("Lucille-test", nameUsingConvenience);
    assertEquals("Lucille-test", nameUsingNull);
    assertEquals("Lucille-runId-test", nameWithRunId);
  }
}
