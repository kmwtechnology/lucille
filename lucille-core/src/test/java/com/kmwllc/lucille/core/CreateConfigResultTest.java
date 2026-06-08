package com.kmwllc.lucille.core;

import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class CreateConfigResultTest {

  @Test
  public void testInvalidConfigId() {
    assertThrows(IllegalArgumentException.class, () -> new CreateConfigResult(null));
    assertThrows(IllegalArgumentException.class, () -> new CreateConfigResult(null, "removed-config-id"));
  }

}
