package com.kmwllc.lucille.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.junit.Test;

public class ThrowableUtilsTest {

  @Test
  public void testIsFatal() {
    assertTrue(ThrowableUtils.isFatal(new OutOfMemoryError("x")));
    assertTrue(ThrowableUtils.isFatal(new InternalError("x")));
    assertTrue(ThrowableUtils.isFatal(new UnknownError("x")));

    // local to the work that caused them
    assertFalse(ThrowableUtils.isFatal(new StackOverflowError("x")));
    assertFalse(ThrowableUtils.isFatal(new NoClassDefFoundError("x")));
    assertFalse(ThrowableUtils.isFatal(new AssertionError("x")));
    assertFalse(ThrowableUtils.isFatal(new IOException("x")));
    assertFalse(ThrowableUtils.isFatal(new RuntimeException("x")));
    assertFalse(ThrowableUtils.isFatal(null));
  }
}
