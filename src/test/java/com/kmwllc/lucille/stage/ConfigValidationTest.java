package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ConfigValidationTest {

  @Test
  public void testConditions() {
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions1.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions2.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions3.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions4.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions5.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions6.conf");
  }

  @Test
  public void testConcatenate() {
    testException(Concatenate.class, "ConfigValidationTest/concatenate1.conf");
    testException(Concatenate.class, "ConfigValidationTest/concatenate2.conf");
    testException(Concatenate.class, "ConfigValidationTest/concatenate3.conf");
  }


  private static void testException(Class<? extends Stage> stageClass, String config) {
    try {
      StageFactory.of(stageClass).get(config);
      fail();
    } catch (StageException e) {
      // expected
    }
  }

  // todo add examples of failures based on the different type of properties
}
