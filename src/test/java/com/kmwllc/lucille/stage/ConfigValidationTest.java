package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ConfigValidationTest {

  @Test
  public void testConditions() {

    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions-field-missing.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions-field-renamed.conf");

    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions-values-renamed.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions-values-renamed.conf");

    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions-optional-unknown.conf");
    testException(NoopStage.class, "ConfigValidationTest/invalid-conditions-optional-renamed.conf");
  }

  @Test
  public void testConcatenate() {
    testException(Concatenate.class, "ConfigValidationTest/concatenate-format-missing.conf");
    testException(Concatenate.class, "ConfigValidationTest/concatenate-dest-missing.conf");
    testException(Concatenate.class, "ConfigValidationTest/concatenate-unknown-property.conf");
    testException(Concatenate.class, "ConfigValidationTest/concatenate-invalid-parent.conf");
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
