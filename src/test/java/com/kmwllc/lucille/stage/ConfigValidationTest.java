package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class ConfigValidationTest {

  private final StageFactory factory = StageFactory.of(NoopStage.class);

  @Test(expected = StageException.class)
  public void testConditionsRequired() throws StageException {
    factory.get("ConfigValidationTest/invalid-conditions.conf");
  }

  @Test(expected = StageException.class)
  public void testConditionsRequired2() throws StageException {
    factory.get("ConfigValidationTest/invalid-conditions2.conf");
  }

  @Test(expected = StageException.class)
  public void testConditionsOptional() throws StageException {
    factory.get("ConfigValidationTest/invalid-conditions3.conf");
  }

  @Test(expected = StageException.class)
  public void testConditionsRequiredMissing() throws StageException {
    factory.get("ConfigValidationTest/invalid-conditions4.conf");
  }

  @Test(expected = StageException.class)
  public void testConditionsRequiredMissing2() throws StageException {
    factory.get("ConfigValidationTest/invalid-conditions5.conf");
  }

  @Test(expected = StageException.class)
  public void testConditionsIllegalProperty() throws StageException {
    factory.get("ConfigValidationTest/invalid-conditions6.conf");
  }
}
