package com.kmwllc.lucille.util;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.StageException;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

public class StageUtilsTest {

  @Test
  public void testValidateListLenNotZero() {
    Exception exception = assertThrows(StageException.class, () -> {
      StageUtils.validateListLenNotZero(new ArrayList<>(), "TestStage");
    });

    String expectedMessage = "An empty field list was supplied to TestStage";
    String actualMessage = exception.getMessage();

    assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  public void testValidateFieldNumsEqual() {
    Exception exception = assertThrows(StageException.class, () -> {
      StageUtils.validateFieldNumsEqual(new ArrayList<String>(Arrays.asList("a", "b")),
          new ArrayList<String>(Arrays.asList("c", "d", "e")), "TestStage");
    });

    String expectedMessage = "Unequal length field lists supplied to TestStage";
    String actualMessage = exception.getMessage();

    assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  public void testValidateFieldNumsSeveralToOne() {
    Exception firstException = assertThrows(StageException.class, () -> {
      StageUtils.validateFieldNumsSeveralToOne(new ArrayList<String>(Arrays.asList("a", "b")),
          new ArrayList<String>(Arrays.asList("c", "d", "e")), "TestStage");
    });
    String actualMessageFirst = firstException.getMessage();

    Exception secondException = assertThrows(StageException.class, () -> {
      StageUtils.validateFieldNumsSeveralToOne(new ArrayList<String>(Arrays.asList("a", "b", "c")),
          new ArrayList<String>(Arrays.asList()), "TestStage");
    });
    String actualMessageSecond = secondException.getMessage();

    String expectedMessage = "TestStage was supplied with an invalid number of fields in the inputted field lists";

    assertTrue(actualMessageFirst.contains(expectedMessage));
    assertTrue(actualMessageSecond.contains(expectedMessage));
  }

  @Test
  public void testValidateListLenNotZeroNoException() throws StageException {
    StageUtils.validateListLenNotZero(new ArrayList<String>(Arrays.asList("a", "b")), "TestStage");
  }

  @Test
  public void testValidateFieldNumsEqualNoException() throws StageException {
    StageUtils.validateFieldNumsEqual(new ArrayList<String>(Arrays.asList("a", "b")),
        new ArrayList<String>(Arrays.asList("c", "d")), "TestStage");
  }

  @Test
  public void testValidateFieldNumsSeveralToOneNoException() throws StageException {
    StageUtils.validateFieldNumsSeveralToOne(new ArrayList<String>(Arrays.asList("a", "b")),
        new ArrayList<String>(Arrays.asList("c")), "TestStage");
    StageUtils.validateFieldNumsSeveralToOne(new ArrayList<String>(Arrays.asList("a", "b")),
        new ArrayList<String>(Arrays.asList("c", "d")), "TestStage");
  }

}
