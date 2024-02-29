package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertThrows;
import org.junit.Test;
import com.kmwllc.lucille.core.StageException;

public class ParseFloatsTest {

  StageFactory factory = StageFactory.of(ParseFloats.class);

  @Test 
  public void testMalformedConfig() {
    assertThrows(StageException.class, () -> factory.get("ParseFloatsTest/bad.conf"));
  }

  @Test 
  public void testProcessDocument() {
    
  }
}
