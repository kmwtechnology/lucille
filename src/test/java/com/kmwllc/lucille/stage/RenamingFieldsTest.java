package com.kmwllc.lucille.stage;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class RenamingFieldsTest {

  @Test
  public void testRenamingFields() throws Exception {
    Config config = ConfigFactory.load("RenamingFieldsTest/config.conf");
    CopyFields stage = new CopyFields(config);
    stage.start();


  }

}
