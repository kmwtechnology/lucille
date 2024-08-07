package com.kmwllc.lucille.enron;

import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VerificationTest {
    @Test
    public void testConf() throws Exception {
      Map<String, List<Exception>> exceptions = Runner.runInValidationMode("opensearchCopy.conf");
      List<Exception> exceptionlist = exceptions.get("pipeline1");
      assertEquals(0, exceptionlist.size());
    }
}
