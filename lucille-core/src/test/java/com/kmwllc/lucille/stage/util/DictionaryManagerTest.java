package com.kmwllc.lucille.stage.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DictionaryManagerTest {

  @Test
  public void testDictionaryInitializedOnceOnly() throws StageException {
    Map<String, List<String>> dict1 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", false, false, ConfigFactory.empty());
    assertTrue(dict1.containsKey("Canada"));
    assertFalse(dict1.containsKey("canada"));

    // the same dictionary instance is returned for the same setting of path and ignoreCase
    Map<String, List<String>> dict2 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", false, false, ConfigFactory.empty());
    assertSame(dict1, dict2);

    // a different dictionary is initialized for a different setting of ignoreCase
    Map<String, List<String>> dict3 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", true, false, ConfigFactory.empty());
    assertNotSame(dict1, dict3);
    assertFalse(dict3.containsKey("Canada"));
    assertTrue(dict3.containsKey("canada"));

    Map<String, List<String>> dict4 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", true, false, ConfigFactory.empty());
    assertSame(dict3, dict4);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDictionaryIsUnmofidiable() throws StageException {
    Map<String, List<String>> dict =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", false, false, ConfigFactory.empty());
    assertTrue(dict.containsKey("Canada"));
    dict.put("Canada", List.of("abc"));
  }
}