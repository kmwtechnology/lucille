package com.kmwllc.lucille.stage.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.StageException;
import java.util.Map;
import org.junit.Test;

public class DictionaryManagerTest {

  @Test
  public void testDictionaryInitializedOnceOnly() throws StageException {
    Map<String, String[]> dict1 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", false, false, Map.of());
    assertTrue(dict1.containsKey("Canada"));
    assertFalse(dict1.containsKey("canada"));

    // the same dictionary instance is returned for the same setting of path and ignoreCase
    Map<String, String[]> dict2 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", false, false, Map.of());
    assertSame(dict1, dict2);

    // a different dictionary is initialized for a different setting of ignoreCase
    Map<String, String[]> dict3 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", true, false, Map.of());
    assertNotSame(dict1, dict3);
    assertFalse(dict3.containsKey("Canada"));
    assertTrue(dict3.containsKey("canada"));

    Map<String, String[]> dict4 =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", true, false, Map.of());
    assertSame(dict3, dict4);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDictionaryIsUnmofidiable() throws StageException {
    Map<String, String[]> dict =
        DictionaryManager.getDictionary("classpath:DictionaryLookupTest/dictionary.txt", false, false, Map.of());
    assertTrue(dict.containsKey("Canada"));
    dict.put("Canada", new String[]{"abc"});
  }
}