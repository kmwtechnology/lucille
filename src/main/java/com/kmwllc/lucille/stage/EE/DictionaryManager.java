package com.kmwllc.lucille.stage.EE;


import com.kmwllc.lucille.util.Range;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by matt on 4/19/17.
 */
public interface DictionaryManager {
  void loadDictionary(InputStream in);
  boolean hasTokens(List<String> t);
  EntityInfo getEntity(List<String> t);
  List<String> findEntityStrings(String input, boolean doNested, boolean doOverlap);
  Map<Range, EntityInfo> findEntities(String input, boolean doNested, boolean doOverlap);
}