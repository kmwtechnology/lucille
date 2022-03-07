//package com.kmwllc.lucille.stage.EE;
//
//import com.kmwllc.lucille.util.Range;
//import org.apache.commons.collections4.trie.PatriciaTrie;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by matt on 4/19/17.
// */
//public class PatriciaTrieDictionaryManager implements DictionaryManager {
//  private static final String sep = ",";
//  private PatriciaTrie<EntityInfo> trie = new PatriciaTrie<>();
//
//  @Override
//  public void loadDictionary(InputStream in) {
//    // TODO: this should be a singleton instance of the dictionary being loaded.. o/w this is wasteful of memory.
//    BufferedReader br = new BufferedReader(new InputStreamReader(in));
//    String s;
//    try {
//      while ((s = br.readLine()) != null) {
//        String[] ss = s.split(sep);
//        EntityInfo ei = new EntityInfo();
//        ei.setTerm(ss[0]);
//        List<String> payloads = new ArrayList<>();
//        if (ss.length > 1) {
//          for (int i = 1; i < ss.length; i++) {
//            payloads.add(ss[i]);
//          }
//        }
//        ei.setPayloads(payloads);
//        trie.put(ss[0], ei);
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Override
//  public boolean hasTokens(List<String> t) {
//    String key = String.join(" ", t);
//    return trie.prefixMap(key).size() > 0;
//  }
//
//  @Override
//  public EntityInfo getEntity(List<String> t) {
//    String key = String.join(" ", t);
//    return trie.get(key);
//  }
//
//  @Override
//  public List<String> findEntityStrings(String input, boolean doNested, boolean doOverlap) {
//    return null;
//  }
//
//  @Override
//  public Map<Range, EntityInfo> findEntities(String input, boolean doNested, boolean doOverlap) {
//    return null;
//  }
//}