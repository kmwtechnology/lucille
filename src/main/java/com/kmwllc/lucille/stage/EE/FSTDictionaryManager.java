package com.kmwllc.lucille.stage.EE;

import com.google.common.base.Strings;

import com.kmwllc.lucille.stage.EE.DictionaryManager;
import com.kmwllc.lucille.stage.EE.EntityInfo;
import com.kmwllc.lucille.util.Range;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by matt on 4/20/17.
 */
public class FSTDictionaryManager implements DictionaryManager {

  private static String separator = ",";
  private FST<BytesRef> fst;
  private Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

  @Override
  public void loadDictionary(InputStream in) {
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String s;
    Builder<BytesRef> b = new Builder<>(FST.INPUT_TYPE.BYTE1, ByteSequenceOutputs.getSingleton());

    try {
      while ((s = br.readLine()) != null) {
        if (separator == null) {
          b.add(toIntsRef(new BytesRef(s.trim())), new BytesRef());
        } else {
          String[] ss = s.split(separator);
          Set<String> payloads = new HashSet<>();
          if (ss.length > 1) {
            for (int i = 1; i < ss.length; i++) {
              payloads.add(ss[i]);
            }
          }
          String p = String.join(separator, payloads);

          b.add(toIntsRef(new BytesRef(ss[0])), new BytesRef(p));
        }

      }
      fst = b.finish();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private IntsRef toIntsRef(BytesRef b) {
    IntsRefBuilder irb = new IntsRefBuilder();
    irb.grow(b.length);
    irb.clear();
    for (int i = 0; i < b.length; i++) {
      irb.append(b.bytes[b.offset + i] & 0xFF);
    }
    return irb.get();
  }

  private <T> boolean hasPrefix(FST<T> fst, BytesRef input) throws IOException {
//    assert fst.inputType == FST.INPUT_TYPE.BYTE1;
    final FST.BytesReader fstReader = fst.getBytesReader();

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    // Accumulate output as we go
    T output = fst.outputs.getNoOutput();
    for (int i = 0; i < input.length; i++) {
      if (fst.findTargetArc(input.bytes[i + input.offset] & 0xFF, arc, arc, fstReader) == null) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean hasTokens(List<String> t) {
    String key = String.join(" ", t);
    try {
      return hasPrefix(fst, new BytesRef(key));
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public EntityInfo getEntity(List<String> t) {
    String key = String.join(" ", t);
    EntityInfo ei = null;
    try {
      BytesRef out = Util.get(fst, new BytesRef(key));
      if (out != null) {
        ei = new EntityInfo();
        ei.setTerm(key);
        String utf = out.utf8ToString();
        utf = utf.trim();
        if (!Strings.isNullOrEmpty(utf) && separator != null) {
          String[] split = utf.split(separator);
          ei.setPayloads(Arrays.asList(split));
        }
      }
      return ei;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public Map<Range, EntityInfo> findEntities(String input, boolean doNested, boolean doOverlap) {
    Map<Range, EntityInfo> output = new HashMap<>();

    String[] tokens = tokenizer.tokenize(input);
    int i = 0;
    while (i < tokens.length) {
      int mark = i;
      Set<List<String>> adds = new HashSet<>();
      List<String> curr = new ArrayList<>();
      do {
        curr.add(tokens[mark++]);
        if (doNested && hasTokens(curr)) {
          adds.add(new ArrayList<>(curr));
        }
      } while (hasTokens(curr) && mark < tokens.length);
      if (!doNested) {
        List<String> longestMatch = (mark == tokens.length && hasTokens(curr)) ?
          curr : curr.subList(0, curr.size() - 1);
        adds.add(longestMatch);
      }

      int longest = 0;
      for (List<String> add : adds) {
        longest = Math.max(longest, add.size());
        EntityInfo ei = add.size() > 0 ? getEntity(add) : null;
        if (ei != null) {
          output.put(new Range(i, i + add.size()), ei);
        }
      }
      if (!doOverlap && adds.size() > 0) {
        if (mark == tokens.length && doNested) {

        } else {
          i += longest - 1;
        }
      }
      if (doOverlap && !doNested && mark == tokens.length) {
        i += longest - 1;
      }

      i++;
    }

    return output;
  }

  public List<String> findEntityStrings(String input, boolean doNested, boolean doOverlap) {
    Map<Range, EntityInfo> entities = findEntities(input, doNested, doOverlap);
    Set<String> found = new HashSet<>();
    for (EntityInfo ei : entities.values()) {
      if (ei.getPayloads() != null) {
        found.addAll(ei.getPayloads());
      } else {
        found.add(ei.getTerm());
      }
    }

    return new ArrayList<>(found);
  }

  public static String getSeparator() {
    return separator;
  }

  public static void setSeparator(String separator) {
    FSTDictionaryManager.separator = separator;
  }
}