package com.kmwllc.lucille.stage.EE;

import com.google.common.base.Strings;
import com.kmwllc.lucille.util.Range;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Dictionary Manager backed by FST.  We use Lucene's implementation of FST's for smoking fast
 * lookup of String keys.  The FST is built from a sorted list of CSV entries.  (If the list is
 * not sorted the FST can not be built).  Entries must contain a term followed by zero or more
 * payloads, each separated by a comma.
 * <p/>
 * To build an FSTDictionaryManager, use the FSTDictionaryManagerFactory. The default manager
 * should be sufficient for most applications (see documentation on FSTDictionaryManagerFactory
 * for details).
 * <p/>
 * Created by matt on 4/20/17.
 */
public class FSTDictionaryManager implements DictionaryManager {
  private FST<BytesRef> fst;
  private final Analyzer analyzer;
  private final CSVFormat csvFormat;
  private final String separator = ",";

  FSTDictionaryManager(Analyzer analyzer, CSVFormat csvFormat) {
    this.analyzer = analyzer;
    this.csvFormat = csvFormat;
  }

  @Override
  public void loadDictionary(InputStream in) throws IOException {
    Builder<BytesRef> b = new Builder<>(FST.INPUT_TYPE.BYTE1, ByteSequenceOutputs.getSingleton());
    Iterable<CSVRecord> recs = csvFormat.parse(new InputStreamReader(in));
    for (CSVRecord rec : recs) {
      if (rec.size() == 1) {
        b.add(toIntsRef(new BytesRef(rec.get(0).trim())), new BytesRef());
      } else {
        Set<String> payloads = new HashSet<>();
        for (int i = 1; i < rec.size(); i++) {
          payloads.add(rec.get(i));
        }
        String p = String.join(separator, payloads);

        b.add(toIntsRef(new BytesRef(rec.get(0))), new BytesRef(p));
      }
    }
    fst = b.finish();
    in.close();
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
   // assert fst.inputType == FST.INPUT_TYPE.BYTE1;

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

  public <T> boolean matchesCompletely(FST<T> fst, BytesRef input) throws IOException {
    //assert fst.inputType == FST.INPUT_TYPE.BYTE1;

    final FST.BytesReader fstReader = fst.getBytesReader();

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    // Accumulate output as we go
    T output = fst.outputs.getNoOutput();
    for (int i = 0; i < input.length; i++) {
      if (fst.findTargetArc(input.bytes[i + input.offset] & 0xFF, arc, arc, fstReader) == null) {
        return false;
      }
      output = fst.outputs.add(output, arc.output());
    }

    if (arc.isFinal()) {
      return true;
    } else {
      return false;
    }
  }

  private List<String> tokenize(String input) {
    List<String> output = new ArrayList<>();
    try {
      TokenStream tstream = analyzer.tokenStream(null, new StringReader(input));
      tstream.reset();
      while (tstream.incrementToken()) {
        output.add(tstream.getAttribute(CharTermAttribute.class).toString());
      }
      tstream.close();
    } catch (IOException e) {
      // Won't happen b/c we're using StringReader and not an IO-based reader
      throw new RuntimeException();
    }
    return output;
  }

  @Override
  public boolean hasTokens(List<String> t) throws IOException {
    String key = String.join(" ", t);
    return hasPrefix(fst, new BytesRef(key));
  }

  public boolean isCompleteMatch(List<String> t) throws IOException {
    String key = String.join(" ", t);
    return matchesCompletely(fst, new BytesRef(key));
  }

  @Override
  public EntityInfo getEntity(List<String> tokens) throws IOException {
    String key = String.join(" ", tokens);
    EntityInfo ei = null;
    BytesRef out = Util.get(fst, new BytesRef(key));
    List<String> payloads = new ArrayList<>();

    if (out != null) {
      String utf = out.utf8ToString();
      utf = utf.trim();
      if (!Strings.isNullOrEmpty(utf) && separator != null) {
        String[] split = utf.split(separator);
        payloads = Arrays.asList(split);
      }
      ei = new EntityInfo(key, payloads);
    }
    return ei;
  }

  @Override
  public List<EntityAnnotation> findEntities(String input, boolean doNested, boolean doOverlap)
    throws IOException {
    List<EntityAnnotation> output = new ArrayList<>();

    List<String> tokens = tokenize(input);
    int i = 0;
    int maxMatchIdx = -1;
    while (i < tokens.size()) {
      int mark = i;
      Set<List<String>> adds = new HashSet<>();
      List<String> curr = new ArrayList<>();
      do {
        curr.add(tokens.get(mark++));
        if (doNested && hasTokens(curr)) {
          adds.add(new ArrayList<>(curr));
        }
      } while (hasTokens(curr) && mark < tokens.size());
      if (!doNested) {
        List<String> longestMatch = (mark == tokens.size() && hasTokens(curr)) ?
          curr : curr.subList(0, curr.size() - 1);
        if (longestMatch.size() > 0) {
          if (i >= maxMatchIdx) {
            adds.add(longestMatch);
          } else if (doOverlap && (i + longestMatch.size()) >= maxMatchIdx) {
            adds.add(longestMatch);
          }
        }
      }

      int longest = 0;
      for (List<String> add : adds) {
        longest = Math.max(longest, add.size());
        EntityInfo ei = add.size() > 0 ? getEntity(add) : null;
        if (ei != null) {
          maxMatchIdx = i + add.size();
          output.add(new EntityAnnotation(new Range(i, i + add.size()), ei));
        }
      }
      if (!doOverlap && adds.size() > 0) {
        if (mark == tokens.size() && doNested) {

        } else {
          i += longest - 1;
        }
      }
      if (doOverlap && !doNested && mark == tokens.size()) {
        i += Math.max(longest - 1, 0);
      }

      i++;
    }

    return output;
  }

  @Override
  public List<String> findEntityStrings(String input, boolean doNested, boolean doOverlap)
    throws IOException {
    List<EntityAnnotation> entities = findEntities(input, doNested, doOverlap);
    List<EntityInfo> entityInfos = entities.stream()
      .map(e -> e.getEntityInfo())
      .collect(Collectors.toList());
    Set<String> found = new HashSet<>();
    for (EntityInfo ei : entityInfos) {
      if (ei.getPayloads().size() > 0) {
        found.addAll(ei.getPayloads());
      } else {
        found.add(ei.getTerm());
      }
    }

    return new ArrayList<>(found);
  }
}