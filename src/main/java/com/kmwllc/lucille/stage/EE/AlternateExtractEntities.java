package com.kmwllc.lucille.stage.EE;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AlternateExtractEntities extends Stage {

  private String inputField;
  private String outputField;
  private String dictionaryFile;
  private String dictionaryManager = "FST";
  private DictionaryManager dictMgr = new PatriciaTrieDictionaryManager();
  private static Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

  public AlternateExtractEntities(Config config) {
    super(config);
    inputField = config.getString("source");
    outputField = config.getString("dest");
    dictionaryFile = config.getString("dictionaries");
    dictionaryManager = config.getString("dictionaryManager");

    switch (dictionaryManager) {
      case "FST":
        dictMgr = new FSTDictionaryManager();
        break;
      case "Trie":
        dictMgr = new PatriciaTrieDictionaryManager();
        break;
      default:
        throw new RuntimeException(String.format("Dictionary Manager (%s) is not supported",
          dictionaryManager));
    }

    try {
      InputStream in = new FileInputStream(dictionaryFile);
      dictMgr.loadDictionary(in);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    String input = doc.getStringList(inputField).get(0);
    String[] tokens = tokenizer.tokenize(input);
    Set<EntityInfo> infos = new HashSet<>();

    for (int i = 0; i < tokens.length; i++) {
      // find longest match
      int mark = i;
      List<String> curr = new ArrayList<>();
      do {
        curr.add(tokens[mark++]);
      } while (dictMgr.hasTokens(curr) && mark < tokens.length);
      List<String> longestMatch = (mark == tokens.length) ? curr : curr.subList(0, curr.size() - 1);

      EntityInfo ei = dictMgr.getEntity(longestMatch);
      if (ei != null) {
        infos.add(ei);
      }
    }

    Set<String> outputs = new HashSet<>();
    for (EntityInfo ei : infos) {
      outputs.addAll(ei.getPayloads());
    }
    for (String payload : outputs) {
      doc.addToField(outputField, payload);
    }

    return null;
  }

}