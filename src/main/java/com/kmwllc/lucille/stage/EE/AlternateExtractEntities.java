package com.kmwllc.lucille.stage.EE;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

public class AlternateExtractEntities extends Stage {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final List<String> dictionaries;
  private DictionaryManager dictMgr;
  private static Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

  public AlternateExtractEntities(Config config) {
    super(config);
    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.dictionaries = config.getStringList("dictionaries");
    this.dictMgr = FSTDictionaryManagerFactory.get().createDefault();
  }

  @Override
  public void start() {
    for (String dictFile : dictionaries) {
      File d = new File(dictFile);
      try {
        InputStream in = new FileInputStream(d);
        dictMgr.loadDictionary(in);
      } catch (Exception e) {
        log.info("dictionaries could not be loaded in.", e);
      }
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // For each of the field names, extract dictionary values from it.
    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one source or dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      List<String> entities = null;
      try {
        entities = dictMgr.findEntityStrings(doc.getString(sourceField), false, false);
      } catch (IOException e) {
        throw new StageException("entities could not be extracted", e);
      }
      doc.update(destField, UpdateMode.APPEND, entities.toArray(new String[0]));
    }

    return null;
  }

}