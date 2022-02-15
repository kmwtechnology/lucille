package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.util.FileUtils;

import com.opencsv.CSVReader;
import com.typesafe.config.Config;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class AlternateApplyStopWords extends Stage {

  private final List<String> dictionaries;
  private final List<String> fieldNames;
  private final List<String> stopWords;

  public AlternateApplyStopWords(Config config) throws StageException {
    super(config);

    this.dictionaries = config.getStringList("dictionaries");
    this.fieldNames = config.hasPath("fields") ? config.getStringList("fields") : null;
    stopWords = new ArrayList<>();
    readStopWords(this.dictionaries);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (fieldNames == null || fieldNames.size() == 0) {
      return null;
    }

    List<String> fields = fieldNames != null ? fieldNames : new ArrayList<>(doc.getFieldNames());
    fields.removeAll(Document.RESERVED_FIELDS);

    for (String field : fields) {
      if (!doc.has(field))
        continue;

      // get value of field
      String val = (String) doc.asMap().get(field);

      // add spaces between all punctuation
      for (int i = val.length(); i > 0; i--) {
        String single = val.substring(i - 1, i);
        if (single.equals("!") || single.equals(",") || single.equals(";") || single.equals(".") || single.equals("?") || single.equals("-")) {
          //str.charAt(i) == '\'' || str.charAt(i) == '\"' || str.charAt(i) == ':') {
          val = val.replace(single, " " + single + " ");
          i = i - 2;
        }
      }

      StringReader reader = new StringReader(val);

      CharArraySet stopSet = StopFilter.makeStopSet(stopWords, true);
      StopWordsAnalyzer analyzer = new StopWordsAnalyzer(stopSet);
      TokenStream stream = analyzer.tokenStream(field, reader);
      try {
        stream.reset();
      } catch (IOException e) {
        System.out.println("Could not be reset.");
      }
      CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
      StringBuffer stringBuffer = new StringBuffer();

      try {
        while (stream.incrementToken()) {
          String term = charTermAttribute.toString();

          System.out.println(term);
          stringBuffer.append(term + " ");
        }
      } catch (IOException e) {
        System.out.println("Stream could not be incremented.");
      }

      try {
        stream.close();
      } catch (IOException e) {
        System.out.println("Stream could not be closed");
      }
      doc.update(field, UpdateMode.OVERWRITE, stringBuffer.toString().trim());
    }


    return null;
  }

  public void readStopWords(List<String> dictionaries) throws StageException {
    for (String dictFile : dictionaries) {
      try (CSVReader reader = new CSVReader(FileUtils.getReader(dictFile))) {

        String[] line;
        boolean ignore = false;
        while ((line = reader.readNext()) != null) {
          if (line.length == 0)
            continue;

          for (String term : line) {
            if (term.contains("\uFFFD")) {
              //log.warn(String.format("Entry \"%s\" on line %d contained malformed characters which were removed. " +
              //"This dictionary entry will be ignored.", term, reader.getLinesRead()));
              ignore = true;
              break;
            }
          }

          if (ignore) {
            ignore = false;
            continue;
          }

          String word = line[0].trim();
          stopWords.add(word);
        }
      } catch (Exception e) {
        throw new StageException("Failed to read from the given file.", e);
      }
    }
  }
}