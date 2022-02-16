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
import java.util.Locale;

/**
 * An alternative implementation of the ApplyStopWords stage.
 */
public class AlternateApplyStopWords extends Stage {

  private final List<String> dictionaries;
  private final List<String> fieldNames;
  private final List<String> stopWords;

  public AlternateApplyStopWords(Config config) throws StageException {
    super(config);

    this.dictionaries = config.getStringList("dictionaries");
    this.fieldNames = config.hasPath("fields") ? config.getStringList("fields") : null;
    this.stopWords = readStopWords(this.dictionaries);
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
          String term = postProcess(charTermAttribute.toString());
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

  /**
   * Read all the stop words from their paths.
   *
   * @param dictionaries
   * @return
   * @throws StageException
   */
  public List<String> readStopWords(List<String> dictionaries) throws StageException {
    List<String> stopWords = new ArrayList<>();
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
    return stopWords;
  }

  /**
   * Process a string which has punctuation so the punctuation is tokenized separately.
   *
   * @param s the string
   * @return the modified string
   */
  public static String preProcess(String s) {

    for (int i = 0; i < s.length(); i++) {
      String single = s.substring(i, i + 1);
      if (single.equals("!") || single.equals(",") || single.equals(";") || single.equals(".") || single.equals("?") || single.equals("-")) {
        int count = 1;
        String replace = "";
        // only need to add on left side
        if (s.substring(i - 1, i).matches("[a-zA-Z]+")) {
          replace = replace + " L";
          count++;
        }

        replace = replace + single;

        if (s.substring(i, i + 1).matches("[a-zA-Z]+")) {
          replace = replace + "R ";
          count++;
        }
        System.out.println(replace);
        s = s.substring(0, i) + replace + s.substring(i + 1);
        i = i + count + 1;
      }
    }
    return s;
  }

  public String postProcess(String s) {
    for (String stopWord : stopWords) {
      if (s.contains(stopWord)) {
        String temp = s.replace(stopWord, "");
        if (temp.equals("?") || temp.equals(".")) {
          s = s.replace(stopWord, "");
        }
      }
    }
    return s;
  }
}