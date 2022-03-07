package com.kmwllc.lucille.stage.EE;

import org.apache.commons.csv.CSVFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Factory to build FSTDictionaryManager.  The default settings should suffice in
 * most cases; but the following parameters may be specified:
 * <ul><li>analyzer - Lucene analyzer to use while reading the dictionary.  You
 * may want to change this if, e.g. you need to extract entities that contain punctuation
 * characters that would be dropped by the Standard tokenizer</li>
 * <li>csvFormat - Commons CSV defines various formats (e.g. EXCEL) and provides means
 * to set things like separator char and quote char</li></ul>
 */
public class FSTDictionaryManagerFactory {
  private static FSTDictionaryManagerFactory INSTANCE;
  private final Analyzer defaultAnalyzer = new StandardAnalyzer();
  private final CSVFormat defaultCSVFormat = CSVFormat.DEFAULT;

  private FSTDictionaryManagerFactory() {}

  /**
   * Gets the singleton FSTDictionaryManagerFactory, initializing it if necessary
   * @return The factory singleton
   */
  public static FSTDictionaryManagerFactory get() {
    if (INSTANCE == null) {
      INSTANCE = new FSTDictionaryManagerFactory();
    }
    return INSTANCE;
  }

  /**
   * Creates the default FST dictionary manager, using default CSV settings and Lucene
   * standard analyzer.  This should be sufficient for majority of cases.
   * @return FST dictionary manager
   */
  public FSTDictionaryManager createDefault() {
    return create(defaultAnalyzer, defaultCSVFormat);
  }

  /**
   * Creates an FST dictionary manager with the specified Lucene analyzer and Commons-CSV
   * format.
   * @param analyzer Lucene analyzer to handle tokenization
   * @param csvFormat CSV format settings for the input
   * @return FST dictionary manager
   */
  public FSTDictionaryManager create(Analyzer analyzer, CSVFormat csvFormat) {
    return new FSTDictionaryManager(analyzer, csvFormat);
  }
}
