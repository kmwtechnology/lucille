package com.kmwllc.lucille.stage;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

/**
 * Custom StopWordsAnalyzer for AlternateApplyStopWords stage.
 */
class StopWordsAnalyzer extends Analyzer {

  private CharArraySet stopWords;

  public StopWordsAnalyzer(CharArraySet stopWords) {
    this.stopWords = stopWords;
  }

  @Override
  protected TokenStreamComponents createComponents(String s) {
    // use whitespace tokenizer to preserve punctuation
    final Tokenizer source = new WhitespaceTokenizer();

    TokenStream result = new LowerCaseFilter(source);

    // use a stop filter for stop words
    StopFilter stopFilter = new StopFilter(result, stopWords);
    result = (TokenStream) stopFilter;
    return new TokenStreamComponents(source, result);
  }
}
