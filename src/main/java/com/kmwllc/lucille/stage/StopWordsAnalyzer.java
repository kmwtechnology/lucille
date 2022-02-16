package com.kmwllc.lucille.stage;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import java.io.IOException;

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

    // using a filtering token filter instead of a lowercase filter
    TokenStream result = new FilteringTokenFilter(source) {
      @Override
      protected boolean accept() throws IOException {
        return true;
      }
    };

    // use a stop filter for stop words
    StopFilter stopFilter = new StopFilter(result, stopWords);
    result = (TokenStream) stopFilter;
    return new TokenStreamComponents(source, result);
  }
}