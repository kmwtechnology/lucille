package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.monitor.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;

public class MatchQuery extends Stage {
  public static final String FIELDS_PARAM = "fields";
  public static final String QUERIES_PARAM = "queries";
  public static final String MATCHEDQUERIES_PARAM = "matchedQueriesField";

  // the list of fields to run the queries against
  private final List<String> fieldsList;

  // the list of queries to run
  private final List<? extends ConfigObject> queryList;

  private final String matchedQueriesField;

  private Monitor monitor;

  public MatchQuery(Config config) {
    super(
        new StageSpec(config)
            .withRequiredProperties(FIELDS_PARAM, QUERIES_PARAM, MATCHEDQUERIES_PARAM));
    fieldsList = config.getStringList(FIELDS_PARAM);
    queryList = config.getObjectList(QUERIES_PARAM);
    matchedQueriesField = config.getString(MATCHEDQUERIES_PARAM);
  }

  @Override
  public void start() throws StageException {
    if (fieldsList.size() == 0) {
      throw new StageException(
          String.format("MatchQuery requires at least one %s property.", FIELDS_PARAM));
    }
    if (queryList.size() == 0) {
      throw new StageException(
          String.format("MatchQuery requires at least one %s property.", QUERIES_PARAM));
    }
    if (StringUtils.isBlank(matchedQueriesField)) {
      throw new StageException(
          String.format("MatchQuery requires a %s property.", MATCHEDQUERIES_PARAM));
    }

    try {
      Analyzer analyzer = new StandardAnalyzer();
      monitor = new Monitor(analyzer);

      // TODO:: default field is the 1st field configured
      QueryParser parser = new QueryParser(fieldsList.get(0), analyzer);

      for (ConfigObject query : this.queryList) {
        for (String queryName : query.keySet()) {
          String q = query.get(queryName).unwrapped().toString();
          MonitorQuery mq = new MonitorQuery(queryName, parser.parse(q));
          monitor.register(mq);
        }
      }
    } catch (IOException | ParseException e) {
      throw new StageException("Failed to start MatchQuery stage.", e);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    try {
      org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

      // add each configured field to the lucene document
      for (String field : this.fieldsList) {
        if (doc.has(field)) {
          luceneDoc.add(new TextField(field, doc.getString(field), Field.Store.YES));
        }
      }

      MatchingQueries<QueryMatch> matches = monitor.match(luceneDoc, QueryMatch.SIMPLE_MATCHER);
      for (QueryMatch match : matches.getMatches()) {
        doc.addToField(this.matchedQueriesField, match.getQueryId());
      }

    } catch (IOException e) {
      throw new StageException("processDocument failed", e);
    }

    return null;
  }
}
