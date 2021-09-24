package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

public class SolrConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrClient client;

  private List<String> postActions;
  private List<String> preActions;

  // There's 2 approaches to this I can think of right now...
  // 1. Have  lists of post/pre Strings, which are directly the requests to send to Solr. So if you wanted to commit,
  // you would put "<commit/>" in the postActions lsit. We could have a syntax for replacing runId, but otherwise we can
  // just iterate through the request Strings and issue them.
  // 2. We have a map of action names to a list of parameters. The list of parameters would be different based on the
  // action. We would support a subset of actions and each action would expect a certain number of parameters.
  // For example, commit would have no params, but deleteByQuery would have to pass in the query string. This
  // puts more of the Solr syntax under the hood but makes the config file more complex and specific in what we support.

  public SolrConnector(Config config) {
    super(config);
    this.preActions = ConfigAccessor.getOrDefault(config, "preActions", new ArrayList<>());
    this.postActions = ConfigAccessor.getOrDefault(config, "postActions", new ArrayList<>());
    this.client = new HttpSolrClient.Builder(config.getString("solr.url")).build();
  }

  @Override
  public void preExecute(String runId) {
    Map<String, String> replacement = new HashMap<>();
    replacement.put("runId", runId);
    StrSubstitutor sub = new StrSubstitutor(replacement, "{", "}");
    preActions  = preActions.stream().map(sub::replace).collect(Collectors.toList());

    executeActions(runId, preActions);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
  }

  @Override
  public void postExecute(String runId) {
    Map<String, String> replacement = new HashMap<>();
    replacement.put("runId", runId);
    StrSubstitutor sub = new StrSubstitutor(replacement, "{", "}");
    postActions = postActions.stream().map(sub::replace).collect(Collectors.toList());

    executeActions(runId, postActions);
  }

  private void executeActions(String runId, List<String> actions) {
    for (String action : actions) {
      // Collection<ContentStream> content = ClientUtils.toContentStreams(action, "application/json; charset=UTF-8");
      RequestWriter.ContentWriter contentWriter = new RequestWriter.StringPayloadContentWriter(action, "text/xml");
      GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update", null);
      request.setContentWriter(contentWriter);

      try {
        NamedList<Object> list = client.request(request);
      } catch (Exception e) {
        log.error("Failed to perform action: " + action, e);
      }
    }
  }
}