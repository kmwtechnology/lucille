package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigUtils;
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

/**
 * Connector for issuing requests to Solr.
 *
 * Connector Parameters:
 *
 *   - preActions (List<String>, Optional) : A list of requests to be issued to Solr. These actions will be performed first.
 *   - postActions (List<String>, Optional) : A list of requests to be issued to Solr. These actions will be performed second.
 *   - solr.url (String) : The url of the Solr instance for this Connector to issue its requests to.
 */
public class SolrConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrClient client;
  private final GenericSolrRequest request;
  private List<String> replacedActions;

  private List<String> postActions;
  private List<String> preActions;

  public SolrConnector(Config config) {
    super(config);
    this.preActions = ConfigUtils.getOrDefault(config, "preActions", new ArrayList<>());
    this.postActions = ConfigUtils.getOrDefault(config, "postActions", new ArrayList<>());
    this.client = new HttpSolrClient.Builder(config.getString("solr.url")).build();
    this.request = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update", null);
    this.replacedActions = new ArrayList<>();
  }

  public SolrConnector(Config config, SolrClient client, List<String> replacedActions) {
    this(config);
    this.client = client;
    this.replacedActions = replacedActions;
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    replacedActions.clear();
    Map<String, String> replacement = new HashMap<>();
    replacement.put("runId", runId);
    StrSubstitutor sub = new StrSubstitutor(replacement, "{", "}");
    replacedActions.addAll(preActions.stream().map(sub::replace).collect(Collectors.toList()));

    executeActions(replacedActions);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    replacedActions.clear();
    Map<String, String> replacement = new HashMap<>();
    replacement.put("runId", runId);
    StrSubstitutor sub = new StrSubstitutor(replacement, "{", "}");
    replacedActions.addAll(postActions.stream().map(sub::replace).collect(Collectors.toList()));

    executeActions(replacedActions);
  }

  public List<String> getPreActions() {
    return preActions;
  }

  public List<String> getPostActions() {
    return postActions;
  }

  private void executeActions(List<String> actions) throws ConnectorException {
    for (String action : actions) {
      RequestWriter.ContentWriter contentWriter = new RequestWriter.StringPayloadContentWriter(action, "text/xml");
      request.setContentWriter(contentWriter);

      try {
        NamedList<Object> resp = client.request(request);
        log.info(String.format("Action \"%s\" complete, response: %s", action, resp.asShallowMap().get("responseHeader")));
      } catch (Exception e) {
        throw new ConnectorException("Failed to perform action: " + action, e);
      }
    }
  }
}