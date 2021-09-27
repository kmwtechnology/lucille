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
 * Connector for issuing requests to Solr. Requests should be formatted as xml Strings and can contain the {runId} wildcard,
 * which will be substituted for the current runId before the requests are issued. This is the only wildcard supported.
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
  private List<String> postReplacedActions;
  private List<String> preReplacedActions;

  private List<String> postActions;
  private List<String> preActions;

  public SolrConnector(Config config) {
    super(config);
    this.preActions = ConfigUtils.getOrDefault(config, "preActions", new ArrayList<>());
    this.postActions = ConfigUtils.getOrDefault(config, "postActions", new ArrayList<>());
    this.client = new HttpSolrClient.Builder(config.getString("solr.url")).build();
    this.request = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update", null);
    this.preReplacedActions = new ArrayList<>();
    this.postReplacedActions = new ArrayList<>();
  }

  public SolrConnector(Config config, SolrClient client) {
    this(config);
    this.client = client;
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    Map<String, String> replacement = new HashMap<>();
    replacement.put("runId", runId);
    StrSubstitutor sub = new StrSubstitutor(replacement, "{", "}");
    preReplacedActions = preActions.stream().map(sub::replace).collect(Collectors.toList());
    executeActions(preReplacedActions);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    Map<String, String> replacement = new HashMap<>();
    replacement.put("runId", runId);
    StrSubstitutor sub = new StrSubstitutor(replacement, "{", "}");
    postReplacedActions = postActions.stream().map(sub::replace).collect(Collectors.toList());
    executeActions(postReplacedActions);
  }

  public List<String> getLastExecutedPreActions() {
    return preReplacedActions;
  }

  public List<String> getLastExecutedPostActions() {
    return postReplacedActions;
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