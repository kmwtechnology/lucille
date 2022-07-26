package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.util.SolrUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Connector for issuing requests to Solr. Requests should be formatted as JSON Strings and can contain
 * the {runId} wildcard, which will be substituted for the current runId before the requests are issued.
 * This is the only wildcard supported. XML can be used instead of JSON with the useXml=true parameter.
 *
 * Connector Parameters:
 *
 *   - preActions (List<String>, Optional) : A list of requests to be issued to Solr. These actions will be performed first.
 *   - postActions (List<String>, Optional) : A list of requests to be issued to Solr. These actions will be performed second.
 *   - solr.url (String) : The url of the Solr instance for this Connector to issue its requests to.
 *   - useXml (boolean, Optional) : indicates whether actions are in xml or json format; defaults to json; note that
 *     Solr does more validation on json commands than xml ones (e.g. it rejects unrecognized JSON commands but
 *     accepts unrecognized XML commands and reports success); therefore, json is preferable
 */
// TODO : Honor and return children documents
public class SolrConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrClient client;
  private final GenericSolrRequest request;
  private List<String> replacedPreActions;
  private List<String> replacedPostActions;

  private List<String> postActions;
  private List<String> preActions;

  private final Map<String, List<String>> solrParams;
  private final String idField;
  private final String actionFormat;

  public SolrConnector(Config config) {
    super(config);
    this.preActions = ConfigUtils.getOrDefault(config, "preActions", new ArrayList<>());
    this.postActions = ConfigUtils.getOrDefault(config, "postActions", new ArrayList<>());
    this.actionFormat = config.hasPath("useXml") && config.getBoolean("useXml") ? "text/xml" : "text/json";
    this.client = SolrUtils.getSolrClient(config);

    this.request = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update", null);
    this.solrParams = new HashMap<>();

    // These parameters should only be set when a pipeline is also supplied
    Set<Map.Entry<String, ConfigValue>> paramSet = config.hasPath("pipeline") ? config.getConfig("solrParams").entrySet() : new HashSet<>();
    this.idField = config.hasPath("pipeline") ? config.getString("idField") : Document.ID_FIELD;
    this.replacedPreActions = new ArrayList<>();
    this.replacedPostActions = new ArrayList<>();

    for (Map.Entry<String, ConfigValue> e : paramSet) {
      Object rawValues = e.getValue().unwrapped();
      if (e.getValue().valueType().equals(ConfigValueType.LIST)) {
        this.solrParams.put(e.getKey(), (List<String>) rawValues);
      } else if (e.getValue().valueType().equals(ConfigValueType.STRING)) {
        this.solrParams.put(e.getKey(), Collections.singletonList((String) rawValues));
      } else if (e.getValue().valueType().equals(ConfigValueType.NUMBER)) {
        this.solrParams.put(e.getKey(), Collections.singletonList(String.valueOf(rawValues)));
      }
    }
  }

  public SolrConnector(Config config, SolrClient client) {
    this(config);
    this.client = client;
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    replacedPreActions =
      preActions.stream().map(x->x.replaceAll("\\{runId\\}", runId)).collect(Collectors.toList());
    executeActions(replacedPreActions);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    // FUTURE : Query output mode: Feeding documents or iterating facet buckets and send those
    if (getPipelineName() == null) {
      return;
    }

    SolrQuery q = new SolrQuery();
    for (Map.Entry<String, List<String>> e : solrParams.entrySet()) {
      String[] vals = e.getValue().toArray(new String[0]);
      q.add(e.getKey(), vals);
    }
    q.add("sort",  idField + " asc");
    q.set("cursorMark", "\\*");

    QueryResponse resp;
    try {
      resp = client.query(q);
    } catch (Exception e) {
      throw new ConnectorException("Unable to query Solr.", e);
    }

    while (true) {
      for (SolrDocument solrDoc : resp.getResults()) {
        String id = createDocId((String) solrDoc.get(idField));
        JsonDocument doc = new JsonDocument(id);

        for (String fieldName : solrDoc.getFieldNames()) {
          // TODO : we might want an option to preserve the id under its original field name
          // TODO : Add configurable field blacklist (not necessarily here)
          fieldName = fieldName.toLowerCase();
          if (fieldName.equals(idField) || fieldName.equals(Document.ID_FIELD)) {
            continue;
          }

          doc.update(fieldName, UpdateMode.DEFAULT, solrDoc.getFieldValues(fieldName).toArray(new String[0]));
        }

        try {
          publisher.publish(doc);
        } catch (Exception e) {
          throw new ConnectorException("Unable to publish document", e);
        }
      }

      if (q.get("cursorMark").equals(resp.getNextCursorMark())) {
        break;
      }

      q.set("cursorMark", resp.getNextCursorMark());
      try {
        resp = client.query(q);
      } catch (Exception e) {
        throw new ConnectorException("Unable to query Solr", e);
      }
    }
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    replacedPostActions =
      postActions.stream().map(x->x.replaceAll("\\{runId\\}", runId)).collect(Collectors.toList());
    executeActions(replacedPostActions);
  }

  @Override
  public void close() throws ConnectorException {
    try {
      client.close();
    } catch (Exception e) {
      throw new ConnectorException("Unable to close the Solr Client", e);
    }
  }

  public List<String> getLastExecutedPreActions() {
    return replacedPreActions;
  }

  public List<String> getLastExecutedPostActions() {
    return replacedPostActions;
  }

  private void executeActions(List<String> actions) throws ConnectorException {
    for (String action : actions) {
      RequestWriter.ContentWriter contentWriter = new RequestWriter.StringPayloadContentWriter(action, actionFormat);
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