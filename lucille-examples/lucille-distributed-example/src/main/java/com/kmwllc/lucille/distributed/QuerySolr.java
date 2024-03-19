package com.kmwllc.lucille.distributed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.MapSolrParams;

class QuerySolr {

  public static void main(String[] args) throws Exception {
    String zkHost = "solr:9983";
    SolrClient client = new CloudHttp2SolrClient.Builder(List.of(zkHost), Optional.empty()).withDefaultCollection("quickstart").build();
    Map<String, String> queryParamMap = new HashMap<String, String>();
    queryParamMap.put("q", "*:*");

    var x = 10;
    if ((x = 6) > 10) {

    }

    MapSolrParams params = new MapSolrParams(queryParamMap);
    QueryResponse response = client.query(params);
    new File("output").mkdirs();
    new File("output/dest.json").delete();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter("output/dest.json", true))) {
      for (SolrDocument doc : response.getResults()) {
        writer.write(doc.jsonStr());
      }
    }
  }
}
