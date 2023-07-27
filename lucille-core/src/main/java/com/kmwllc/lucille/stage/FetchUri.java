package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import java.util.Map.Entry;
import java.util.Scanner;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class FetchUri extends Stage {

  private final Map<String, Object> fieldMap;
  private CloseableHttpClient client;

  public FetchUri(Config config) {
    super(config, new StageSpec().withRequiredParents("fieldMapping"));
    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, Object> e : fieldMap.entrySet()) {
      final HttpGet httpGet = new HttpGet(doc.getString(e.getKey()));
      try (CloseableHttpClient client = HttpClients.createDefault();) {
        HttpResponse httpResponse = client.execute(httpGet);
        byte[] bytes = IOUtils.toByteArray(httpResponse.getEntity().getContent());
        doc.setField((String) e.getValue(), bytes);
      } catch (ClientProtocolException ex) {
        throw new RuntimeException(ex);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return null;
  }
}
