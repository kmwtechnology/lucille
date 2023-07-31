package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import java.util.Map.Entry;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class FetchUri extends Stage {

  private final Map<String, Object> fieldMap;
  private CloseableHttpClient client;

  public FetchUri(Config config) {
    super(config, new StageSpec().withRequiredParents("fieldMapping"));
    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
    client = HttpClients.createDefault();
  }

  void setClient(CloseableHttpClient newClient) {
    this.client = newClient;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, Object> e : fieldMap.entrySet()) {
      String fieldName = e.getKey();
      String destinationFieldName = (String) e.getValue();
      String uri = doc.getString(fieldName);

      HttpGet httpGet = new HttpGet(uri);
      CloseableHttpResponse httpResponse = null;
      try {
        httpResponse = client.execute(httpGet);
        HttpEntity ent = httpResponse.getEntity();
        InputStream content = ent.getContent();
        byte[] bytes = IOUtils.toByteArray(content);
        doc.setField(destinationFieldName, bytes);
        EntityUtils.consume(ent);
      } catch (ClientProtocolException ex) {
        throw new StageException("Error processing GET", ex);
      } catch (IOException ex) {
        throw new StageException("Error processing GET", ex);
      } finally {
        try {
          httpResponse.close();
        } catch (IOException ex) {
          throw new StageException("Error processing GET", ex);
        }
      }
    }

    return null;
  }

  @Override
  public void stop() throws StageException {
    try {
      client.close();
    } catch (IOException e) {
      throw new StageException("Error closing client", e);
    }
  }

}
