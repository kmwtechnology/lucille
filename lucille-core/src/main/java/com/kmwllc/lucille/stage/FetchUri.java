package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class FetchUri extends Stage {

  private final String source;
  private final String dest;
  private final String statusSuffix;
  private final String sizeSuffix;
  private final int maxDownloadSize;
  private CloseableHttpClient client;

  public FetchUri(Config config) {
    super(config, new StageSpec().withRequiredProperties("source", "dest").withOptionalProperties("size_suffix", "status_suffix", "max_size"));

    this.source = config.getString("source");
    this.dest = config.getString("dest");
    this.statusSuffix = ConfigUtils.getOrDefault(config, "status_suffix", "status_code");
    this.sizeSuffix = ConfigUtils.getOrDefault(config, "size_suffix", "size");
    this.maxDownloadSize = ConfigUtils.getOrDefault(config, "max_size", Integer.MAX_VALUE);
  }

  void setClient(CloseableHttpClient client) {
    this.client = client;
  }

  @Override
  public void start() throws StageException {
    client = HttpClients.createDefault();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String uri = doc.getString(this.source);

    HttpGet httpGet = new HttpGet(uri);
    try (CloseableHttpResponse httpResponse = this.client.execute(httpGet)) {
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      HttpEntity ent = httpResponse.getEntity();
      InputStream content = ent.getContent();
      BoundedInputStream boundedContentStream = new BoundedInputStream(content, maxDownloadSize);
      byte[] bytes = IOUtils.toByteArray(boundedContentStream);
      long contentSize = bytes.length; // is it okay to be a long? Does it need to be an int?

      doc.setField(this.dest, bytes);
      doc.setField(this.source + "_" + statusSuffix, statusCode);
      doc.setField(this.source + "_" + sizeSuffix, contentSize);

      EntityUtils.consume(ent);
    } catch (ClientProtocolException ex) {
      doc.setField(this.source + "_error", ex.getMessage());
    } catch (IOException ex) {
      doc.setField(this.source + "_error", ex.getMessage());
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
