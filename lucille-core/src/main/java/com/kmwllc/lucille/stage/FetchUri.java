package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;
import nl.altindag.ssl.util.UriUtils;
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

  private final String errorSuffix;
  private final int maxDownloadSize;
  private CloseableHttpClient client;

  public FetchUri(Config config) {
    super(config, new StageSpec().withRequiredProperties("source", "dest")
        .withOptionalProperties("size_suffix", "status_suffix", "max_size", "error_suffix"));
    this.source = config.getString("source");
    this.dest = config.getString("dest");
    this.statusSuffix = ConfigUtils.getOrDefault(config, "status_suffix", "status_code");
    this.sizeSuffix = ConfigUtils.getOrDefault(config, "size_suffix", "size");
    this.errorSuffix = ConfigUtils.getOrDefault(config, "error_suffix", "error");
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
    if (!doc.has(source) || doc.getString(source).isEmpty()) {
      return null;
    }
    String uri = doc.getString(source);

    // Following checks for first valid URL and if not, then valid URI, and then if not places error message in error field
    try {
      new URL(uri).toURI();
    } catch (URISyntaxException e) {
      doc.setField(source + "_" + errorSuffix, e.getClass().getCanonicalName() + " " + e.getMessage());
      return null;
    } catch (MalformedURLException e) {
      try {
        UriUtils.validate(URI.create(uri));
      } catch (IllegalArgumentException ex) {
        doc.setField(source + "_" + errorSuffix, e.getClass().getCanonicalName() + " " + e.getMessage());
        return null;
      }
    }

    HttpGet httpGet = new HttpGet(uri);
    try (CloseableHttpResponse httpResponse = client.execute(httpGet)) {
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      HttpEntity ent = httpResponse.getEntity();
      InputStream content = ent.getContent();
      BoundedInputStream boundedContentStream = new BoundedInputStream(content, maxDownloadSize);
      byte[] bytes = IOUtils.toByteArray(boundedContentStream);
      long contentSize = bytes.length;

      doc.setField(dest, bytes);
      doc.setField(source + "_" + statusSuffix, statusCode);
      doc.setField(source + "_" + sizeSuffix, contentSize);

      EntityUtils.consume(ent);
    } catch (ClientProtocolException e) {
      doc.setField(source + "_" + errorSuffix, e.getClass().getCanonicalName() + " " + e.getMessage());
    } catch (IOException e) {
      doc.setField(source + "_" + errorSuffix, e.getClass().getCanonicalName() + " " + e.getMessage());
    }
    return null;
  }

  @Override
  public void stop() throws StageException {
    try {
      if (client == null) {
        client.close();
      }
    } catch (IOException e) {
      throw new StageException("Error closing client", e);
    }
  }

}
