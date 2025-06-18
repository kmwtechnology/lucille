package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.ExponentialBackoffRetryHandler;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.StatusCodeResponseInterceptor;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches byte data of a given URL field and places data into a specified field
 * <br>
 * Config Parameters -
 * <br>
 * source (String) : Field name of URL to be fetched; document will be skipped if the field with this name is absent or empty
 * dest (String) : Field name of destination for byte data; document will be skipped if the field with this name is absent or empty
 * size_suffix (String, Optional) : suffix to be appended to end of source field name for the size of data
 *  e.g. url --&gt; url_(size_suffix) where source name is url
 * status_suffix (String, Optional) : suffix to be appended to end of source field name for the status code of the fetch request
 * error_suffix (String, Optional) : suffix to be appended to end of source field name for any errors in process
 * max_size (Integer, Optional) : max size, in bytes, of data to be read from fetch
 * max_retries (Integer, Optional) : max number of tries the request will be made. Defaults to 0 retries.
 * initial_expiry_ms (Integer, Optional) : number of milliseconds that would be waited before retrying the request. Defaults to 100ms.
 * max_expiry_ms (Integer, Optional) : max number of milliseconds that would be waited before retrying a request. Defaults to 10000ms, 10s.
 * connection_request_timeout (Integer, Optional) : the connection request timeout in milliseconds. Defaults to 60000ms, 1m.
 * connect_timeout (Integer, Optional) : the connection timeout in milliseconds. Defaults to 60000ms, 1m.
 * socket_timeout (Integer, Optional) : the socket timeout in milliseconds. Defaults to 60000ms, 1m.
 */
public class FetchUri extends Stage {

  public static Spec SPEC = Spec.stage()
      .reqStr("source", "dest")
      .optStr("size_suffix", "status_suffix", "error_suffix")
      .optNum("max_retries", "initial_expiry_ms", "max_expiry_ms", "connection_request_timeout", "connect_timeout", "socket_timeout", "max_size")
      .optList("status_code_retry_list", new TypeReference<List<String>>(){})
      .optParent("headers", new TypeReference<Map<String, Object>>(){});

  private static final Logger log = LoggerFactory.getLogger(FetchUri.class);

  private final String source;
  private final String dest;
  private final String statusSuffix;
  private final String sizeSuffix;
  private final String errorSuffix;
  private final int maxDownloadSize;
  private final Header[] headers;
  private final int maxNumRetries;
  private final int initialExpiry;
  private final int maxExpiry;
  private final int connectionRequestTimeout;
  private final int connectTimeout;
  private final int socketTimeout;
  private List<String> statusCodeRetryList;

  private CloseableHttpClient client;

  public FetchUri(Config config) {
    super(config);

    this.source = config.getString("source");
    this.dest = config.getString("dest");
    this.statusSuffix = ConfigUtils.getOrDefault(config, "status_suffix", "status_code");
    this.sizeSuffix = ConfigUtils.getOrDefault(config, "size_suffix", "size");
    this.errorSuffix = ConfigUtils.getOrDefault(config, "error_suffix", "error");
    this.maxDownloadSize = ConfigUtils.getOrDefault(config, "max_size", Integer.MAX_VALUE);
    this.headers = ConfigUtils.createHeaderArray(config, "headers");
    this.maxNumRetries = ConfigUtils.getOrDefault(config, "max_retries", 0);
    this.initialExpiry = ConfigUtils.getOrDefault(config, "initial_expiry_ms", 100);
    this.maxExpiry = ConfigUtils.getOrDefault(config, "max_expiry_ms", 10000);
    this.connectionRequestTimeout = ConfigUtils.getOrDefault(config, "connection_request_timeout", 60000);
    this.connectTimeout = ConfigUtils.getOrDefault(config, "connect_timeout", 60000);
    this.socketTimeout = ConfigUtils.getOrDefault(config, "socket_timeout", 60000);
    this.statusCodeRetryList = ConfigUtils.getOrDefault(config, "status_code_retry_list", new ArrayList<String>())
        .stream().map(String::toLowerCase).collect(Collectors.toList());
  }

  // Method exists for testing with mockito mocks
  void setClient(CloseableHttpClient client) {
    this.client = client;
  }

  // Method exists for testing statusCodeRetryList logic
  List<String> getStatusCodeRetryList() {
    return this.statusCodeRetryList;
  }

  @Override
  public void start() throws StageException {
    // do not allow 200, 20x, or 2xx in the status code retry list.
    if (this.statusCodeRetryList.contains("200") || this.statusCodeRetryList.contains("20x") || this.statusCodeRetryList.contains("2xx")) {
      throw new StageException("Do not add 200, 2xx, or 20x to the status_code_retry_list because this is a successful response and "
          + "you will always reach the maximum number of retries (" + this.maxNumRetries + ").");
    }
    // log warning if any status codes in 200-299 are added to the statusCodeRetryList
    if (this.statusCodeRetryList.stream().anyMatch(code -> code.startsWith("2"))) {
      log.warn("The status_code_retry_list contains values that are within 200-299. This may not be a good idea because these statuses "
          + "represent successful responses, but will continue processing.");
    }
    // log warning if list contains values that are not codes 100 to 599 nor the 'x' wildcards.
    List<String> validStatusCodeRetryList = this.statusCodeRetryList.stream().filter(code -> code.matches("[1-5]([xX]{2}|\\d([\\dx]|[\\dX]))")).collect(Collectors.toList());
    if (!validStatusCodeRetryList.equals(this.statusCodeRetryList)) {
      log.warn("The status_code_retry_list contains values that do not represent status codes. They must be with 100 and 599 and "
          + "must not contain any other characters besides 'x' for wildcards. Please remove these values.");
      this.statusCodeRetryList = validStatusCodeRetryList;
    }
    if (this.maxNumRetries == 0 && !this.statusCodeRetryList.isEmpty()) {
      log.warn("You have set a list of status codes to retry on but the number of retries (maxNumRetries) is set to 0. "
          + "Nothing will be retried.");
    }

    // separate status code retry list into wildcards and non-wildcards
    List<String> statusCodeList = new ArrayList<>();
    List<String> statusCodeWildcardList = new ArrayList<>();
    for (String statusCode : this.statusCodeRetryList) {
      if (statusCode.contains("x")) {
        statusCodeWildcardList.add(statusCode.replaceAll("x", ""));
      } else {
        statusCodeList.add(statusCode);
      }
    }

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(this.connectionRequestTimeout)
        .setConnectTimeout(this.connectTimeout)
        .setSocketTimeout(this.socketTimeout)
        .build();

    client = HttpClientBuilder
        .create()
        .setDefaultRequestConfig(requestConfig)
        .setRetryHandler(new ExponentialBackoffRetryHandler(this.maxNumRetries, this.initialExpiry, this.maxExpiry))
        .addInterceptorFirst(new StatusCodeResponseInterceptor(statusCodeList, statusCodeWildcardList))
        .build();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source) || doc.getString(source).isEmpty()) {
      return null;
    }
    String url = doc.getString(source);

    try {
      new URL(url).toURI();
    } catch (URISyntaxException e) {
      setErrorField(doc, e);
      return null;
    } catch (MalformedURLException e) {
      setErrorField(doc, e);
      return null;
    }

    HttpGet httpGet = new HttpGet(url);
    if (this.headers != null) {
      httpGet.setHeaders(this.headers);
    }

    try (CloseableHttpResponse httpResponse = client.execute(httpGet)) {
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      HttpEntity ent = httpResponse.getEntity();
      try (BoundedInputStream boundedContentStream = new BoundedInputStream(ent.getContent(), maxDownloadSize)) {
        byte[] bytes = IOUtils.toByteArray(boundedContentStream);
        long contentSize = bytes.length;

        doc.setField(dest, bytes);
        doc.setField(source + "_" + statusSuffix, statusCode);
        doc.setField(source + "_" + sizeSuffix, contentSize);
      }
    } catch (ClientProtocolException e) {
      setErrorField(doc, e);
    } catch (IOException e) {
      setErrorField(doc, e);
    }
    return null;
  }

  @Override
  public void stop() throws StageException {
    try {
      if (client != null) {
        client.close();
      }
    } catch (IOException e) {
      throw new StageException("Error closing client", e);
    }
  }

  // sets the error field of the doc with the name of the exception and message from exception
  private void setErrorField(Document doc, Exception e) {
    doc.setField(source + "_" + errorSuffix, e.getClass().getCanonicalName() + " " + e.getMessage());
  }

}
