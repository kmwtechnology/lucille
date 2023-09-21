package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
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


/**
 * Fetches byte data of a given URL field and places data into a specified field
 * <br>
 * Config Parameters -
 * <br>
 * inputDataPath (String, Optional) : file path to a text file that stores datapoints to be randomly placed into field,
 *  defaults to numeric data based on cardinality
 * fieldName (String, Optional) : Field name of field where data is placed, defaults to "data"
 * cardinality (int, Optional) : size of the subset of datapoints to be grabbed either from
 *  given datapath or from random numbers
 * minNumOfTerms (Integer, Optional) : minimum number of terms to be in the field, defaults to null
 * maxNumOfTerms (Integer, Optional) : maximum number of terms to be in the field, defaults to null
 * fieldType (FieldType, Optional) : setting for type of field, default or nested, allows for further settings to be easily added
 */
public class AddRandomField extends Stage {

  enum FieldType {
    DEFAULT, NESTED
  }

  private final String inputDataPath;
  private final String fieldName;
  private final int cardinality;
  private final Integer minNumOfTerms;
  private final Integer maxNumOfTerms;
  private final FieldType fieldtype;

  public AddRandomField(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("input_data_path", "field_name", "cardinality", "min_num_of_terms",
        "max_num_of_terms", "field_type"));
    this.inputDataPath = ConfigUtils.getOrDefault(config, "input_data_path", null);
    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.cardinality = ConfigUtils.getOrDefault(config, "cardinality", null);
    this.minNumOfTerms = ConfigUtils.getOrDefault(config, "min_num_of_terms", null);
    this.maxNumOfTerms = ConfigUtils.getOrDefault(config, "max_num_of_terms", null);
    this.fieldtype = FieldType.valueOf(ConfigUtils.getOrDefault(config, "field_type", "default"));

    if (this.minNumOfTerms == null ^ this.maxNumOfTerms == null) {
      throw new StageException("Both minimum and maximum number of terms must be specified");
    }
    if (this.minNumOfTerms > this.maxNumOfTerms) {
      throw new StageException("Minimum number of terms must be less than or equal to maximum");
    }
  }

  // Method exists for testing with mockito mocks
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
    String url = doc.getString(source);

    // Following checks for valid URL
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

    try (CloseableHttpResponse httpResponse = client.execute(httpGet)) {
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      HttpEntity ent = httpResponse.getEntity();
      try (BoundedInputStream boundedContentStream = new BoundedInputStream(ent.getContent(), maxDownloadSize);) {
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
