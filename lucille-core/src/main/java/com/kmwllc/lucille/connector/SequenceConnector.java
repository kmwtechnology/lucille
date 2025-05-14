package com.kmwllc.lucille.connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector implementation that produces blank documents given amount to produce
 */
public class SequenceConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(SequenceConnector.class);

  /**
   * Configuration class for {@link SequenceConnector}.
   * <p>
   * Holds parameters for generating a sequence of blank documents, including:
   * <ul>
   *   <li><b>numDocs</b>: The total number of documents to generate.</li>
   *   <li><b>startWith</b>: The starting integer value for document IDs.</li>
   * </ul>
   * <p>
   * Provides methods to apply configuration from a {@link Config} object,
   * validate parameter values, and access the configured values.
   */
  public static class SequenceConnectorConfig extends BaseConnectorConfig {
    /**
     * The total number of documents to generate. Must be greater than or equal to 0.
     * Default value is 100.
     */
    private long numDocs = 100;
    /**
     * The starting integer value for document IDs. Must be greater than or equal to 0.
     * Default value is 0.
     */
    private int startWith = 0;

    /**
     * Applies configuration values from the provided {@link Config} object.
     *
     * @param config the configuration object containing parameters
     */
    public void apply(Config config) {
      super.apply(config);
      numDocs = config.getLong("numDocs");
      startWith = config.getInt("startWith");
    }

    /**
     * Validates the configuration parameters to ensure they are within acceptable ranges.
     *
     * @throws Exception if any parameter is invalid
     */
    public void validate() throws Exception { 
      super.validate();
      if (numDocs < 0) {
        throw new ConnectorException("Invalid value for numDocs. Must be greater than or equal to 0.");
      }
      if (startWith < 0) {
        throw new ConnectorException("Invalid value for startWith. Must be greater than or equal to 0.");
      }
    }

    /**
     * Returns the total number of documents to generate.
     *
     * @return the number of documents
     */
    public long getNumDocs() {
      return numDocs;
    }

    /**
     * Returns the starting integer value for document IDs.
     *
     * @return the starting value for document IDs
     */
    public int getStartWith() {
      return startWith;
    }
  }

  private SequenceConnectorConfig config;

  public SequenceConnector(Config params) throws Exception {
    super(params, Spec.connector()
        .withRequiredProperties("numDocs")
        .withOptionalProperties("startWith"));

    config = new SequenceConnectorConfig();
    config.apply(params);
    config.validate();
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    for (int i = 0; i < config.numDocs; i++) {
      Document doc = Document.create(createDocId(Integer.toString(i + config.startWith)));
      try {
        publisher.publish(doc);
      } catch (Exception e) {
        throw new ConnectorException("Error creating or publishing document", e);
      }
    }
  }
}
