import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import org.junit.ClassRule;
import org.junit.Test;

public class APIPreventConcurrentIntegrationTest {

  // This is the config used in "RunnerManager.testNonTrivialSimultaneousRuns". It takes roughly 1 second.
  // A sleep connector would be nice, but we do not have access to it in this module!
  private static final String IMDB_JSON = """
  {
    "connectors": [
      {
        "class": "com.kmwllc.lucille.connector.CSVConnector",
        "name": "imdb-connector",
        "pipeline": "imdb-pipeline",
        "path": "classpath:APIPreventConcurrentIntegrationTest/imdb.csv"
      }
    ],
    "pipelines": [
      {
        "name": "imdb-pipeline",
        "stages": [
          {
            "name": "deleteFields",
            "class": "com.kmwllc.lucille.stage.DeleteFields",
            "fields": [
              "production_countries",
              "spoken_languages",
              "original_language",
              "original_title",
              "imdb_id",
              "status"
            ]
          },
          {
            "name": "renameFields",
            "class": "com.kmwllc.lucille.stage.RenameFields",
            "fieldMapping": {
              "title": "movie_title",
              "vote_average": "average_vote",
              "vote_count": "num_votes",
              "runtime": "runtime_in_min",
              "adult": "is_adult"
            }
          },
          {
            "name": "replacePatterns",
            "class": "com.kmwllc.lucille.stage.ReplacePatterns",
            "source": [
              "overview",
              "genres",
              "keywords",
              "tagline"
            ],
            "dest": [
              "replaced_overview",
              "replaced_genres",
              "replaced_keywords",
              "replaced_tagline"
            ],
            "regex": [
              "and",
              "villain",
              "then",
              "where",
              "who",
              "of",
              "a"
            ],
            "replacement": "REPLACEMENT"
          }
        ]
      }
    ],
    "indexer": {
      "type": "CSV",
      "batchSize": 1,
      "batchTimeout": 1000,
      "logRate": 1000
    },
    "csv": {
      "columns": [
        "replaced_overview",
        "replaced_genres",
        "replaced_keywords",
        "replaced_tagline"
      ],
      "path": "output1.csv",
      "append": false,
      "includeHeader": false
    }
  }
  """;

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(
      APIApplication.class, ResourceHelpers.resourceFilePath("test-conf-prevent-concurrent.yml"));

  private final Client client = RULE.client();
  private final String url = String.format("http://localhost:%d/", RULE.getLocalPort());
  private final String authHeader =
      "Basic " + Base64.getEncoder().encodeToString("admin:password".getBytes());

  @Test
  public void testPreventConcurrentRuns() throws Exception {
    Response configStatus1 = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity(IMDB_JSON, MediaType.APPLICATION_JSON));
    String configResponse1 = configStatus1.readEntity(String.class);
    // same retrieval as in APIIntegrationTest
    String configId1 = configResponse1.substring(13, configResponse1.length() - 2);

    Response configStatus2 = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity(IMDB_JSON, MediaType.APPLICATION_JSON));
    String configResponse2 = configStatus2.readEntity(String.class);
    // same retrieval as in APIIntegrationTest
    String configId2 = configResponse2.substring(13, configResponse2.length() - 2);

    // There should be enough room to prevent race conditions here.
    Response runPostStatus = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{\"configId\": \"" + configId1 + "\"}", MediaType.APPLICATION_JSON));
    assertEquals(200, runPostStatus.getStatus());
    System.out.println("ENTITY 1: " + runPostStatus.readEntity(String.class));

    // identical config, but referenced by a different ID, so it can run.
    Response runPostStatus2 = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{\"configId\": \"" + configId2 + "\"}", MediaType.APPLICATION_JSON));
    assertEquals(200, runPostStatus2.getStatus());

    Response rejectedStatus = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{\"configId\": \"" + configId1 + "\"}", MediaType.APPLICATION_JSON));
    System.out.println("ENTITY 2: " + rejectedStatus.readEntity(String.class));
    assertEquals(400, rejectedStatus.getStatus());
  }
}
