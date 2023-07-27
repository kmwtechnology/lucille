package com.kmwllc.lucille.indexer;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.*;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import io.grpc.StatusRuntimeException;
import org.apache.http.auth.InvalidCredentialsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GoogleVertexIndexer extends Indexer {

    private static final Logger log = LoggerFactory.getLogger(GoogleVertexIndexer.class);

    private static boolean rerunIndex;

    private final boolean bypass;
    private UpsertDatapointsRequest.Builder requestBuilder;
    private IndexDatapoint.Builder datapointBuilder = IndexDatapoint.newBuilder();
    private IndexServiceSettings.Builder settingsBuilder;
    private IndexServiceSettings settings;
    private String accessToken;

    public GoogleVertexIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
        super(config, manager, metricsPrefix);
        this.bypass = bypass;

        String projectId = config.getString("googlevertex.projectId");
        String region = config.getString("googlevertex.region");
        String indexId = config.getString("googlevertex.indexId");

        requestBuilder = UpsertDatapointsRequest.newBuilder().setIndex(IndexName.of(projectId, region, indexId).toString());
        try {
            accessToken = getAccessToken();
            // TODO: Look into configuring the CredentialsProvider to point directly at the service acct key JSON
            settingsBuilder = IndexServiceSettings.newBuilder()
                    .setEndpoint("us-west1-aiplatform.googleapis.com:443")
                    .setCredentialsProvider(
                            FixedCredentialsProvider.create(GoogleCredentials.create(AccessToken.newBuilder().setTokenValue(accessToken).build())));
            settings = settingsBuilder.build();
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    @Override
    public boolean validateConnection() {
        /*try (IndexServiceClient client = IndexServiceClient.create(settings)) {
            UpsertDatapointsRequest request = requestBuilder.addAllDatapoints(new ArrayList<>()).build();
            UpsertDatapointsResponse response = client.upsertDatapoints(request);

            // TODO: Find a way to determine is the request was successful from the response
        } catch (IOException e) {
            return false;
        }*/

        return true;
    }

    @Override
    protected void sendToIndex(List<Document> documents) throws IOException, InterruptedException {
        if (bypass) {
            return;
        }

        List<IndexDatapoint> datapoints = new ArrayList<>();
        for (Document doc : documents) {
            datapointBuilder = datapointBuilder.addAllFeatureVector(doc.getFloatList("values"));
            datapointBuilder = datapointBuilder.setDatapointId(doc.getString("id"));
            datapoints.add(datapointBuilder.build());
            datapointBuilder = datapointBuilder.clear();
        }

         do {
            rerunIndex = false;
            try (IndexServiceClient client = IndexServiceClient.create(settings)) {
                UpsertDatapointsRequest req = requestBuilder.addAllDatapoints(datapoints).build();
                requestBuilder = requestBuilder.clearDatapoints();
                UpsertDatapointsResponse response = client.upsertDatapoints(req);
            } catch (Exception e) {
                rerunIndex = true;
                try {
                    System.out.println("Retrieving new access token...");
                    String accessToken = getAccessToken();
                    settingsBuilder = settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(
                            GoogleCredentials.create(AccessToken.newBuilder().setTokenValue(accessToken).build())));
                    settings = settingsBuilder.build();
                } catch (Exception e2) {
                    System.out.println("Failed to retrieve new access token");
                    System.out.println(e2.getMessage() + "\n" + Arrays.toString(e2.getStackTrace()));
                    rerunIndex = false;
                }
            }
        } while (rerunIndex);
    }

    @Override
    public void closeConnection() {
        // no-op
    }

    private String getAccessToken() throws IOException {
        // String filename = "current-token.txt";
        // File f = File.createTempFile(filename, ".txt");
        // ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(f);

        // Process getToken = builder.start();

        String command = "gcloud auth print-access-token";
        Process getToken = Runtime.getRuntime().exec(command);

        StringBuilder sb = new StringBuilder();
        try (BufferedReader in =
                     new BufferedReader(new InputStreamReader(getToken.getInputStream()))) {
            in.lines().forEach(sb::append);
        }

        return sb.toString();
    }
}
