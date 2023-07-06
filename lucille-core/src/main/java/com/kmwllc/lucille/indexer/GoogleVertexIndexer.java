package com.kmwllc.lucille.indexer;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.*;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GoogleVertexIndexer extends Indexer {

    private static final Logger log = LoggerFactory.getLogger(GoogleVertexIndexer.class);

    private final boolean bypass;

    private final UpsertDatapointsRequest.Builder requestBuilder;
    private IndexDatapoint.Builder datapointBuilder = IndexDatapoint.newBuilder();
    private IndexServiceSettings settings;

    public GoogleVertexIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
        super(config, manager, metricsPrefix);
        this.bypass = bypass;

        String projectId = config.getString("googlevertex.projectId");
        String region = config.getString("googlevertex.region");
        String indexId = config.getString("googlevertex.indexId");
        String accessToken = config.getString("googlevertex.accessToken");

        requestBuilder = UpsertDatapointsRequest.newBuilder().setIndex(IndexName.of(projectId, region, indexId).toString());
        try {
            settings = IndexServiceSettings.newBuilder()
                    .setCredentialsProvider(
                            FixedCredentialsProvider.create(GoogleCredentials.create(AccessToken.newBuilder().setTokenValue(accessToken).build())))
                    .setEndpoint("us-west1-aiplatform.googleapis.com:443")
                    .build();
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
    protected void sendToIndex(List<Document> documents) throws IOException {
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

        try (IndexServiceClient client = IndexServiceClient.create(settings)) {
            UpsertDatapointsRequest req = requestBuilder.addAllDatapoints(datapoints).build();
            UpsertDatapointsResponse response = client.upsertDatapoints(req);
        }
    }

    @Override
    public void closeConnection() {
        // no-op
    }
}
