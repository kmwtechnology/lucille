package com.kmwllc.lucille.weaviate.indexer;

import static org.junit.Assert.*;

import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Response;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateError;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.misc.model.Meta;
import java.util.List;
import org.junit.Before;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

public class WeaviateIndexerTest {

  private WeaviateClient mockClient;

  @Before
  public void setup() {
    mockClient = Mockito.mock(WeaviateClient.class);

    Mockito.when(mockClient.misc().metaGetter().run()).thenReturn(
        new Result<>(new Response<>(200, new Meta(), null)),
        new Result<>(new Response<>(404, new Meta(), null
            // todo see how to construct a success and error responses
//            new WeaviateError(
//            404, List.of(new WeaviateErrorMessage("error1", new RuntimeException(""))))
        ))
    );

    // todo check this
//    Result<ObjectGetResponse[]> mockResponse = Mockito.<Result<ObjectGetResponse[]>>mock(Result.class);
    Result<ObjectGetResponse[]> mockResponse = Mockito.mock(Result.class);
    Mockito.when(any(ObjectsBatcher.class).run()).thenReturn(mockResponse);
  }

}