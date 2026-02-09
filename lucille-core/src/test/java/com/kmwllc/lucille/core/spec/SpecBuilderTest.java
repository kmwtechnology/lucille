package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.util.FileContentFetcher;
import java.util.List;
import org.junit.Test;

public class SpecBuilderTest {

  @Test
  public void testInclude() {
    Spec testSpec1 = SpecBuilder.withoutDefaults()
        .optionalString("fetcherClass")
        .build();

    Spec testSpec2 = SpecBuilder.stage()
        .optionalString("textField")
        .optionalList("metadataWhitelist", new TypeReference<List<String>>() {
        })
        .optionalList("metadataBlacklist", new TypeReference<List<String>>() {
        })
        .optionalNumber("textContentLimit")
        .optionalParent(FileConnector.AZURE_PARENT_SPEC)
        .include(testSpec1).build();

    assertTrue(testSpec1.getLegalProperties().contains("fetcherClass"));

    assertTrue(testSpec2.getLegalProperties().contains("fetcherClass"));
    assertTrue(testSpec2.getLegalProperties().contains("textField"));
    assertTrue(testSpec2.getLegalProperties().contains("textContentLimit"));
    assertTrue(testSpec2.getLegalProperties().contains("azure"));

    Spec testSpec3 = SpecBuilder.withoutDefaults()
        .optionalString("textField")
        .build();

    assertFalse(testSpec3.getLegalProperties().contains("fetcherClass"));

  }
}
