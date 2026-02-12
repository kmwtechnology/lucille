package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import java.util.List;
import org.junit.Test;

public class SpecBuilderTest {

  @Test
  public void testInclude() {
    Spec spec1 = SpecBuilder.withoutDefaults()
        .optionalString("fetcherClass")
        .build();

    Spec spec2 = SpecBuilder.stage()
        .optionalString("textField")
        .optionalList("metadataWhitelist", new TypeReference<List<String>>() {})
        .optionalList("metadataBlacklist", new TypeReference<List<String>>() {})
        .optionalNumber("textContentLimit")
        .optionalParent(FileConnector.AZURE_PARENT_SPEC)
        .include(spec1).build();

    assertTrue(spec1.getLegalProperties().contains("fetcherClass"));

    assertTrue(spec2.getLegalProperties().contains("fetcherClass"));
    assertTrue(spec2.getLegalProperties().contains("textField"));
    assertTrue(spec2.getLegalProperties().contains("metadataWhitelist"));
    assertTrue(spec2.getLegalProperties().contains("metadataBlacklist"));
    assertTrue(spec2.getLegalProperties().contains("textContentLimit"));
    assertTrue(spec2.getLegalProperties().contains("azure"));

    Spec spec3 = SpecBuilder.withoutDefaults()
        .optionalString("textField").build();

    assertFalse(spec3.getLegalProperties().contains("fetcherClass"));
  }

  @Test
  public void testMultipleIncludes() {
    Spec spec1 = SpecBuilder.withoutDefaults()
        .optionalString("field1").build();

    Spec spec2 = SpecBuilder.withoutDefaults()
        .optionalString("field2")
        .optionalNumber("field3").build();

    Spec spec3 = SpecBuilder.withoutDefaults()
        .optionalBoolean("field4").build();

    Spec combinedSpec = SpecBuilder.stage()
        .optionalString("baseField")
        .include(spec1)
        .include(spec2)
        .include(spec2)
        .include(spec1)
        .include(spec3).build();

    assertTrue(combinedSpec.getLegalProperties().contains("baseField"));
    assertTrue(combinedSpec.getLegalProperties().contains("field1"));
    assertTrue(combinedSpec.getLegalProperties().contains("field2"));
    assertTrue(combinedSpec.getLegalProperties().contains("field3"));
    assertTrue(combinedSpec.getLegalProperties().contains("field4"));

    assertEquals(9, combinedSpec.getLegalProperties().size());
  }
}