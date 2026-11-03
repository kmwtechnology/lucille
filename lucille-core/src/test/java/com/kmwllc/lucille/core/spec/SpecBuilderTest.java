package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import java.util.ArrayList;
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
        .optionalList("whitelist", new TypeReference<List<String>>() {})
        .optionalList("blacklist", new TypeReference<List<String>>() {})
        .optionalNumber("textContentLimit")
        .optionalParent(FileConnector.AZURE_PARENT_SPEC)
        .include(spec1).build();

    assertTrue(spec1.getLegalProperties().contains("fetcherClass"));

    assertTrue(spec2.getLegalProperties().contains("fetcherClass"));
    assertTrue(spec2.getLegalProperties().contains("textField"));
    assertTrue(spec2.getLegalProperties().contains("whitelist"));
    assertTrue(spec2.getLegalProperties().contains("blacklist"));
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

    assertEquals(10, combinedSpec.getLegalProperties().size());
  }

  @Test
  public void testDeclarationOrder() {
    Spec spec = SpecBuilder.stage()
        .requiredString("zeta")
        .optionalNumber("alpha")
        .optionalBoolean("mu")
        .build();

    assertEquals(List.of("name", "class", "enabled", "conditions", "conditionPolicy", "zeta", "alpha", "mu"),
        List.copyOf(spec.getLegalProperties()));

    List<String> jsonNames = new ArrayList<>();
    spec.toJson().get("fields").forEach(field -> jsonNames.add(field.get("name").asText()));
    assertEquals(List.copyOf(spec.getLegalProperties()), jsonNames);
  }

  @Test
  public void testDuplicateNameIsRejected() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> SpecBuilder.withoutDefaults().optionalString("field").optionalNumber("field"));
    assertTrue(e.getMessage().contains("\"field\""));

    // redeclaring a default property is a duplicate too
    assertThrows(IllegalArgumentException.class, () -> SpecBuilder.stage().optionalString("conditionPolicy"));
    assertThrows(IllegalArgumentException.class, () -> SpecBuilder.fileHandler().optionalString("docIdPrefix"));

    // as is a clash between an included Spec and a declared property
    Spec included = SpecBuilder.withoutDefaults().optionalString("field").build();
    assertThrows(IllegalArgumentException.class, () -> SpecBuilder.withoutDefaults().optionalString("field").include(included));

    // the parent Spec's name is included in the message
    e = assertThrows(IllegalArgumentException.class,
        () -> SpecBuilder.parent("myParent").optionalString("field").optionalString("field"));
    assertTrue(e.getMessage().contains("\"myParent\""));
  }
}