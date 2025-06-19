package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ObjectPropertyTest {

  @Test
  public void testParentValidation() {
    ParentSpec filterOptionsSpec = Spec.parent("filterOptions")
        .optionalList("includes", new TypeReference<List<String>>(){})
        .optionalList("excludes", new TypeReference<List<String>>(){})
        .optionalString("lastModifiedCutoff");

    Property parentProperty = new ObjectProperty(filterOptionsSpec, true);

    Config filterOptions = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/filterOptions.conf");
    parentProperty.validate(filterOptions);

    Config badFilterOptions = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/badFilterOptions.conf");
    assertThrows(IllegalArgumentException.class, () -> parentProperty.validate(badFilterOptions));

    Property fieldParentProperty = new ObjectProperty(filterOptionsSpec, true);
    Config badType = ConfigFactory.parseResourcesAnySyntax("PropertyTest/boolean.conf");
    assertThrows(IllegalArgumentException.class, () -> fieldParentProperty.validate(badType));
  }

  @Test
  public void testTypeReferenceValidation() {
    Config mapStringToListString = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/mapStringToListString.conf");
    ObjectProperty mapStringToListStringProperty = new ObjectProperty("fieldMappings", true, new TypeReference<Map<String, List<String>>>(){});

    // should not throw an exception
    mapStringToListStringProperty.validate(mapStringToListString);

    // fields are mapped to a variety of objects
    Config mapStringToObject = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/mapStringToObject.conf");
    ObjectProperty mapStringToObjectProperty = new ObjectProperty("fieldMappings", true, new TypeReference<Map<String, Object>>(){});
    mapStringToObjectProperty.validate(mapStringToObject);

    // should not throw an exception
    mapStringToListStringProperty.validate(mapStringToListString);

    // While we suggest you use a Spec if you know your field names, will still make sure this works.
    Config bookConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/book.conf");
    ObjectProperty bookProperty = new ObjectProperty("book", true, new TypeReference<BookItem>(){});
    bookProperty.validate(bookConfig);
  }

  @Test
  public void testTypeJson() {
    // 1. an ObjectProperty w/ a ParentSpec
    ParentSpec filterOptionsSpec = Spec.parent("filterOptions")
        .optionalList("includes", new TypeReference<List<String>>(){})
        .optionalList("excludes", new TypeReference<List<String>>(){})
        .optionalString("lastModifiedCutoff");

    Property parentProperty = new ObjectProperty(filterOptionsSpec, true);

    JsonNode typeNode = parentProperty.typeJson();
    assertEquals("OBJECT", typeNode.get("type").asText());
    assertEquals(filterOptionsSpec.toJson(), typeNode.get("child"));

    // 2. an ObjectProperty w/ a TypeReference
    parentProperty = new ObjectProperty("fieldListMapping", true, new TypeReference<Map<String, List<String>>>(){});

    typeNode = parentProperty.typeJson();
    assertEquals("OBJECT", typeNode.get("type").asText());
    assertEquals("java.util.Map<java.lang.String, java.util.List<java.lang.String>>", typeNode.get("typeReference").asText());
  }

  private static class BookItem {
    public String title;
    public String author;
  }
}
