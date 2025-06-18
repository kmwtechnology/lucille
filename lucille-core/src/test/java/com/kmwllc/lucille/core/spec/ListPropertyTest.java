package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.Test;

public class ListPropertyTest {

  @Test
  public void testSpecList() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/books.conf");
    Property property = new ListProperty("books", true, Spec.withoutDefaults().requiredString("author", "title"));
    property.validate(config);

    Config boolListConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/boolList.conf");
    assertThrows(IllegalArgumentException.class, () -> property.validate(boolListConfig));
  }

  @Test
  public void testTypeReferences() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/numList.conf");
    Property property = new ListProperty("list", true, new TypeReference<List<Integer>>(){});
    property.validate(config);

    // both Integer and Double should work
    property = new ListProperty("list", true, new TypeReference<List<Double>>(){});
    property.validate(config);

    // or just "Number"
    property = new ListProperty("list", true, new TypeReference<List<Number>>(){});
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/boolList.conf");
    property = new ListProperty("list", true, new TypeReference<List<Boolean>>(){});
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/stringList.conf");
    property = new ListProperty("list", true, new TypeReference<List<String>>(){});
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/configList.conf");
    property = new ListProperty("list", true, new TypeReference<List<Object>>(){});
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listOfList.conf");
    property = new ListProperty("list", true, new TypeReference<List<List<Object>>>(){});
    property.validate(config);

    // not something we recommend you do if you know field names, but will check it is working.
    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/books.conf");
    property = new ListProperty("books", true, new TypeReference<List<BookItem>>(){});
    property.validate(config);
  }

  @Test
  public void testEmptyList() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/emptyList.conf");
    Property property = new ListProperty("list", true, new TypeReference<List<Number>>(){});
    property.validate(config);

    property = new ListProperty("list", true, new TypeReference<List<Boolean>>(){});
    property.validate(config);

    property = new ListProperty("list", true ,new TypeReference<List<List<Object>>>(){});
    property.validate(config);

    property = new ListProperty("list", true, new TypeReference<List<Object>>(){});
    property.validate(config);

    property = new ListProperty("list", true, Spec.withoutDefaults().requiredString("author", "title"));
    property.validate(config);

    property = new ListProperty("list", true, Spec.withoutDefaults());
    property.validate(config);
  }

  // a list of strings that are numbers should be able to be read as numbers, strings that are booleans, etc.
  @Test
  public void testTypeReferenceHOCONConversions() {
    Config stringsThatAreNumbersConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listOfStringsThatAreNumbers.conf");
    Property property = new ListProperty("list", true, new TypeReference<List<Number>>(){});
    property.validate(stringsThatAreNumbersConfig);

    property = new ListProperty("list", true, new TypeReference<List<Double>>(){});
    property.validate(stringsThatAreNumbersConfig);

    // the numbers are doubles and won't serialize to Integers.
    Property integerProperty = new ListProperty("list", true, new TypeReference<List<Integer>>(){});
    assertThrows(IllegalArgumentException.class, () -> integerProperty.validate(stringsThatAreNumbersConfig));

    // should be able to read the list of numbers as a list of strings
    Config numListConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/numList.conf");
    property = new ListProperty("list", true, new TypeReference<List<String>>(){});
    property.validate(numListConfig);
  }

  @Test
  public void testBadTypes() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/boolList.conf");
    Property property = new ListProperty("list", true, new TypeReference<List<Number>>(){});
    assertThrows(IllegalArgumentException.class, () -> property.validate(config));

    Config justStringConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listIsJustAString.conf");
    assertThrows(IllegalArgumentException.class, () -> property.validate(justStringConfig));
  }

  private static class BookItem {
    public String title;
    public String author;
  }
}
