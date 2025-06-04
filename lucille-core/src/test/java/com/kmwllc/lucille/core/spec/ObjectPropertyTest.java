package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class ObjectPropertyTest {

  @Test
  public void testParentValidation() {
    ParentSpec filterOptionsSpec = Spec.parent("filterOptions").withOptionalProperties("includes", "excludes", "lastModifiedCutoff");
    Property parentProperty = new ObjectProperty(filterOptionsSpec, true);

    Config filterOptions = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/filterOptions.conf");
    parentProperty.validate(filterOptions);

    Config badFilterOptions = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ObjectProperty/badFilterOptions.conf");
    assertThrows(IllegalArgumentException.class, () -> parentProperty.validate(badFilterOptions));

    Property fieldParentProperty = new ObjectProperty(filterOptionsSpec, true);
    Config badType = ConfigFactory.parseResourcesAnySyntax("PropertyTest/boolean.conf");
    assertThrows(IllegalArgumentException.class, () -> fieldParentProperty.validate(badType));
  }
}
