package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class OneOfPropertyTest {

  private static final OneOfProperty AZURE_PROP = new OneOfProperty(
    SpecBuilder.withoutDefaults()
          .requiredString("connectionString").build(),
    SpecBuilder.withoutDefaults()
        .requiredString("accountName", "accountKey").build()
  );

  private static final OneOfProperty AZURE_PROP_WITH_NUMBER = new OneOfProperty(
      SpecBuilder.withoutDefaults()
          .requiredString("connectionString")
          .build(),
      SpecBuilder.withoutDefaults()
          .requiredString("accountName", "accountKey")
          .requiredNumber("someNumber")
          .build()
  );

  private static final OneOfProperty AZURE_PROP_CS_ALLOWS_EXTRA = new OneOfProperty(
      SpecBuilder.withoutDefaults()
          .requiredString("connectionString")
          .optionalString("extra")
          .build(),
      SpecBuilder.withoutDefaults()
          .requiredString("accountName", "accountKey")
          .build()
  );

  private static final OneOfProperty AZURE_PROP_TWO_ALLOW_EXTRA = new OneOfProperty(
      SpecBuilder.withoutDefaults()
          .requiredString("connectionString")
          .optionalString("extra")
          .build(),
      SpecBuilder.withoutDefaults()
          .requiredString("accountName", "accountKey")
          .requiredNumber("someNumber")
          .build(),
      SpecBuilder.withoutDefaults()
          .requiredString("somethingElse")
          .optionalString("extra")
          .build()
  );

  @Test
  public void testOneOfValidation() {
    Config goodConnStr = ConfigFactory.parseMap(Map.of(
        "connectionString", "this-is-my-connection-string"
    ));
    AZURE_PROP.validate(goodConnStr);

    Config goodAccount = ConfigFactory.parseMap(Map.of(
        "accountName", "this-is-my-account-name",
        "accountKey", "accountKey"
    ));
    AZURE_PROP.validate(goodAccount);
  }

  @Test
  public void testMultipleAndNonePresent() {
    Config tooMuch = ConfigFactory.parseMap(Map.of(
        "connectionString", "this-is-my-connection-string",
        "accountName", "this-is-my-account-name",
        "accountKey", "accountKey"
    ));

    String message = assertThrows(IllegalArgumentException.class, () -> AZURE_PROP.validate(tooMuch)).getMessage();
    assertTrue(message.contains("multiple were found"));

    String emptyMessage = assertThrows(IllegalArgumentException.class, () -> AZURE_PROP.validate(ConfigFactory.empty())).getMessage();
    assertTrue(emptyMessage.contains("none were found"));
  }

  @Test
  public void testOneOfButMissingProp() {
    Config missingAccountKey = ConfigFactory.parseMap(Map.of(
        "accountName", "this-is-my-account-name"
    ));
    assertThrows(IllegalArgumentException.class, () -> AZURE_PROP.validate(missingAccountKey));
  }

  @Test
  public void testOneOfButBadType() {
    Config badNumberType = ConfigFactory.parseMap(Map.of(
        "accountName", "this-is-my-account-name",
        "accountKey", "this-is-my-key",
        // unfortunately we cannot use the previous spec, since numbers will be parsed as strings w/o problems :)
        "someNumber", "THIS IS NOT A NUMBER"
    ));

    assertThrows(IllegalArgumentException.class, () -> AZURE_PROP_WITH_NUMBER.validate(badNumberType));
  }

  @Test
  public void testOneOfWithOptionals() {
    Config connStrWithExtra = ConfigFactory.parseMap(Map.of(
        "connectionString", "this-is-my-connection-string",
        "extra", "I get to put an extra here!"
    ));

    AZURE_PROP_CS_ALLOWS_EXTRA.validate(connStrWithExtra);
    AZURE_PROP_TWO_ALLOW_EXTRA.validate(connStrWithExtra);

    Config accountWithExtra = ConfigFactory.parseMap(Map.of(
        "extra", "I get to put an extra here!",
        "accountName", "name",
        "accountKey", "key"
    ));

    assertThrows(IllegalArgumentException.class, () -> AZURE_PROP_CS_ALLOWS_EXTRA.validate(accountWithExtra));
    assertThrows(IllegalArgumentException.class, () -> AZURE_PROP_TWO_ALLOW_EXTRA.validate(accountWithExtra));

    Config somethingElseWithExtra = ConfigFactory.parseMap(Map.of(
        "somethingElse", "This allows extra",
        "extra", "this is my extra!"
    ));

    assertThrows(IllegalArgumentException.class, () -> AZURE_PROP_CS_ALLOWS_EXTRA.validate(somethingElseWithExtra));
    // "extra" is optional in both a config using "connectionString" and a config using "somethingElse".
    // this test is ensuring we do not receive errors because this optional property (in both) is present
    // in what is a valid option for our "one of" property.
    // (since we throw errors when optional properties are present when their "oneOf" doesn't allow them,
    // but another one does)
    AZURE_PROP_TWO_ALLOW_EXTRA.validate(somethingElseWithExtra);
  }
}
