import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.AuthConfiguration;
import com.kmwllc.lucille.config.AuthConfiguration.AuthType;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import org.junit.Test;

public class APIApplicationAuthTest {

  @Test
  public void testStartupWithMissingAuthType() {
    DropwizardTestSupport<LucilleAPIConfiguration> support = new DropwizardTestSupport<>(
        APIApplication.class, ResourceHelpers.resourceFilePath("test-conf-missing-auth-type.yml"));

    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, support::before);

    assertTrue(thrown.getMessage(), thrown.getMessage().contains("auth.type"));
  }

  @Test
  public void testTypeNotRequiredWhenAuthDisabled() throws Exception {
    AuthConfiguration authConfig = new AuthConfiguration();
    authConfig.setEnabled(false);

    assertNull(authConfig.getType());

    LucilleAPIConfiguration config = new LucilleAPIConfiguration();
    config.setAuthConfig(authConfig);

    new APIApplication().run(config, new Environment("auth-disabled-test"));
  }

  @Test
  public void testBlankAuthTypeIsNoAuth() {
    AuthConfiguration authConfig = new AuthConfiguration();

    authConfig.setType(null);
    assertEquals(AuthType.NO_AUTH, authConfig.getType());

    authConfig.setType("");
    assertEquals(AuthType.NO_AUTH, authConfig.getType());
  }
}
