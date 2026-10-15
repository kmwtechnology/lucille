package auth;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.kmwllc.lucille.auth.AuthNotRequired;
import com.kmwllc.lucille.auth.RequireAuthDynamicFeature;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import java.lang.reflect.Method;
import java.util.Set;
import org.junit.Test;

public class RequireAuthDynamicFeatureTest {

  private final ContainerRequestFilter authFilter = mock(ContainerRequestFilter.class);
  private final FeatureContext context = mock(FeatureContext.class);

  // A resource declaring one method that should be authenticated, and one that should not.
  public static class SampleResource {

    public void getAll() {}

    @AuthNotRequired
    public void ping() {}
  }

  // A resource whose methods should all be reachable without authentication.
  @AuthNotRequired
  public static class SampleExemptResource {

    public void ping() {}
  }

  @Test
  public void testUnannotatedMethodIsAuthenticated() {
    new RequireAuthDynamicFeature(authFilter).configure(resourceInfo(SampleResource.class, "getAll"), context);

    verify(context).register(authFilter);
  }

  @Test
  public void testAnnotatedMethodIsNotAuthenticated() {
    new RequireAuthDynamicFeature(authFilter).configure(resourceInfo(SampleResource.class, "ping"), context);

    verifyNoInteractions(context);
  }

  @Test
  public void testAnnotatedClassIsNotAuthenticated() {
    new RequireAuthDynamicFeature(authFilter).configure(resourceInfo(SampleExemptResource.class, "ping"), context);

    verifyNoInteractions(context);
  }

  @Test
  public void testExemptPackageIsNotAuthenticated() {
    // PrincipalImpl is declared in io.dropwizard.auth
    new RequireAuthDynamicFeature(authFilter, Set.of("io.dropwizard.auth"))
        .configure(resourceInfo(PrincipalImpl.class, "getName"), context);

    verifyNoInteractions(context);
  }

  @Test
  public void testSubpackageOfExemptPackageIsNotAuthenticated() {
    new RequireAuthDynamicFeature(authFilter, Set.of("io.dropwizard"))
        .configure(resourceInfo(PrincipalImpl.class, "getName"), context);

    verifyNoInteractions(context);
  }

  @Test
  public void testPackageThatMerelyStartsWithAnExemptPackageIsAuthenticated() {
    // io.drop is a prefix of io.dropwizard.auth, but it is not one of its parent packages
    new RequireAuthDynamicFeature(authFilter, Set.of("io.drop"))
        .configure(resourceInfo(PrincipalImpl.class, "getName"), context);

    verify(context).register(authFilter);
  }

  @Test
  public void testAuthFilterIsRequired() {
    assertThrows(NullPointerException.class, () -> new RequireAuthDynamicFeature(null));
  }

  // Builds the ResourceInfo that Jersey would supply for the named method of the given resource class.
  private ResourceInfo resourceInfo(Class<?> resourceClass, String methodName) {
    Method method;

    try {
      method = resourceClass.getMethod(methodName);
    } catch (NoSuchMethodException e) {
      throw new AssertionError(resourceClass.getName() + " does not declare " + methodName + ".", e);
    }

    return new ResourceInfo() {
      @Override
      public Method getResourceMethod() {
        return method;
      }

      @Override
      public Class<?> getResourceClass() {
        return resourceClass;
      }
    };
  }
}
