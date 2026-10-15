package com.kmwllc.lucille.auth;

import io.dropwizard.auth.AuthDynamicFeature;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies an authentication filter to every JAX-RS resource method, so that the same authentication policy is
 * applied to the whole API.
 * <p>
 * Jersey calls {@link #configure(ResourceInfo, FeatureContext)} once per resource method at startup. The
 * filter is applied to that method unless the method, or the class declaring it, is annotated with
 * {@link AuthNotRequired}, or the class belongs to an exempt package.
 * <p>
 * Only the per-method configuration of {@link AuthDynamicFeature} is overridden. Its
 * {@link AuthDynamicFeature#configure(FeatureContext)} is inherited unchanged, and supplies the
 * authentication filter with the injection manager it uses to record the authenticated user in Jetty's
 * request log.
 * @see AuthNotRequired
 */
public class RequireAuthDynamicFeature extends AuthDynamicFeature {

  /**
   * Logger for the RequireAuthDynamicFeature.
   */
  private static final Logger log = LoggerFactory.getLogger(RequireAuthDynamicFeature.class);

  /**
   * The authentication filter applied to resource methods that are not exempt.
   */
  private final ContainerRequestFilter authFilter;

  /**
   * Packages whose resource methods are exempt from authentication, subpackages included.
   */
  private final Set<String> exemptPackages;

  /**
   * Constructs a RequireAuthDynamicFeature where the only endpoints reachable without authentication are
   * those annotated with {@link AuthNotRequired}.
   * @param authFilter the authentication filter to apply
   */
  public RequireAuthDynamicFeature(ContainerRequestFilter authFilter) {
    this(authFilter, Set.of());
  }

  /**
   * Constructs a RequireAuthDynamicFeature.
   * @param authFilter the authentication filter to apply
   * @param exemptPackages packages whose resource methods should be reachable without authentication,
   *     subpackages included. Intended for resource classes that cannot be annotated, such as those
   *     registered by the Swagger bundle.
   */
  public RequireAuthDynamicFeature(ContainerRequestFilter authFilter, Set<String> exemptPackages) {
    super(authFilter);
    this.authFilter = Objects.requireNonNull(authFilter, "An authentication filter is required.");
    this.exemptPackages = Set.copyOf(exemptPackages);
  }

  /**
   * Applies the authentication filter to the given resource method, unless the method is exempt.
   * @param resourceInfo the resource class and method being configured
   * @param context the per-method configuration the filter is registered into
   */
  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    String description = describe(resourceInfo);

    if (isExempt(resourceInfo)) {
      log.info("Authentication is not required for {}.", description);
      return;
    }

    log.info("Requiring authentication for {}.", description);
    context.register(authFilter);
  }

  /**
   * Determines whether the given resource method is exempt from authentication.
   * @param resourceInfo the resource class and method being configured
   * @return true if the method, or its class, is annotated with {@link AuthNotRequired}, or the class belongs
   *     to an exempt package
   */
  private boolean isExempt(ResourceInfo resourceInfo) {
    Method method = resourceInfo.getResourceMethod();
    Class<?> resourceClass = resourceInfo.getResourceClass();

    return method.isAnnotationPresent(AuthNotRequired.class)
        || resourceClass.isAnnotationPresent(AuthNotRequired.class)
        || isInExemptPackage(resourceClass);
  }

  /**
   * Determines whether the given resource class belongs to an exempt package or one of its subpackages.
   * @param resourceClass the resource class to check
   * @return true if the class belongs to an exempt package
   */
  private boolean isInExemptPackage(Class<?> resourceClass) {
    String resourcePackage = resourceClass.getPackageName();

    return exemptPackages.stream()
        .anyMatch(exempt -> resourcePackage.equals(exempt) || resourcePackage.startsWith(exempt + "."));
  }

  /**
   * Describes a resource method for logging.
   * @param resourceInfo the resource class and method being configured
   * @return the resource class and method name, such as LivenessResource#isAlive
   */
  private String describe(ResourceInfo resourceInfo) {
    return resourceInfo.getResourceClass().getSimpleName() + "#" + resourceInfo.getResourceMethod().getName();
  }
}
