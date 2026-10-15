package com.kmwllc.lucille.auth;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a JAX-RS resource class or method as intentionally reachable without authentication.
 * <p>
 * This is the only way to opt out of the authentication that {@link RequireAuthDynamicFeature} applies to every
 * resource method. Applying it to a class exempts every resource method that class declares.
 * </p>
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface AuthNotRequired {

}
