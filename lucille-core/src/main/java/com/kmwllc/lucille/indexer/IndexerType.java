package com.kmwllc.lucille.indexer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the {@code indexer.type} name an {@link com.kmwllc.lucille.core.Indexer} implementation registers (for
 * example {@code "Solr"}), so the implementation can be selected by {@code indexer.type} without {@code lucille-core}
 * referencing the class directly.
 *
 * <p>Registration is automatic: annotate the Indexer with {@code @IndexerType} <b>and</b> with
 * {@code @com.google.auto.service.AutoService(Indexer.class)}. Google AutoService finds the {@code @AutoService} class
 * at compile time and generates that module's {@code META-INF/services/com.kmwllc.lucille.core.Indexer} service file,
 * so no service file is written or maintained by hand. At runtime {@link IndexerFactory} reads the registered class
 * names from those service files on the classpath, loads each class without instantiating it, reads this annotation's
 * {@link #value()}, and matches it case-insensitively against the configured {@code indexer.type}.
 *
 * <p>Retention is {@link RetentionPolicy#RUNTIME} because {@code IndexerFactory} reads the type name from this
 * annotation at runtime; it is the single source of truth for the name (there is no separate field).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IndexerType {

  /**
   * @return the {@code indexer.type} name this Indexer registers (e.g. {@code "Solr"}).
   */
  String value();
}
