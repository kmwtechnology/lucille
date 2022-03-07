package com.kmwllc.lucille.stage.EE;

import com.kmwllc.lucille.util.Range;

import java.util.Objects;

/**
 * Simple POJO representing a single Entity annotation.  It is composed
 * of the EntityInfo itself and a Range objects indicating where in the
 * original input it was found.
 */
public class EntityAnnotation {
  private final Range range;
  private final EntityInfo entityInfo;

  /**
   * Constructs a new Entity Annotation from the specified range and entity info.
   * @param range Where the Entity occurs
   * @param entityInfo The entity (term and payloads)
   */
  public EntityAnnotation(Range range, EntityInfo entityInfo) {
    this.range = range;
    this.entityInfo = entityInfo;
  }

  /**
   * Gets the Range where the Entity is found in the original inpu
   * @return Range object
   */
  public Range getRange() {
    return range;
  }

  /**
   * Gets the EntityInfo (term + (optional) payloads).
   * @return EntityInfo object
   */
  public EntityInfo getEntityInfo() {
    return entityInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityAnnotation that = (EntityAnnotation) o;
    return Objects.equals(range, that.range) &&
      Objects.equals(entityInfo, that.entityInfo);
  }

  @Override
  public int hashCode() {

    return Objects.hash(range, entityInfo);
  }
}

