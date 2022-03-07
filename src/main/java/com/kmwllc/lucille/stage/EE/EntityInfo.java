package com.kmwllc.lucille.stage.EE;

import java.util.List;

/**
 * A POJO holding information about an extracted Entity.
 * The EntityInfo object holds the term which matched and any payloads associated
 * with the matching term.  EntityInfo's are defined as equal if they have the same
 * term and payloads.  Thus, an instance of EntityInfo may be reliably used in Sets
 * or as a hash key
 * <p/>
 * Created by matt on 4/19/17.
 */
public class EntityInfo {
  private final String term;
  private final List<String> payloads;

  /**
   * Constructs a new EntityInfo from the specified term and payloads
   * @param term Term that matched
   * @param payloads Payloads associated with the matching term
   */
  public EntityInfo(String term, List<String> payloads) {
    this.term = term;
    this.payloads = payloads;
  }

  /**
   * Retrieves the term that matched
   * @return Matching term
   */
  public String getTerm() {
    return term;
  }

  /**
   * Retrieves the payloads (if any) associated with a matching term.  An empty
   * list will be returned if no payloads are found.
   * @return (Possibly empty) List of payloads
   */
  public List<String> getPayloads() {
    return payloads;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EntityInfo that = (EntityInfo) o;

    if (term != null ? !term.equals(that.term) : that.term != null) return false;
    return payloads != null ? payloads.equals(that.payloads) : that.payloads == null;
  }

  @Override
  public int hashCode() {
    int result = term != null ? term.hashCode() : 0;
    result = 31 * result + (payloads != null ? payloads.hashCode() : 0);
    return result;
  }
}
