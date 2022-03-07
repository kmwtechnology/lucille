package com.kmwllc.lucille.stage.EE;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by matt on 4/19/17.
 */
public class EntityInfo {
  private String term;
  private List<String> payloads;

  public EntityInfo() {

  }

  public String getTerm() {
    return term;
  }

  public void setTerm(String term) {
    this.term = term;
  }

  public List<String> getPayloads() {
    return payloads;
  }

  public void setPayloads(List<String> payloads) {
    this.payloads = payloads;
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