package com.kmwllc.lucille.ocr.stage;


import java.io.Serializable;
import java.util.List;

public class FormTemplate implements Serializable {

  private String name;
  private List<Rectangle> regions;

  public FormTemplate() {
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public void setRegions(List<Rectangle> regions) {
    this.regions = regions;
  }

  public List<Rectangle> getRegions() {
    return this.regions;
  }

  @Override
  public String toString() {
    return "FormTemplate{" +
      "name='" + name + '\'' +
      ", regions=" + regions +
      '}';
  }
}
