package com.kmwllc.lucille.ocr.stage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class FormTemplate {

  public String name;
  public List<Rectangle> regions;

  @JsonCreator
  public FormTemplate(
    @JsonProperty("name") String name,
    @JsonProperty("regions") List<Rectangle> regions) {
    this.name = name;
    this.regions = regions;
  }

  @Override
  public String toString() {
    return "FormTemplate{" +
      "name='" + name + '\'' +
      ", regions=" + regions +
      '}';
  }
}
