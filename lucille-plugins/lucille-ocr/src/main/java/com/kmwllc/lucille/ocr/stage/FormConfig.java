package com.kmwllc.lucille.ocr.stage;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class FormConfig implements Serializable {
  private Map<Integer, FormTemplate> pageTemplates;

  public FormConfig() {
    this.pageTemplates = new LinkedHashMap<>();
  }

  public Map<Integer, FormTemplate> getPageTemplates() {
    return pageTemplates;
  }

  public void setPageTemplates(Map<Integer, FormTemplate> pageTemplates) {
    this.pageTemplates = pageTemplates;
  }
}
