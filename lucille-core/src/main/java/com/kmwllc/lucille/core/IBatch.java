package com.kmwllc.lucille.core;

import java.util.List;

public interface IBatch {
  public List<Document> add(Document doc);

  public List<Document> flushIfExpired();

  public List<Document> flush();
}
