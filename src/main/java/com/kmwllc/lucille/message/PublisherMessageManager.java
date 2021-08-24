package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;

public interface PublisherMessageManager {

  void sendForProcessing(Document document) throws Exception;

  void close();
}
