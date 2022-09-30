package com.kmwllc.lucille.message;

import com.typesafe.config.Config;

public interface IndexerMessageManagerFactory {

  static IndexerMessageManagerFactory getConstantFactory(IndexerMessageManager manager) {
    return new IndexerMessageManagerFactory() {
      @Override
      public IndexerMessageManager create() {
        return manager;
      }
    };
  }

  static IndexerMessageManagerFactory getKafkaFactory(Config config, String pipelineName) {
    return new IndexerMessageManagerFactory() {
      @Override
      public IndexerMessageManager create() {
        return new KafkaIndexerMessageManager(config, pipelineName);
      }
    };
  }

  IndexerMessageManager create();
}
