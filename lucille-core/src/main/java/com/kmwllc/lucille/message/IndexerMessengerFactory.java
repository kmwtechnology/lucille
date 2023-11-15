package com.kmwllc.lucille.message;

import com.typesafe.config.Config;

public interface IndexerMessengerFactory {

  IndexerMessenger create();

  static IndexerMessengerFactory getConstantFactory(IndexerMessenger messenger) {
    return new IndexerMessengerFactory() {
      @Override
      public IndexerMessenger create() {
        return messenger;
      }
    };
  }

  static IndexerMessengerFactory getKafkaFactory(Config config, String pipelineName) {
    return new IndexerMessengerFactory() {
      @Override
      public IndexerMessenger create() {
        return new KafkaIndexerMessenger(config, pipelineName);
      }
    };
  }

}
