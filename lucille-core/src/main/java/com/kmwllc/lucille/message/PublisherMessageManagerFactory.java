package com.kmwllc.lucille.message;

import com.typesafe.config.Config;

public interface PublisherMessageManagerFactory {

  PublisherMessageManager create();

  static PublisherMessageManagerFactory getConstantFactory(PublisherMessageManager manager) {
    return new PublisherMessageManagerFactory() {
      @Override
      public PublisherMessageManager create() {
        return manager;
      }
    };
  }

  static PublisherMessageManagerFactory getKafkaFactory(Config config) {
    return new PublisherMessageManagerFactory() {
      @Override
      public PublisherMessageManager create() {
        return new KafkaPublisherMessageManager(config);
      }
    };
  }

}
