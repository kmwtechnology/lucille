package com.kmwllc.lucille.message;

import com.typesafe.config.Config;

public interface PublisherMessengerFactory {

  PublisherMessenger create();

  static PublisherMessengerFactory getConstantFactory(PublisherMessenger messenger) {
    return new PublisherMessengerFactory() {
      @Override
      public PublisherMessenger create() {
        return messenger;
      }
    };
  }

  static PublisherMessengerFactory getKafkaFactory(Config config) {
    return new PublisherMessengerFactory() {
      @Override
      public PublisherMessenger create() {
        return new KafkaPublisherMessenger(config);
      }
    };
  }

}
