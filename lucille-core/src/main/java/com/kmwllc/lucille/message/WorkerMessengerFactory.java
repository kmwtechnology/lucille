package com.kmwllc.lucille.message;

import com.typesafe.config.Config;

public interface WorkerMessengerFactory {

  WorkerMessenger create();

  static WorkerMessengerFactory getConstantFactory(WorkerMessenger messenger) {
    return new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        return messenger;
      }
    };
  }

  static WorkerMessengerFactory getKafkaFactory(Config config, String pipelineName) {
    return new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        return new KafkaWorkerMessenger(config, pipelineName);
      }
    };
  }

}
