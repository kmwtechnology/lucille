package com.kmwllc.lucille.message;

import com.typesafe.config.Config;

public interface WorkerMessageManagerFactory {
  public WorkerMessageManager create();

  static WorkerMessageManagerFactory getConstantFactory(WorkerMessageManager manager) {
    return new WorkerMessageManagerFactory() {
      @Override
      public WorkerMessageManager create() {
        return manager;
      }
    };
  }

  static WorkerMessageManagerFactory getKafkaFactory(Config config, String pipelineName) {
    return new WorkerMessageManagerFactory() {
      @Override
      public WorkerMessageManager create() {
        return new KafkaWorkerMessageManager(config, pipelineName);
      }
    };
  }

}
