package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.DefaultFileContentFetcher;
import com.typesafe.config.Config;
import java.lang.reflect.Constructor;

public class FileContentFetcherFactory {

  public FileContentFetcher create(Config config)
  {
    if(config == null || !config.hasPath("fetcherClass")) {
      return new DefaultFileContentFetcher(config);
    }

    String fetcherClass = config.getString("fetcherClass");
    try {
      Class<?> clazz = Class.forName(fetcherClass);
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      return (FileContentFetcher) constructor.newInstance(config);
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate FileContentFetcher of type " + fetcherClass, e);
    }
  }
}
