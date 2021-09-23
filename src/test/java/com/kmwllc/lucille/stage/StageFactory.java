package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Utility for instantiating stages from with a test. Provide various get methods that handle the
 * details of acquiring a Config, creating a stage instance with the Config, and then starting
 * the stage. The stage is returned in a "ready to use" condition.
 */
public class StageFactory {

  private Class<? extends Stage> stageClass;

  private StageFactory(Class<? extends Stage> stageClass) {
    this.stageClass = stageClass;
  }

  /**
   * Creates a StageFactory that returns instances of the designated Stage subclass.
   */
  public static StageFactory of(Class<? extends Stage> stageClass) {
    return new StageFactory(stageClass);
  }

  /**
   * Creates a new instance of the stage with an empty config, starts it, and returns it.
   */
  public Stage get() throws StageException {
    return get(ConfigFactory.empty());
  }

  /**
   * Creates a new instance of the stage with a Config loaded from the given resource path, starts it, and returns it.
   */
  public Stage get(String configResourceName) throws StageException {
    return get(ConfigFactory.load(configResourceName));
  }

  /**
   * Creates a new instance of the stage with a Config built from the provided Map, starts it, and returns it.
   */
  public Stage get(Map map) throws StageException {
    return get(ConfigFactory.parseMap(map));
  }

  /**
   * Creates a new instance of the stage with the designated Config, starts it, and returns it.
   */
  public Stage get(Config config) throws StageException {
    Stage stage;

    try {
      Constructor<? extends Stage> constructor = stageClass.getConstructor(Config.class);
      stage = constructor.newInstance(config);
    } catch (Exception e) {
      throw new StageException("Failed to instantiate stage: " + stageClass.getName(), e);
    }

    stage.start();

    return stage;
  }

}
