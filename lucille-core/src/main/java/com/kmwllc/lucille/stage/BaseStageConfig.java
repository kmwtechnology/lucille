package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.BaseConfig;
import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;


public class BaseStageConfig extends BaseConfig {

    private String name;
    private String className;
    private String[] conditions;
    private String conditionPolicy;

    public void apply(Config config) {
        name = ConfigUtils.getOrDefault(config, "name", null);
        className = ConfigUtils.getOrDefault(config, "class", null);
        conditionPolicy = ConfigUtils.getOrDefault(config, "conditionPolicy", null);
    }

    
}
