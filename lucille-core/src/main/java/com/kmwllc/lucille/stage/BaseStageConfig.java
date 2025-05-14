package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.BaseConfig;
import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;
import com.kmwllc.lucille.core.StageException;


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

    // TODO - add validation - classname and name are not required?
    public void validate() throws StageException {
//        if (name == null || className == null) {
//            throw new StageException("name and className must be specified");
//        }
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String[] getConditions() {
        return conditions;
    }

    public String getConditionPolicy() {
        return conditionPolicy;
    }
    
}
