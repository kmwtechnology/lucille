package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import com.kmwllc.lucille.core.BaseConfigException;

/**
 * Adds random Booleans to documents given parameters.
 *
 * <br>
 *
 * Config Parameters:
 * <p> <b>field_name</b> (String, Optional) : Field name of field where data is placed. Defaults to "data".
 * <p> <b>percent_true</b> (Integer, Optional) : Determines the rate at which "true" is selected and placed on a document. Defaults to 50.
 */
public class AddRandomBoolean extends Stage {

  public static class AddRandomBooleanConfig extends BaseStageConfig {

    private String fieldName = "data";
    private int percentTrue = 50;

    public void apply(Config config) {
      fieldName = ConfigUtils.getOrDefault(config, "field_name", fieldName);
      percentTrue = ConfigUtils.getOrDefault(config, "percent_true", percentTrue);
    }

    public void validate() throws StageException {
      try {
        super.validate();
      } catch (BaseConfigException e) {
        throw new StageException(e);
      }
      if (percentTrue > 100 || percentTrue < 0) {
        throw new StageException("Invalid value for percent_true. Must be between 0 and 100 inclusive.");
      }
    }
  
    public String getFieldName() { return fieldName; }
    public int getPercentTrue() { return percentTrue; }

  }

  AddRandomBooleanConfig config;

  public AddRandomBoolean(Config configIn) throws Exception {
    super(configIn, Spec.stage()
        .withOptionalProperties("field_name", "percent_true"));

    config = new AddRandomBooleanConfig();
    config.apply(configIn);
    config.validate();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setField(config.fieldName, (ThreadLocalRandom.current().nextInt(100) < config.percentTrue));

    return null;
  }

}
