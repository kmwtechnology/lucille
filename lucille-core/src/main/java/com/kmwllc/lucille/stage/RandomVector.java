package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.StageSpec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class RandomVector extends Stage {

  private final List<String> fields;
  private final int dimensions;
  private final Random random;

  private final UpdateMode updateMode;

  public RandomVector(Config config) {
    super(config, new StageSpec()
        .withOptionalProperties("update_mode")
        .withRequiredProperties("fields", "dimensions"));
    this.fields = config.getStringList("fields");
    this.updateMode = UpdateMode.fromConfig(config);
    this.dimensions = config.getInt("dimensions");
    this.random = new Random();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      Float[] floats = new Float[dimensions];
      for (int i = 0; i < this.dimensions; i++) {
        floats[i] = 2 * random.nextFloat() - 1;
      }
      doc.update(field, updateMode, floats);
    }
    return null;
  }
}
