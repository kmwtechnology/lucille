package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.JSONUtils;
import com.typesafe.config.Config;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies values from a source field to a destination field based on the field mapping. You can turn on the ability to copy fields
 * as nested json by setting the isNested parameter to true or keep it false to treat the field names as literal field names.
 *
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldMapping (Map&lt;String, String&gt;) : A 1-1 mapping of source field names to destination field names.</li>
 *   <li>update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'. Cannot be set with isNested.</li>
 *   <li>isNested (Boolean, Optional) : Sets whether or not to treat the given field name as nested json or a literal field name.
 *   It true, the field name will be split on '.' and each part will be treated as a level of nesting. Cannot be set with update_mode.
 *   Defaults to false. </li>
 * </ul>
 */
public class CopyFields extends Stage {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final ObjectMapper mapper = new ObjectMapper();

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredParent("fieldMapping", new TypeReference<Map<String, String>>() {})
      .optionalString("update_mode")
      .optionalBoolean("isNested").build();

  private final Map<String, Object> fieldMapping;
  private final UpdateMode updateMode;
  private final boolean isNested;

  public CopyFields(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
    this.updateMode = UpdateMode.fromConfig(config);
    this.isNested = ConfigUtils.getOrDefault(config, "isNested", false);
  }

  @Override
  public void start() throws StageException {
    if (this.fieldMapping.size() == 0) {
      throw new StageException("fieldMapping must have at least one source-dest pair for Copy Fields");
    }

    if (this.isNested && this.updateMode != null) {
      log.info("Cannot set both isNested and update_mode in Copy Fields at the same time. Ignoring update_mode, fields will be overwritten.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, Object> fieldPair : this.fieldMapping.entrySet()) {
      String source = fieldPair.getKey();
      String dest = (String) fieldPair.getValue();

      if (!this.isNested) {
        if (!doc.has(source)) {
          continue;
        }
        doc.update(dest, updateMode, doc.getStringList(source).toArray(new String[0]));
      } else {
        // deal with nested case
        String[] nestedSourceField = source.split("[.]");
        JsonNode sourceJson = doc.getJson(nestedSourceField[0]);
        if (sourceJson == null) {
          continue;
        }
        JsonNode sourceVal;
        if (nestedSourceField.length == 1) {
          sourceVal = sourceJson;
        } else {
          try {
            sourceVal = JSONUtils.getNestedValue(nestedSourceField, sourceJson, 1);
          } catch (IOException e) {
            log.error("Cannot copy field {} for doc {}: {}", source,
                doc.getId(), e.getMessage());
            return null;
          }
        }

        if (sourceVal == null) {
          continue;
        }

        // get dest json field value, if exists
        String[] nestedDestField = dest.split("[.]");
        if (!doc.has(nestedDestField[0])) {
          doc.setField(nestedDestField[0], mapper.createObjectNode());
        }
        JsonNode destJson = doc.getJson(nestedDestField[0]);

        if (nestedDestField.length == 1) {
          doc.setField(nestedDestField[0], sourceVal);
        } else {
          try {
            JSONUtils.putNestedFieldValue(destJson, nestedDestField, sourceVal, 1);
          }  catch (IOException e) {
            log.error("Cannot update json field {} with value {} for doc {}: {}", dest, sourceVal, doc.getId(), e.getMessage());
            return null;
          }
        }
      }
    }

    return null;
  }
}
