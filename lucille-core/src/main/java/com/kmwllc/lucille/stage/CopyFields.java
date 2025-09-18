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
import com.typesafe.config.Config;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.Pair;
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

  private List<Pair<Document.Segment[],Document.Segment[]>> nestedFieldPairs = new ArrayList<>();

  public CopyFields(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
    this.updateMode = UpdateMode.fromConfig(config);
    this.isNested = ConfigUtils.getOrDefault(config, "isNested", false);
  }

  @Override
  public void start() throws StageException {
    if (fieldMapping.size() == 0) {
      throw new StageException("fieldMapping must have at least one source-dest pair for Copy Fields");
    }

    if (isNested && updateMode != null) {
      log.info("Cannot set both isNested and update_mode in Copy Fields at the same time. Ignoring update_mode, fields will be overwritten.");
    }

    // create source and destination field parts if nested so we don't have to split them up for every doc
    if (isNested) {
      for (Entry<String, Object> entry : fieldMapping.entrySet()) {
        Document.Segment[] source = Document.Segment.parse(entry.getKey());
        Document.Segment[] dest = Document.Segment.parse((String) entry.getValue());
        nestedFieldPairs.add(Pair.of(source, dest));
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (isNested) {
      for (Pair<Document.Segment[],Document.Segment[]> fieldPair : nestedFieldPairs) {
        JsonNode sourceVal = doc.getNestedJson(fieldPair.getKey());
        if (sourceVal == null) {
          continue;
        }
        try {
          doc.setNestedJson(fieldPair.getValue(), sourceVal);
        } catch (IllegalArgumentException ex) {
          // TODO add field name to error by converting Segment[] back to name
          log.error("Failed to set field on doc {}.\n {}", doc.getId(), ex.getMessage());
        }
      }
    } else {
      for (Entry<String, Object> fieldPair : fieldMapping.entrySet()) {
        if (!doc.has(fieldPair.getKey())) {
          continue;
        }
        doc.update((String) fieldPair.getValue(), updateMode, doc.getStringList(fieldPair.getKey()).toArray(new String[0]));
      }
    }
    return null;
  }
}