package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.util.Iterator;

public class SetLookup extends Stage {

  private final String file;
  private final String source;
  private final String destination;
  private final boolean ignoreMissingSource;
  private final boolean ignoreCase;
  private final boolean dropFound;

  private LookupCollection<String> values;

  public SetLookup(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("file_path", "source")
        .withOptionalProperties("destination", "ignore_missing_source", "ignore_case", "drop_found")
    );

    // todo consider writing ids to file after indexing was successful

    // required
    file = config.getString("file_path");
    source = config.getString("source");

    // optional
    destination = ConfigUtils.getOrDefault(config, "destination", "setContains");
    ignoreMissingSource = ConfigUtils.getOrDefault(config, "ignore_missing_source", false);
    ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    dropFound = ConfigUtils.getOrDefault(config, "drop_found", false);
  }

  @Override
  public void start() throws StageException {
    values = new StringHashSet(file, ignoreCase);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (!doc.has(source)) {
      doc.setField(destination, ignoreMissingSource);
      return null;
    }

    String value = doc.getString(source);
    if (ignoreCase) {
      value = value.toLowerCase();
    }

    boolean found = values.contains(value);
    doc.setField(destination, found);

    if (found && dropFound) {
      values.remove(value);
    }

    return null;
  }
}
