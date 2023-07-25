package com.kmwllc.lucille.filetraverser.data.producer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.filetraverser.data.DocumentProducer;
import de.siegmar.fastcsv.reader.NamedCsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRow;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FastCSVDocumentProducer implements DocumentProducer {

  private final boolean copyParentMetadata;

  public FastCSVDocumentProducer(boolean copyParentMetadata) {
    this.copyParentMetadata = copyParentMetadata;
  }

  @Override
  public List<Document> produceDocuments(Path file, Document parent)
      throws DocumentException, IOException {

    List<Document> docs = new ArrayList<>();
    try (NamedCsvReader csv = NamedCsvReader.builder().build(file, Charset.defaultCharset())) {
      // Assume the first column is the ID column
      // Note that csv.getHeader() returns a Set but we assume
      // it's going to be a LinkedHashSet with predictable iteration order
      String idColumnName = csv.getHeader().stream().findFirst().get();
      for (NamedCsvRow row : csv) {
        Document doc = parent.deepCopy();
        doc.setField(Document.ID_FIELD, row.getField(idColumnName));
        Map<String, String> fields = row.getFields();
        fields.forEach(doc::setField);
        doc.setField("originalLineNumber", row.getOriginalLineNumber());
        docs.add(doc);
      }
    }

    return docs;
  }
}
