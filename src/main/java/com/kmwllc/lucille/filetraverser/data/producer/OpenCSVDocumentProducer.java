package com.kmwllc.lucille.filetraverser.data.producer;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.filetraverser.data.DocumentProducer;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class OpenCSVDocumentProducer implements DocumentProducer {

  private final boolean copyParentMetadata;

  public OpenCSVDocumentProducer(boolean copyParentMetadata) {
    this.copyParentMetadata = copyParentMetadata; // TODO: not used, consider removing
  }

  @Override
  public List<JsonDocument> produceDocuments(Path file, JsonDocument parent) throws DocumentException, IOException {

    List<JsonDocument> docs = new ArrayList<>();

    try (Reader fileReader = Files.newBufferedReader(file);
         CSVReader csvReader = new CSVReader(fileReader)) {

      // Assume first line is header
      String[] header = csvReader.readNext();
      if (header==null || header.length==0) {
        return docs;
      }

      String[] line;
      int lineNum = 0;
      while ((line = csvReader.readNext()) != null) {
        lineNum++;

        // skip blank lines and lines with no value in the first column
        if (line.length==0 || (line.length==1 && StringUtils.isBlank(line[0]))) {
          continue;
        }

        // assume first column holds the ID
        JsonDocument doc = new JsonDocument(line[0]);
        doc.setOrAddAll(parent);

        int maxIndex = Math.min(header.length,line.length);
        for (int i=1;i<maxIndex;i++) {
          if (line[i]!=null) {
            doc.setField(header[i], line[i]);
            doc.setField("csvLineNumber", lineNum);
          }
        }
        docs.add(doc);
      }

    } catch (CsvValidationException e) {
      throw new IOException(e);
    }

    return docs;
  }

  public static void main(String[] args) throws Exception {
    OpenCSVDocumentProducer producer = new OpenCSVDocumentProducer(false);
    List<JsonDocument> docs = producer.produceDocuments(Path.of("/Volumes/Work/lucille/test.csv"), new JsonDocument("test"));
    for (JsonDocument doc : docs) {
      System.out.println(doc);
    }
  }

}
