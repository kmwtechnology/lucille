package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class FileToFileIngestIT {

  @Test
  public void fullIT() throws Exception {
    File f = null;
    try {
      Config config = ConfigFactory.parseFile(new File("src/test/resources/FileToFileIngestIT/file-to-file-example.conf"));
      Runner.runInTestMode(config);
      f = new File("output/dest.csv");
      List<String> lines = Files.readAllLines(f.toPath());
      assertEquals("\"my_name\",\"my_country\",\"my_price\"", lines.get(0));
      assertEquals("\"Carbonara\",\"Italy\",\"30\"", lines.get(1));
      assertEquals("\"Pizza\",\"Italy\",\"10\"", lines.get(2));
      assertEquals("\"Tofu Soup\",\"Korea\",\"12\"", lines.get(3));
      assertEquals("\"my_name\",\"my_country\",\"my_price\"", lines.get(4));
      assertEquals("\"Burger\",\"USA\",\"30\"", lines.get(5));
      assertEquals("\"Salad\",\"USA\",\"10\"", lines.get(6));
      assertEquals("\"Wine\",\"France\",\"12\"", lines.get(7));
    } finally {
      f.delete();
    }
  }
}
