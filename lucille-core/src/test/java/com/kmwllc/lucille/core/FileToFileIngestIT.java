package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class FileToFileIngestIT {
  
  @Test 
  public void fullIT() throws Exception {
    Config config = ConfigFactory.parseFile(new File("src/test/resources/FileToFileIngestIT/file-to-file-example.conf"));
    Runner.runInTestMode(config);

    File f = new File("output/dest.csv");
    BufferedReader reader = new BufferedReader(new FileReader(f));
    assertEquals("\"my_name\",\"my_country\",\"my_price\"", reader.readLine());
    assertEquals("\"Carbonara\",\"Italy\",\"30\"", reader.readLine());
    assertEquals("\"Pizza\",\"Italy\",\"10\"", reader.readLine());
    assertEquals("\"Tofu Soup\",\"Korea\",\"12\"", reader.readLine());
    assertEquals("\"my_name\",\"my_country\",\"my_price\"", reader.readLine());
    assertEquals("\"Burger\",\"USA\",\"30\"", reader.readLine());
    assertEquals("\"Salad\",\"USA\",\"10\"", reader.readLine());
    assertEquals("\"Wine\",\"France\",\"12\"", reader.readLine());
    reader.close();
  }
}
