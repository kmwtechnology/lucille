package com.kmwllc.lucille.api;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

/**
 * Checks that the runs IngestDriver started through the API each wrote exactly their own docs, with every
 * pipeline stage applied, to their own output CSV.
 * <p>
 * Not picked up by the normal build (the name doesn't match surefire's default includes). It's run by the
 * verifier container in docker-compose.yml, after IngestDriver completes successfully.
 * There, the runs' CSV output is mounted at /output.
 */
public class VerifyIngestResult {

  @Test
  public void verifyRunA() throws Exception {
    verifyOutput("a");
  }

  @Test
  public void verifyRunB() throws Exception {
    verifyOutput("b");
  }

  // Each run writes to its own CSV (columns: id, run, num, run_label, scratch). If the runs overlapped
  // somehow, a file would contain the other run's docs, duplicates, or be missing docs. The other
  // columns check that every stage in the pipeline ran on every doc.
  private static void verifyOutput(String run) throws Exception {
    Path output = Paths.get("/output/run_" + run + ".csv");
    assertTrue("run " + run + " did not write " + output, Files.exists(output));

    List<String> actualRows = Files.readAllLines(output).stream()
        .map(line -> line.replace("\"", ""))
        .sorted()
        .collect(Collectors.toList());
    // every row should be exactly "<run>-<n>,<run>,<n>,<run>:<run>-<n>," (scratch deleted, so empty)
    List<String> expectedRows = IntStream.range(0, IngestDriver.EXPECTED_DOCS)
        .mapToObj(n -> run + "-" + n + "," + run + "," + n + "," + run + ":" + run + "-" + n + ",")
        .sorted()
        .collect(Collectors.toList());

    List<String> missing = new ArrayList<>(expectedRows);
    missing.removeAll(actualRows);
    List<String> unexpected = new ArrayList<>(actualRows);
    unexpected.removeAll(expectedRows);
    assertTrue(output + " is missing rows (first 10): " + missing.stream().limit(10).toList(), missing.isEmpty());
    assertTrue(output + " has unexpected rows (first 10): " + unexpected.stream().limit(10).toList(), unexpected.isEmpty());
    // catches duplicated rows, which the checks above can't see
    assertEquals(output + " has the wrong number of rows", expectedRows.size(), actualRows.size());
  }
}
