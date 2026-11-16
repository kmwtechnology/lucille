package com.kmwllc.lucille.salesforce.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import java.time.Instant;
import org.junit.Test;

public class SalesforceValuesTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testConvertsSalesforceJsonValues() throws Exception {
    assertNull(SalesforceValues.toFieldValue(NullNode.getInstance(), "string"));
    assertEquals(Boolean.FALSE, SalesforceValues.toFieldValue(MAPPER.readTree("false"), "boolean"));
    assertEquals(Long.valueOf(10), SalesforceValues.toFieldValue(MAPPER.readTree("10"), "int"));
    assertEquals(Double.valueOf(12.5), SalesforceValues.toFieldValue(MAPPER.readTree("12.5"), "currency"));
    assertEquals("Acme", SalesforceValues.toFieldValue(MAPPER.readTree("\"Acme\""), "string"));
    assertEquals("2024-01-15T13:45:30.000+0000",
        SalesforceValues.toFieldValue(MAPPER.readTree("\"2024-01-15T13:45:30.000+0000\""), "string"));
    assertEquals("2024-06-01", SalesforceValues.toFieldValue(MAPPER.readTree("\"2024-06-01\""), "date"));

    assertEquals(Instant.parse("2024-01-15T13:45:30.000Z"),
        SalesforceValues.toFieldValue(MAPPER.readTree("\"2024-01-15T13:45:30.000+0000\""), "datetime"));
    assertEquals(Instant.parse("2024-01-15T13:45:30Z"),
        SalesforceValues.toFieldValue(MAPPER.readTree("\"2024-01-15T13:45:30Z\""), "datetime"));
    assertEquals(Instant.parse("2024-01-15T13:45:30Z"),
        SalesforceValues.toFieldValue(MAPPER.readTree("\"2024-01-15T13:45:30+00:00\""), "datetime"));
    assertEquals("not-a-time", SalesforceValues.toFieldValue(MAPPER.readTree("\"not-a-time\""), "datetime"));
    assertTrue(SalesforceValues.toFieldValue(MAPPER.readTree("{\"city\":\"Boston\"}"), "address") instanceof com.fasterxml.jackson.databind.JsonNode);
  }
}
