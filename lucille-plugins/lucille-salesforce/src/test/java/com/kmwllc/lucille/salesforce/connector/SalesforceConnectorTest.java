package com.kmwllc.lucille.salesforce.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.FieldSummary;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.QueryPage;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.SObjectSummary;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class SalesforceConnectorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testCrawlsNamedStandardAndCustomObjects() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account")).object(standard("Contact")).object(standard("Opportunity"))
        .object(standard("Lead")).object(custom("Project__c"));
    salesforce.fields.put("Account", fields("Id", "Name", "Region__c"));
    salesforce.fields.put("Project__c", fields("Id", "Name", "Budget__c"));
    salesforce.fields.put("Lead", fields("Id", "Name", "LastModifiedDate"));
    salesforce.records.put("Account", List.of(record("""
        {"Id":"001000000000001AAA","Name":"Acme","Region__c":"Northeast"}
        """)));
    salesforce.records.put("Contact", List.of(
        record("{\"Id\":\"003000000000001AAA\",\"Name\":\"Ada Lovelace\"}"),
        record("{\"Id\":null,\"Name\":\"Missing\"}")));
    salesforce.records.put("Opportunity", List.of(record("{\"Id\":\"006000000000001AAA\",\"Name\":\"Big Deal\"}")));
    salesforce.records.put("Lead", List.of(record("""
        {"Id":"00Q000000000001AAA","Name":"Jane Lead","LastModifiedDate":"2024-01-15T13:45:30.000+0000"}
        """)));
    salesforce.records.put("Project__c", List.of(record("""
        {"Id":"a01000000000001AAA","Name":"Widget Project","Budget__c":5000}
        """)));

    SalesforceConnector connector = connector("""
        apiVersion: "v67.0"
        docIdPrefix: "sf-"
        includeStandard: false
        includeCustom: false
        sobjects: ["Account", "Contact", "Opportunity", "Lead", "Project__c"]
        """, salesforce);

    List<Document> docs = run(connector);

    assertEquals(5, docs.size());
    assertEquals("sf-001000000000001AAA", docs.get(0).getId());
    assertEquals("Account", docs.get(0).getString("salesforce_sobject"));
    assertEquals("Acme", docs.get(0).getString("Name"));
    assertEquals("Northeast", docs.get(0).getString("Region__c"));
    assertEquals("001000000000001AAA", docs.get(0).getString("Id"));
    assertEquals("sf-003000000000001AAA", docs.get(1).getId());
    assertEquals("Contact", docs.get(1).getString("salesforce_sobject"));
    assertEquals("Opportunity", docs.get(2).getString("salesforce_sobject"));
    assertEquals("Lead", docs.get(3).getString("salesforce_sobject"));
    assertEquals(Instant.parse("2024-01-15T13:45:30.000Z"), docs.get(3).getInstant("LastModifiedDate"));
    assertEquals("Project__c", docs.get(4).getString("salesforce_sobject"));
    assertEquals(Long.valueOf(5000), docs.get(4).getLong("Budget__c"));
    assertTrue(salesforce.soql.get(0).contains("Region__c"));
    assertTrue(salesforce.soql.get(4).contains("Budget__c"));
    assertTrue(connector.getMessage().contains("Published 5 documents"));
    assertTrue(connector.getMessage().contains("skipped 1 records"));
  }

  @Test
  public void testDiscoversStandardAndCustomObjects() throws Exception {
    FakeClient salesforce = catalog();
    salesforce.records.put("Account", List.of(record("{\"Id\":\"001000000000001AAA\",\"Name\":\"Acme\"}")));
    salesforce.records.put("Widget__c", List.of(record("{\"Id\":\"a01000000000001AAA\",\"Name\":\"Widget\"}")));

    List<Document> docs = run(connector("""
        excludeSobjects: ["Task"]
        """, salesforce));

    assertEquals(List.of("Account", "Widget__c"), docs.stream().map(doc -> doc.getString("salesforce_sobject")).toList());
    assertFalse(String.join("\n", salesforce.soql).contains("Task"));
    assertFalse(String.join("\n", salesforce.soql).contains("Report"));
  }

  @Test
  public void testIncludeFlagsFilterDiscovery() throws Exception {
    FakeClient standardOnly = catalog();
    run(connector("includeCustom: false", standardOnly));
    assertEquals(List.of("FROM Account", "FROM Task"), fromClauses(standardOnly));

    FakeClient customOnly = catalog();
    run(connector("includeStandard: false", customOnly));
    assertEquals(List.of("FROM Widget__c"), fromClauses(customOnly));
  }

  @Test
  public void testDiscoveredObjectThatCannotBeQueriedIsSkipped() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account")).object(custom("Broken__c"));
    salesforce.records.put("Account", List.of(record("{\"Id\":\"001000000000001AAA\",\"Name\":\"Acme\"}")));
    salesforce.queryFailures.add("Broken__c");

    SalesforceConnector connector = connector("", salesforce);
    List<Document> docs = run(connector);

    assertEquals(1, docs.size());
    assertEquals("Acme", docs.get(0).getString("Name"));
    assertTrue(connector.getMessage().contains("skipped 0 records and 1 objects"));
  }

  @Test
  public void testNamedObjectFailureFailsTheRun() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(custom("Broken__c"));
    salesforce.queryFailures.add("Broken__c");
    SalesforceConnector connector = connector("sobjects: [\"Broken__c\"]", salesforce);
    ConnectorException error = assertThrows(ConnectorException.class, () -> run(connector));
    assertTrue(error.getMessage().contains("Broken__c"));
  }

  @Test
  public void testMissingObjectFailsTheRun() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account"));
    SalesforceConnector connector = connector("sobjects: [\"DoesNotExist__c\"]", salesforce);
    ConnectorException error = assertThrows(ConnectorException.class, () -> run(connector));
    assertTrue(error.getMessage().contains("DoesNotExist__c"));
  }

  @Test
  public void testQueryForAnObjectOutsideTheCrawlFails() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account")).object(standard("Contact"));
    SalesforceConnector connector = connector("""
        sobjects: ["Account"]
        queries: {
          Contact: "SELECT Id FROM Contact"
        }
        """, salesforce);
    ConnectorException error = assertThrows(ConnectorException.class, () -> run(connector));
    assertTrue(error.getMessage().contains("Contact"));
  }

  @Test
  public void testQueryOverrideIsSentAsWritten() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(custom("Project__c"));
    salesforce.records.put("Project__c", List.of(record("{\"Id\":\"a01000000000001AAA\",\"Name\":\"Widget\"}")));
    String soql = "SELECT Id, Name FROM Project__c WHERE Active__c = true";

    List<Document> docs = run(connector("""
        queries: {
          Project__c: "%s"
        }
        """.formatted(soql), salesforce));

    assertEquals(List.of(soql), salesforce.soql);
    assertEquals(1, docs.size());
    assertEquals("Project__c", docs.get(0).getString("salesforce_sobject"));
    assertEquals("Widget", docs.get(0).getString("Name"));
  }

  @Test
  public void testWhereLimitAndPagination() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account"));
    salesforce.records.put("Account", List.of(record("{\"Id\":\"001000000000001AAA\",\"Name\":\"Acme\"}")));
    salesforce.nextUrl = "/services/data/v67.0/query/next-account";
    salesforce.nextPage = new QueryPage(2, true, null,
        List.of(record("{\"Id\":\"001000000000002AAA\",\"Name\":\"Beta\"}")));

    List<Document> docs = run(connector("""
        sobjects: ["Account"]
        where: "WHERE Industry = 'order by mail'"
        limit: 2
        """, salesforce));

    assertEquals(List.of("001000000000001AAA", "001000000000002AAA"), docs.stream().map(Document::getId).toList());
    assertTrue(salesforce.soql.get(0).contains("WHERE Industry = 'order by mail' ORDER BY Id LIMIT 2"));
    assertEquals("MORE /services/data/v67.0/query/next-account", salesforce.soql.get(1));
  }

  @Test
  public void testIgnoreFieldsAndIdField() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account"));
    salesforce.fields.put("Account", fields("Id", "Name", "AnnualRevenue"));
    salesforce.records.put("Account", List.of(record("""
        {"Id":"001000000000001AAA","Name":"Acme"}
        """)));

    List<Document> docs = run(connector("""
        sobjects: ["Account"]
        idField: "Name"
        ignoreFields: ["AnnualRevenue"]
        """, salesforce));

    assertEquals("Acme", docs.get(0).getId());
    assertEquals("001000000000001AAA", docs.get(0).getString("Id"));
    assertFalse(salesforce.soql.get(0).contains("AnnualRevenue"));
    assertNull(docs.get(0).getString("AnnualRevenue"));
  }

  @Test
  public void testChunksWideObjectsAndMergesOnId() throws Exception {
    List<String> fields = new ArrayList<>();
    fields.add("Id");
    for (int i = 0; i < 12; i++) {
      fields.add(String.format("Field%02d__c", i));
    }
    int maxLength = 179;
    int budget = 30;
    List<List<String>> chunks = Soql.chunkFields("Widget__c", fields, null, null, maxLength, budget);
    String tail = fields.get(fields.size() - 1);
    assertTrue(chunks.size() > 1);
    assertFalse(chunks.get(0).contains(tail));

    FakeClient salesforce = new FakeClient();
    salesforce.object(custom("Widget__c"));
    List<FieldSummary> described = new ArrayList<>();
    for (String field : fields) {
      described.add(new FieldSummary(field, "Id".equals(field) ? "id" : "string", false));
    }
    salesforce.fields.put("Widget__c", described);
    salesforce.records.put("Widget__c", List.of(record("""
        {"Id":"a01000000000001AAA","Name":"Widget"}
        """)));
    salesforce.extraRecords.put("Widget__c", List.of(record(
        "{\"Id\":\"a01000000000001AAA\",\"" + tail + "\":\"tail\"}")));

    SalesforceConnector connector = new SalesforceConnector(config("sobjects: [\"Widget__c\"]"), salesforce, maxLength, budget);
    List<Document> docs = run(connector);

    assertEquals("tail", docs.get(0).getString(tail));
    assertFalse(salesforce.soql.get(0).contains(tail));
    assertTrue(salesforce.soql.stream().anyMatch(sql -> sql.contains("WHERE Id IN (")));
  }

  @Test
  public void testReservedFieldFailsANamedObject() throws Exception {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account"));
    salesforce.fields.put("Account", List.of(
        new FieldSummary("Id", "id", false),
        new FieldSummary("run_id", "string", false)));
    SalesforceConnector connector = connector("sobjects: [\"Account\"]", salesforce);
    ConnectorException error = assertThrows(ConnectorException.class, () -> run(connector));
    assertTrue(error.getMessage().contains("run_id"));
  }

  @Test
  public void testSelectableFieldsSkipCompoundBinaryAndDeprecated() throws Exception {
    List<FieldSummary> fields = List.of(
        new FieldSummary("Id", "id", false),
        new FieldSummary("BillingAddress", "address", false),
        new FieldSummary("Body", "base64", false),
        new FieldSummary("Old__c", "string", true),
        new FieldSummary("Region__c", "string", false),
        new FieldSummary("AnnualRevenue", "currency", false));

    assertEquals(List.of("Id", "Region__c"),
        SalesforceConnector.selectableFields("Account", fields, Set.of("annualrevenue"), false, "salesforce_sobject"));

    List<String> withBinary = SalesforceConnector.selectableFields("Account", fields, Set.of(), true, null);
    assertTrue(withBinary.contains("Body"));
    assertFalse(withBinary.contains("BillingAddress"));
    assertFalse(withBinary.contains("Old__c"));

    List<FieldSummary> reserved = List.of(
        new FieldSummary("Id", "id", false),
        new FieldSummary("run_id", "string", false));
    assertThrows(ConnectorException.class,
        () -> SalesforceConnector.selectableFields("Account", reserved, Set.of(), false, null));
  }

  @Test
  public void testRejectsMissingAuth() {
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(ConfigFactory.parseString("""
        pipeline: "salesforce"
        """)));
  }

  @Test
  public void testRejectsIncompletePasswordFlow() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new SalesforceConnector(ConfigFactory.parseString("""
            pipeline: "salesforce"
            clientId: "id"
            clientSecret: "secret"
            username: "user@example.com"
            """)));
    assertTrue(error.getMessage().contains("password"));
  }

  @Test
  public void testRejectsTokenWithoutInstance() {
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(ConfigFactory.parseString("""
        pipeline: "salesforce"
        accessToken: "token"
        """)));
  }

  @Test
  public void testRejectsDiscoveryWithNothingIncluded() {
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(config("""
        includeStandard: false
        includeCustom: false
        """)));
  }

  @Test
  public void testRejectsBadWhereApiVersionAndBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(config("where: \"LIMIT 1\"")));
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(config("apiVersion: \"nope\"")));
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(config("batchSize: 10")));
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(config("ignoreFields: [\"Id\"]")));
    assertThrows(IllegalArgumentException.class, () -> new SalesforceConnector(ConfigFactory.parseString("""
        accessToken: "token"
        instanceUrl: "https://example.my.salesforce.com"
        """)));
  }

  @Test
  public void testClientCredentialsConfigBuilds() throws Exception {
    SalesforceConnector connector = new SalesforceConnector(ConfigFactory.parseString("""
        pipeline: "salesforce"
        clientId: "id"
        clientSecret: "secret"
        loginUrl: "https://mycompany.my.salesforce.com/"
        """));
    try {
      assertEquals("salesforce", connector.getPipelineName());
    } finally {
      connector.close();
    }
  }

  @Test
  public void testCrawlThroughTheRestApi() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    List<String> queries = Collections.synchronizedList(new ArrayList<>());
    List<String> paths = Collections.synchronizedList(new ArrayList<>());
    List<String> batchHeaders = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger accountQueries = new AtomicInteger();
    server.createContext("/", exchange -> {
      String path = exchange.getRequestURI().getPath();
      paths.add(path);
      String auth = exchange.getRequestHeaders().getFirst("Authorization");
      if (path.contains("/services/data/") && !"Bearer token-1".equals(auth)) {
        send(exchange, 401, "[{\"message\":\"bad token\",\"errorCode\":\"INVALID_SESSION_ID\"}]");
        return;
      }
      if (path.endsWith("/sobjects")) {
        send(exchange, 200, """
            {"sobjects":[
              {"name":"Account","label":"Account","custom":false,"queryable":true},
              {"name":"Widget__c","label":"Widget","custom":true,"queryable":true}
            ]}
            """);
        return;
      }
      if (path.endsWith("/sobjects/Account/describe")) {
        send(exchange, 200, """
            {"fields":[
              {"name":"Id","type":"id"},
              {"name":"Name","type":"string"},
              {"name":"AnnualRevenue","type":"currency"},
              {"name":"NumberOfEmployees","type":"int"},
              {"name":"LastModifiedDate","type":"datetime"},
              {"name":"DoNotCall","type":"boolean"},
              {"name":"Fax","type":"phone"},
              {"name":"BillingAddress","type":"address"},
              {"name":"Body","type":"base64"},
              {"name":"Region__c","type":"string"}
            ]}
            """);
        return;
      }
      if (path.endsWith("/sobjects/Widget__c/describe")) {
        send(exchange, 200, """
            {"fields":[
              {"name":"Id","type":"id"},
              {"name":"Name","type":"string"},
              {"name":"Amount__c","type":"double"}
            ]}
            """);
        return;
      }
      if (path.endsWith("/query")) {
        String batch = exchange.getRequestHeaders().getFirst("Sforce-Query-Options");
        if (batch != null) {
          batchHeaders.add(batch);
        }
        String soql = soql(exchange);
        queries.add(soql);
        if (soql.contains("FROM Account") && accountQueries.incrementAndGet() == 1) {
          send(exchange, 200, """
              {"totalSize":2,"done":false,"nextRecordsUrl":"/services/data/v67.0/query/01g-next",
               "records":[{"attributes":{"type":"Account"},"Id":"001000000000001AAA","Name":"Acme",
                 "AnnualRevenue":1000000.5,"NumberOfEmployees":10,
                 "LastModifiedDate":"2024-01-15T13:45:30.000+0000","DoNotCall":false,"Fax":null,
                 "Region__c":"Northeast"}]}
              """);
          return;
        }
        if (soql.contains("FROM Widget__c")) {
          send(exchange, 200, """
              {"totalSize":1,"done":true,"records":[{"attributes":{"type":"Widget__c"},
                "Id":"a01000000000001AAA","Name":"Widget","Amount__c":12.5}]}
              """);
          return;
        }
      }
      if (path.endsWith("/query/01g-next")) {
        send(exchange, 200, """
            {"totalSize":2,"done":true,"records":[
              {"attributes":{"type":"Account"},"Id":"001000000000002AAA","Name":"Beta"},
              {"attributes":{"type":"Account"},"Id":null,"Name":"Nobody"}
            ]}
            """);
        return;
      }
      send(exchange, 404, "{\"message\":\"not found\"}");
    });
    server.start();
    try {
      String base = "http://localhost:" + server.getAddress().getPort();
      SalesforceConnector connector = new SalesforceConnector(ConfigFactory.parseString("""
          pipeline: "salesforce"
          accessToken: "token-1"
          instanceUrl: "%s/"
          sobjects: ["Account", "Widget__c"]
          batchSize: 500
          """.formatted(base)));
      TestMessenger messenger = new TestMessenger();
      PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
      try {
        connector.preExecute("run1");
        connector.execute(publisher);
      } finally {
        connector.close();
      }

      List<Document> docs = messenger.getDocsSentForProcessing();
      assertEquals(3, docs.size());
      Document acme = docs.get(0);
      assertEquals("001000000000001AAA", acme.getId());
      assertEquals("Account", acme.getString("salesforce_sobject"));
      assertEquals("Acme", acme.getString("Name"));
      assertEquals(Double.valueOf(1000000.5), acme.getDouble("AnnualRevenue"));
      assertEquals(Long.valueOf(10), acme.getLong("NumberOfEmployees"));
      assertEquals(Instant.parse("2024-01-15T13:45:30.000Z"), acme.getInstant("LastModifiedDate"));
      assertFalse(acme.getBoolean("DoNotCall"));
      assertFalse(acme.has("Fax"));
      assertEquals("Northeast", acme.getString("Region__c"));
      assertEquals("Beta", docs.get(1).getString("Name"));
      assertEquals("Widget__c", docs.get(2).getString("salesforce_sobject"));
      assertEquals(Double.valueOf(12.5), docs.get(2).getDouble("Amount__c"));

      String accountQuery = queries.get(0);
      assertTrue(accountQuery.contains("Region__c"));
      assertFalse(accountQuery.contains("BillingAddress"));
      assertFalse(accountQuery.contains("Body"));
      assertTrue(queries.get(1).contains("Amount__c"));
      assertFalse(batchHeaders.isEmpty());
      assertTrue(batchHeaders.stream().allMatch("batchSize=500"::equals));
      assertTrue(paths.stream().anyMatch(path -> path.contains("/services/data/v67.0/sobjects")));
      assertTrue(connector.getMessage().contains("Published 3 documents"));
      assertTrue(connector.getMessage().contains("skipped 1 records"));
    } finally {
      server.stop(0);
    }
  }

  private static List<Document> run(SalesforceConnector connector) throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    try {
      connector.preExecute("run1");
      connector.execute(publisher);
      return messenger.getDocsSentForProcessing();
    } finally {
      connector.close();
    }
  }

  private static SalesforceConnector connector(String extra, FakeClient salesforce) {
    return new SalesforceConnector(config(extra), salesforce, Soql.MAX_QUERY_LENGTH, Soql.ID_CLAUSE_BUDGET);
  }

  private static Config config(String extra) {
    return ConfigFactory.parseString("""
        pipeline: "salesforce"
        accessToken: "token"
        instanceUrl: "https://example.my.salesforce.com"
        """ + extra);
  }

  private static JsonNode record(String json) throws Exception {
    return MAPPER.readTree(json);
  }

  private static List<FieldSummary> fields(String... namesAndTypes) {
    List<FieldSummary> fields = new ArrayList<>();
    for (int i = 0; i < namesAndTypes.length; i++) {
      String name = namesAndTypes[i];
      String type = "Id".equals(name) ? "id" : "LastModifiedDate".equals(name) ? "datetime" : "string";
      fields.add(new FieldSummary(name, type, false));
    }
    return fields;
  }

  private static SObjectSummary standard(String name) {
    return new SObjectSummary(name, name, false, true, false, false);
  }

  private static SObjectSummary custom(String name) {
    return new SObjectSummary(name, name, true, true, false, false);
  }

  /** A small org: two crawlable objects plus objects discovery must ignore. */
  private static FakeClient catalog() {
    FakeClient salesforce = new FakeClient();
    salesforce.object(standard("Account"));
    salesforce.object(custom("Widget__c"));
    salesforce.object(standard("Task"));
    salesforce.object(new SObjectSummary("Report", "Report", false, false, false, false));
    salesforce.object(new SObjectSummary("Legacy__c", "Legacy", true, true, true, false));
    salesforce.object(new SObjectSummary("Tooling__x", "Tooling", false, true, false, true));
    salesforce.object(new SObjectSummary("Bad Name", "Bad", false, true, false, false));
    return salesforce;
  }

  private static List<String> fromClauses(FakeClient salesforce) {
    List<String> clauses = new ArrayList<>();
    for (String sql : salesforce.soql) {
      int from = sql.indexOf("FROM ");
      if (from >= 0) {
        clauses.add(sql.substring(from).split(" ")[0] + " " + sql.substring(from).split(" ")[1]);
      }
    }
    return clauses;
  }

  private static String soql(HttpExchange exchange) {
    String raw = exchange.getRequestURI().getRawQuery();
    if (raw == null || !raw.startsWith("q=")) {
      return "";
    }
    return URLDecoder.decode(raw.substring(2), StandardCharsets.UTF_8);
  }

  private static void send(HttpExchange exchange, int status, String body) throws java.io.IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private static final class FakeClient implements SalesforceClient {

    private final List<SObjectSummary> objects = new ArrayList<>();
    private final java.util.Map<String, List<FieldSummary>> fields = new java.util.HashMap<>();
    private final java.util.Map<String, List<JsonNode>> records = new java.util.HashMap<>();
    private final java.util.Map<String, List<JsonNode>> extraRecords = new java.util.HashMap<>();
    private final java.util.Set<String> queryFailures = new java.util.HashSet<>();
    private final List<String> soql = new ArrayList<>();
    private String nextUrl;
    private QueryPage nextPage;

    private FakeClient object(SObjectSummary summary) {
      objects.add(summary);
      return this;
    }

    @Override
    public void connect() {
    }

    @Override
    public String instanceUrl() {
      return "https://example.my.salesforce.com";
    }

    @Override
    public List<SObjectSummary> listSObjects() {
      return List.copyOf(objects);
    }

    @Override
    public List<FieldSummary> describeFields(String sobject) {
      return fields.getOrDefault(sobject, fields("Id", "Name"));
    }

    @Override
    public QueryPage query(String sql) throws java.io.IOException {
      soql.add(sql);
      String object = Soql.referencedObject(sql);
      if (queryFailures.contains(object)) {
        throw new java.io.IOException("MALFORMED_QUERY: " + object + " requires a filter");
      }
      if (sql.contains(" WHERE Id IN (")) {
        List<JsonNode> rows = extraRecords.getOrDefault(object, List.of());
        return new QueryPage(rows.size(), true, null, rows);
      }
      List<JsonNode> rows = records.getOrDefault(object, List.of());
      if (nextUrl != null && nextPage != null) {
        int total = rows.size() + nextPage.records().size();
        return new QueryPage(total, false, nextUrl, rows);
      }
      return new QueryPage(rows.size(), true, null, rows);
    }

    @Override
    public QueryPage queryMore(String nextRecordsUrl) {
      soql.add("MORE " + nextRecordsUrl);
      return nextPage;
    }

    @Override
    public void close() {
    }
  }
}
