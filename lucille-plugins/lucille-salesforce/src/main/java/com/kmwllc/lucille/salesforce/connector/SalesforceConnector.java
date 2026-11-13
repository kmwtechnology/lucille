package com.kmwllc.lucille.salesforce.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.FieldSummary;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.QueryPage;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.SObjectSummary;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Crawls Salesforce standard and custom objects and publishes each record as a Document.
 *
 * <p>Standard objects (Account, Contact, Opportunity, Lead, and the rest of the org) and custom objects (API names
 * ending in {@code __c}, and any other object Salesforce marks custom) are read through the REST API. Each object's
 * queryable fields, including custom fields on a standard object, are selected from its describe result. Compound
 * address and geolocation fields are left out, because SOQL selects their components instead, and base64 fields are
 * left out unless {@code includeBinary} is set. A record's {@code Id} becomes the Document id, with {@code docIdPrefix}
 * applied. Datetime values are stored as instants. Nulls are omitted.
 *
 * <p>Name the objects in {@code sobjects}, or omit it to crawl every queryable standard and custom object the user can
 * see. An object discovered that way which Salesforce refuses to query (some require a filter) is logged and skipped.
 * An object named in {@code sobjects} or {@code queries} fails the run instead, so a typo or a missing permission is
 * not silent. Objects with too many fields to fit in one request are read in several queries and merged on {@code Id}.
 *
 * <p>Uploaded files are Attachment, ContentDocument, ContentVersion, and Document records. This connector publishes
 * their fields and leaves the bytes out. {@link com.kmwllc.lucille.salesforce.stage.FetchSalesforceFile} downloads the
 * bytes so TextExtractor can parse them. Keep {@code includeBinary} false, and do not ignore
 * {@code LatestPublishedVersionId} on ContentDocument.
 *
 * <p>Config Parameters:
 * <ul>
 *   <li>pipeline (String): Pipeline that receives the documents. Required, because the connector's work is publishing records.</li>
 *   <li>accessToken (String, Optional): Session token. When set, {@code instanceUrl} is required and no login is performed.</li>
 *   <li>instanceUrl (String, Optional): Instance the token belongs to, such as {@code https://mycompany.my.salesforce.com}.</li>
 *   <li>clientId (String, Optional): Connected-app consumer key. Required unless {@code accessToken} is set.</li>
 *   <li>clientSecret (String, Optional): Connected-app consumer secret. Required unless {@code accessToken} is set.</li>
 *   <li>username (String, Optional): Username for the username-password flow. When set, {@code password} is required.
 *       When omitted, the client-credentials flow is used.</li>
 *   <li>password (String, Optional): Password for the username-password flow, with the security token appended when the
 *       org requires one.</li>
 *   <li>loginUrl (String, Optional): Token endpoint host. Defaults to {@code https://login.salesforce.com}. Use
 *       {@code https://test.salesforce.com} for a sandbox. The client-credentials flow needs the org's My Domain URL.</li>
 *   <li>apiVersion (String, Optional): REST API version, such as {@code 67.0} or {@code v67.0}. Defaults to 67.0.</li>
 *   <li>sobjects (List&lt;String&gt;, Optional): API names to crawl, standard and custom. Omit to discover queryable objects.</li>
 *   <li>excludeSobjects (List&lt;String&gt;, Optional): API names to skip during discovery.</li>
 *   <li>includeStandard (Boolean, Optional): When discovering, include standard objects. Defaults to true. Ignored when
 *       {@code sobjects} or {@code queries} names the objects.</li>
 *   <li>includeCustom (Boolean, Optional): When discovering, include custom objects. Defaults to true. Ignored when
 *       {@code sobjects} or {@code queries} names the objects.</li>
 *   <li>queries (Map&lt;String, String&gt;, Optional): Full SOQL statement per object API name, used instead of the
 *       generated SELECT. When {@code sobjects} is omitted, the keys are the objects to crawl.</li>
 *   <li>where (String, Optional): SOQL condition appended to every generated query. Not applied to {@code queries}.</li>
 *   <li>idField (String, Optional): Field whose value becomes the Document id. Defaults to {@code Id}.</li>
 *   <li>sobjectField (String, Optional): Field set to the object API name on every document. Defaults to
 *       {@code salesforce_sobject}. A blank value disables it.</li>
 *   <li>ignoreFields (List&lt;String&gt;, Optional): Field API names left out of generated queries. {@code Id} cannot be ignored.</li>
 *   <li>includeDeleted (Boolean, Optional): Read with queryAll so deleted and archived records are included. Defaults to false.</li>
 *   <li>includeBinary (Boolean, Optional): Select base64 fields. Defaults to false, because those values are large.</li>
 *   <li>batchSize (Integer, Optional): Records per page, sent as {@code Sforce-Query-Options}. Salesforce accepts 200 to 2000.
 *       Defaults to 2000.</li>
 *   <li>limit (Integer, Optional): SOQL LIMIT on each generated query. Not applied to {@code queries}.</li>
 *   <li>requestTimeoutMs (Integer, Optional): Per-request timeout in milliseconds. Defaults to 120000.</li>
 *   <li>maxRetries (Integer, Optional): Retries after a timeout, a connection failure, or HTTP 408, 429, or 5xx.
 *       Defaults to 3. A 401 on a token this client requested is refreshed once, and that refresh is not a retry.</li>
 * </ul>
 */
public class SalesforceConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(SalesforceConnector.class);

  /** REST API version used when {@code apiVersion} is omitted. */
  static final String DEFAULT_API_VERSION = "67.0";

  private static final String DEFAULT_SOBJECT_FIELD = "salesforce_sobject";
  private static final int DEFAULT_BATCH_SIZE = 2000;

  /** Describe types that cannot appear in a SOQL SELECT. Components of address and location fields are separate fields. */
  private static final Set<String> NON_SELECTABLE_TYPES = Set.of(
      "address", "location", "anytype", "complexvalue", "datacategorygroupreference");

  public static final Spec SPEC = SpecBuilder.connector()
      .optionalString("accessToken", "instanceUrl", "clientId", "clientSecret", "username", "password", "loginUrl",
          "apiVersion", "where", "idField", "sobjectField")
      .optionalList("sobjects", new TypeReference<List<String>>(){})
      .optionalList("excludeSobjects", new TypeReference<List<String>>(){})
      .optionalList("ignoreFields", new TypeReference<List<String>>(){})
      .optionalParent("queries", new TypeReference<Map<String, String>>(){})
      .optionalBoolean("includeStandard", "includeCustom", "includeDeleted", "includeBinary")
      .optionalNumber("batchSize", "limit", "requestTimeoutMs", "maxRetries")
      .build();

  private final SalesforceClient client;
  private final List<String> sobjects;
  private final Set<String> excludeLower;
  private final Set<String> ignoreLower;
  private final Map<String, String> queries;
  private final boolean includeStandard;
  private final boolean includeCustom;
  private final boolean explicitSelection;
  private final String where;
  private final String idField;
  private final String sobjectField;
  private final boolean includeBinary;
  private final Integer limit;
  private final int maxSoqlLength;
  private final int idClauseBudget;
  private boolean connected;

  public SalesforceConnector(Config config) {
    this(config, null, Soql.MAX_QUERY_LENGTH, Soql.ID_CLAUSE_BUDGET);
  }

  /** Supplies the client and the query-length limits. Tests use this to avoid the network and to force field chunking. */
  SalesforceConnector(Config config, SalesforceClient client, int maxSoqlLength, int idClauseBudget) {
    super(config);
    if (getPipelineName() == null || getPipelineName().isBlank()) {
      throw new IllegalArgumentException("SalesforceConnector requires a pipeline.");
    }

    SalesforceAuth.Settings auth = SalesforceAuth.from(config);

    this.sobjects = readNames(config, "sobjects");
    if (config.hasPath("sobjects") && sobjects.isEmpty()) {
      throw new IllegalArgumentException(
          "sobjects is empty. Omit it to crawl every queryable object, or name at least one object.");
    }
    this.excludeLower = new HashSet<>();
    for (String name : readNames(config, "excludeSobjects")) {
      excludeLower.add(name.toLowerCase(Locale.ROOT));
    }
    this.queries = readQueries(config);
    this.includeStandard = ConfigUtils.getBooleanOrDefault(config, "includeStandard", true);
    this.includeCustom = ConfigUtils.getBooleanOrDefault(config, "includeCustom", true);
    this.explicitSelection = !sobjects.isEmpty() || !queries.isEmpty();
    if (!includeStandard && !includeCustom && !explicitSelection) {
      throw new IllegalArgumentException(
          "includeStandard and includeCustom are both false, and neither sobjects nor queries names an object to crawl.");
    }

    this.where = Soql.normalizeWhere(ConfigUtils.getStringOrDefault(config, "where", null));
    this.idField = ConfigUtils.getStringOrDefault(config, "idField", "Id");
    if (!Soql.isIdentifier(idField)) {
      throw new IllegalArgumentException("idField must be a Salesforce field API name.");
    }
    String configuredSobjectField = ConfigUtils.getStringOrDefault(config, "sobjectField", DEFAULT_SOBJECT_FIELD);
    this.sobjectField = blankToNull(configuredSobjectField);
    if (sobjectField != null && Document.RESERVED_FIELDS.contains(sobjectField)) {
      throw new IllegalArgumentException("sobjectField \"" + sobjectField + "\" is a reserved field name.");
    }
    if (sobjectField != null && sobjectField.equals(idField)) {
      throw new IllegalArgumentException("sobjectField must be different from idField.");
    }

    this.ignoreLower = new HashSet<>();
    if (config.hasPath("ignoreFields")) {
      for (String field : config.getStringList("ignoreFields")) {
        if (field == null || field.isBlank()) {
          throw new IllegalArgumentException("ignoreFields must not contain a blank name.");
        }
        if ("id".equals(field.toLowerCase(Locale.ROOT))) {
          throw new IllegalArgumentException("ignoreFields must not include Id.");
        }
        ignoreLower.add(field.toLowerCase(Locale.ROOT));
      }
    }
    this.includeBinary = ConfigUtils.getBooleanOrDefault(config, "includeBinary", false);
    boolean includeDeleted = ConfigUtils.getBooleanOrDefault(config, "includeDeleted", false);

    int batchSize = ConfigUtils.getIntOrDefault(config, "batchSize", DEFAULT_BATCH_SIZE);
    if (batchSize < 200 || batchSize > 2000) {
      throw new IllegalArgumentException("batchSize must be from 200 to 2000, got " + batchSize + ".");
    }
    Integer configuredLimit = ConfigUtils.getIntOrDefault(config, "limit", null);
    if (configuredLimit != null && configuredLimit <= 0) {
      throw new IllegalArgumentException("limit must be positive, got " + configuredLimit + ".");
    }
    this.limit = configuredLimit;
    if (maxSoqlLength < 1) {
      throw new IllegalArgumentException("maxSoqlLength must be positive.");
    }
    this.maxSoqlLength = maxSoqlLength;
    this.idClauseBudget = idClauseBudget;

    this.client = client != null ? client : auth.open(batchSize, includeDeleted);
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    connect();
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    if (publisher == null) {
      throw new ConnectorException("SalesforceConnector requires a pipeline to publish documents to.");
    }
    connect();
    List<SObjectSummary> selected = selectObjects();
    long customCount = selected.stream().filter(SObjectSummary::custom).count();
    log.info("Crawling {} Salesforce objects ({} standard, {} custom) at {}.",
        selected.size(), selected.size() - customCount, customCount, client.instanceUrl());

    int published = 0;
    int skippedRecords = 0;
    int skippedObjects = 0;
    int completedObjects = 0;
    for (SObjectSummary object : selected) {
      try {
        CrawlStats stats = crawlObject(object, publisher);
        published += stats.published;
        skippedRecords += stats.skipped;
        completedObjects++;
      } catch (ConnectorException e) {
        if (explicitSelection) {
          updateMessage(published, skippedRecords, skippedObjects, completedObjects);
          throw e;
        }
        skippedObjects++;
        log.warn("Skipping Salesforce object {}: {}", object.name(), e.getMessage());
        log.debug("Salesforce object {} failed.", object.name(), e);
      }
    }
    updateMessage(published, skippedRecords, skippedObjects, completedObjects);
    log.info(getMessage());
  }

  @Override
  public void close() throws ConnectorException {
    try {
      client.close();
    } catch (IOException e) {
      throw new ConnectorException("Error closing Salesforce client.", e);
    }
  }

  private void connect() throws ConnectorException {
    if (connected) {
      return;
    }
    try {
      client.connect();
    } catch (IOException e) {
      throw new ConnectorException("Unable to connect to Salesforce: " + e.getMessage(), e);
    }
    connected = true;
    log.info("Connected to Salesforce at {}.", client.instanceUrl());
  }

  private List<SObjectSummary> selectObjects() throws ConnectorException {
    List<SObjectSummary> listed;
    try {
      listed = client.listSObjects();
    } catch (IOException e) {
      throw new ConnectorException("Unable to list Salesforce objects: " + e.getMessage(), e);
    }
    Map<String, SObjectSummary> byLower = new LinkedHashMap<>();
    for (SObjectSummary object : listed) {
      if (object.name() == null || object.name().isBlank()) {
        continue;
      }
      byLower.putIfAbsent(object.name().toLowerCase(Locale.ROOT), object);
    }

    List<SObjectSummary> selected = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    if (!sobjects.isEmpty()) {
      for (String name : sobjects) {
        addExplicit(name, byLower, seen, selected);
      }
    } else if (!queries.isEmpty()) {
      for (String name : queries.keySet()) {
        addExplicit(name, byLower, seen, selected);
      }
    } else {
      for (SObjectSummary object : byLower.values()) {
        if (includeDiscovered(object)) {
          selected.add(object);
        }
      }
    }
    checkQueryKeys(byLower, selected);
    return selected;
  }

  private void addExplicit(String name, Map<String, SObjectSummary> byLower, Set<String> seen,
      List<SObjectSummary> selected) throws ConnectorException {
    SObjectSummary object = byLower.get(name.toLowerCase(Locale.ROOT));
    if (object == null) {
      throw new ConnectorException("Salesforce object not found: " + name);
    }
    if (!seen.add(object.name().toLowerCase(Locale.ROOT))) {
      log.warn("Salesforce object {} is listed more than once; crawling it once.", object.name());
      return;
    }
    if (!object.queryable()) {
      throw new ConnectorException("Salesforce object " + object.name() + " is not queryable.");
    }
    if (!Soql.isIdentifier(object.name())) {
      throw new ConnectorException("Salesforce object " + object.name() + " is not a SOQL identifier.");
    }
    if (excludeLower.contains(object.name().toLowerCase(Locale.ROOT))) {
      throw new ConnectorException("Salesforce object " + object.name()
          + " is listed in sobjects or queries and also in excludeSobjects.");
    }
    selected.add(object);
  }

  private boolean includeDiscovered(SObjectSummary object) {
    if (!object.queryable() || object.deprecatedAndHidden() || object.interfaceType()) {
      return false;
    }
    if (!Soql.isIdentifier(object.name())) {
      log.warn("Skipping Salesforce object {} because its name is not a SOQL identifier.", object.name());
      return false;
    }
    if (excludeLower.contains(object.name().toLowerCase(Locale.ROOT))) {
      return false;
    }
    if (object.custom()) {
      return includeCustom;
    }
    return includeStandard;
  }

  private void checkQueryKeys(Map<String, SObjectSummary> byLower, List<SObjectSummary> selected)
      throws ConnectorException {
    Set<String> selectedLower = new HashSet<>();
    for (SObjectSummary object : selected) {
      selectedLower.add(object.name().toLowerCase(Locale.ROOT));
    }
    for (String key : queries.keySet()) {
      SObjectSummary match = byLower.get(key.toLowerCase(Locale.ROOT));
      if (match == null) {
        throw new ConnectorException("queries names \"" + key + "\", but Salesforce has no such object.");
      }
      if (!selectedLower.contains(match.name().toLowerCase(Locale.ROOT))) {
        throw new ConnectorException("queries names \"" + key + "\", but that object is not part of this crawl.");
      }
    }
  }

  private CrawlStats crawlObject(SObjectSummary object, Publisher publisher) throws ConnectorException {
    log.info("Crawling Salesforce object {} ({}).", object.name(), object.custom() ? "custom" : "standard");
    List<FieldSummary> described;
    try {
      described = client.describeFields(object.name());
    } catch (IOException e) {
      throw new ConnectorException("Unable to describe Salesforce object " + object.name() + ": " + e.getMessage(), e);
    }
    Map<String, String> types = new HashMap<>();
    for (FieldSummary field : described) {
      if (field.name() != null) {
        types.put(field.name(), field.type());
      }
    }
    String override = queryFor(object);
    if (override != null) {
      return pageThrough(object, override, types, List.of(), publisher);
    }
    List<String> fields = selectableFields(object.name(), described, ignoreLower, includeBinary, sobjectField);
    if (!fields.contains(idField)) {
      throw new ConnectorException("idField \"" + idField + "\" is not a queryable field of " + object.name()
          + ". It may be listed in ignoreFields, or it may be a compound or binary field.");
    }
    List<List<String>> chunks;
    try {
      chunks = Soql.chunkFields(object.name(), fields, where, limit, maxSoqlLength, idClauseBudget);
    } catch (IllegalArgumentException e) {
      throw new ConnectorException(e.getMessage(), e);
    }
    if (chunks.size() > 1) {
      log.info("Salesforce object {} has {} fields; reading them in {} queries and merging on Id.",
          object.name(), fields.size(), chunks.size());
    }
    String soql = Soql.select(object.name(), chunks.get(0), where, limit);
    return pageThrough(object, soql, types, chunks.subList(1, chunks.size()), publisher);
  }

  private String queryFor(SObjectSummary object) {
    for (Map.Entry<String, String> entry : queries.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(object.name())) {
        return entry.getValue();
      }
    }
    return null;
  }

  private CrawlStats pageThrough(SObjectSummary object, String soql, Map<String, String> types,
      List<List<String>> extraChunks, Publisher publisher) throws ConnectorException {
    QueryPage page = runQuery(object.name(), soql);
    if (page.totalSize() >= 0) {
      log.info("Salesforce object {} matches {} record{}.", object.name(), page.totalSize(),
          page.totalSize() == 1 ? "" : "s");
    }
    int published = 0;
    int skipped = 0;
    while (true) {
      List<JsonNode> records = page.records() == null ? List.of() : page.records();
      if (!extraChunks.isEmpty() && !records.isEmpty()) {
        enrich(object.name(), extraChunks, records);
      }
      CrawlStats stats = publishRecords(object.name(), records, types, publisher);
      published += stats.published;
      skipped += stats.skipped;
      if (!hasMore(page)) {
        break;
      }
      page = runQueryMore(object.name(), page.nextRecordsUrl());
    }
    log.info("Published {} document{} from Salesforce object {}.", published, published == 1 ? "" : "s", object.name());
    return new CrawlStats(published, skipped);
  }

  private void enrich(String objectName, List<List<String>> extraChunks, List<JsonNode> records) throws ConnectorException {
    Map<String, ObjectNode> byId = new LinkedHashMap<>();
    for (JsonNode record : records) {
      if (!(record instanceof ObjectNode objectNode)) {
        throw new ConnectorException("Salesforce returned a non-object record for " + objectName + ".");
      }
      String id = text(objectNode.get("Id"));
      if (Document.isValidId(id)) {
        byId.putIfAbsent(id, objectNode);
      }
    }
    if (byId.isEmpty()) {
      return;
    }
    List<String> ids = List.copyOf(byId.keySet());
    for (List<String> chunk : extraChunks) {
      List<String> idQueries;
      try {
        idQueries = Soql.queriesForIds(objectName, chunk, ids, maxSoqlLength);
      } catch (IllegalArgumentException e) {
        throw new ConnectorException(e.getMessage(), e);
      }
      for (String soql : idQueries) {
        QueryPage page = runQuery(objectName, soql);
        while (true) {
          for (JsonNode row : page.records() == null ? List.<JsonNode>of() : page.records()) {
            if (!row.isObject()) {
              continue;
            }
            ObjectNode target = byId.get(text(row.get("Id")));
            if (target == null) {
              continue;
            }
            for (Map.Entry<String, JsonNode> entry : row.properties()) {
              if (!"attributes".equals(entry.getKey()) && !"Id".equals(entry.getKey())) {
                target.set(entry.getKey(), entry.getValue());
              }
            }
          }
          if (!hasMore(page)) {
            break;
          }
          page = runQueryMore(objectName, page.nextRecordsUrl());
        }
      }
    }
  }

  private CrawlStats publishRecords(String objectName, List<JsonNode> records, Map<String, String> types,
      Publisher publisher) throws ConnectorException {
    int published = 0;
    int skipped = 0;
    for (JsonNode record : records) {
      if (record == null || !record.isObject()) {
        skipped++;
        log.error("Skipping a {} result that is not a record.", objectName);
        continue;
      }
      String sourceId = text(record.get(idField));
      if (!Document.isValidId(sourceId)) {
        skipped++;
        log.error("Skipping {} record: its {} is null or blank.", objectName, idField);
        continue;
      }
      Document doc = Document.create(createDocId(sourceId));
      copyFields(objectName, doc, record, types);
      if (sobjectField != null) {
        if (doc.has(sobjectField)) {
          throw new ConnectorException("A " + objectName + " record already has a field named \"" + sobjectField
              + "\". Set sobjectField to another name.");
        }
        doc.setField(sobjectField, objectName);
      }
      try {
        publisher.publish(doc);
      } catch (Exception e) {
        throw new ConnectorException("Unable to publish Salesforce record " + sourceId + ".", e);
      }
      published++;
    }
    return new CrawlStats(published, skipped);
  }

  private void copyFields(String objectName, Document doc, JsonNode record, Map<String, String> types)
      throws ConnectorException {
    for (Map.Entry<String, JsonNode> entry : record.properties()) {
      String name = entry.getKey();
      if ("attributes".equals(name)) {
        continue;
      }
      JsonNode value = entry.getValue();
      if (value == null || value.isNull()) {
        continue;
      }
      if (Document.RESERVED_FIELDS.contains(name)) {
        throw new ConnectorException("Field \"" + name + "\" on Salesforce object " + objectName
            + " is reserved. Add it to ignoreFields, or leave it out of the SOQL in queries.");
      }
      if (value.isArray()) {
        copyArray(doc, name, value, types.get(name));
      } else if (value.isObject()) {
        doc.setField(name, value);
      } else {
        Object converted = SalesforceValues.toFieldValue(value, types.get(name));
        if (converted != null) {
          doc.setField(name, converted);
        }
      }
    }
  }

  private static void copyArray(Document doc, String name, JsonNode value, String salesforceType) {
    if (value.isEmpty()) {
      return;
    }
    if (!scalarArray(value)) {
      doc.setField(name, value);
      return;
    }
    boolean first = true;
    for (JsonNode item : value) {
      Object converted = SalesforceValues.toFieldValue(item, salesforceType);
      if (converted == null) {
        continue;
      }
      if (first) {
        doc.setField(name, converted);
        first = false;
      } else {
        doc.addToField(name, converted);
      }
    }
  }

  private static boolean scalarArray(JsonNode node) {
    for (JsonNode item : node) {
      if (item != null && (item.isObject() || item.isArray())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Field API names to put in a generated SELECT. {@code Id} is first. Compound types and, unless requested, base64
   * fields are omitted. A reserved field name fails the object so the caller can list it in {@code ignoreFields}.
   */
  static List<String> selectableFields(String objectName, List<FieldSummary> described, Set<String> ignoredLower,
      boolean includeBinary, String sobjectField) throws ConnectorException {
    boolean sawId = false;
    List<String> names = new ArrayList<>();
    names.add("Id");
    for (FieldSummary field : described) {
      if (field == null || field.name() == null) {
        continue;
      }
      if ("Id".equals(field.name())) {
        sawId = true;
        continue;
      }
      if (field.deprecatedAndHidden()) {
        continue;
      }
      if (ignoredLower.contains(field.name().toLowerCase(Locale.ROOT))) {
        continue;
      }
      if (!Soql.isIdentifier(field.name())) {
        log.warn("Skipping field {} on {} because it is not a SOQL identifier.", field.name(), objectName);
        continue;
      }
      String type = field.type() == null ? "" : field.type().toLowerCase(Locale.ROOT);
      if (NON_SELECTABLE_TYPES.contains(type)) {
        continue;
      }
      if (!includeBinary && "base64".equals(type)) {
        continue;
      }
      if (Document.RESERVED_FIELDS.contains(field.name())) {
        throw new ConnectorException("Field \"" + field.name() + "\" on Salesforce object " + objectName
            + " is reserved. Add it to ignoreFields.");
      }
      if (sobjectField != null && sobjectField.equalsIgnoreCase(field.name())) {
        throw new ConnectorException("sobjectField \"" + sobjectField + "\" collides with a field on " + objectName
            + ". Set sobjectField to another name.");
      }
      names.add(field.name());
    }
    if (!sawId) {
      throw new ConnectorException("Salesforce describe for " + objectName + " did not include an Id field.");
    }
    return names;
  }

  private QueryPage runQuery(String objectName, String soql) throws ConnectorException {
    log.debug("Querying Salesforce object {} with: {}", objectName, soql);
    try {
      return client.query(soql);
    } catch (IOException e) {
      throw new ConnectorException("Query failed for Salesforce object " + objectName + ": " + e.getMessage(), e);
    }
  }

  private QueryPage runQueryMore(String objectName, String nextRecordsUrl) throws ConnectorException {
    try {
      return client.queryMore(nextRecordsUrl);
    } catch (IOException e) {
      throw new ConnectorException("Query failed for Salesforce object " + objectName + ": " + e.getMessage(), e);
    }
  }

  private static boolean hasMore(QueryPage page) {
    return page != null && !page.done() && page.nextRecordsUrl() != null && !page.nextRecordsUrl().isBlank();
  }

  private static String text(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asText();
  }

  private void updateMessage(int published, int skippedRecords, int skippedObjects, int objects) {
    setMessage("Published " + published + " documents from " + objects
        + " Salesforce objects; skipped " + skippedRecords + " records and " + skippedObjects + " objects.");
  }

  private static List<String> readNames(Config config, String path) {
    if (!config.hasPath(path)) {
      return List.of();
    }
    List<String> names = new ArrayList<>();
    for (String name : config.getStringList(path)) {
      if (name == null || name.isBlank() || !Soql.isIdentifier(name)) {
        throw new IllegalArgumentException(path + " entry \"" + name + "\" is not a Salesforce API name.");
      }
      names.add(name);
    }
    return names;
  }

  private static Map<String, String> readQueries(Config config) {
    Map<String, String> queries = new LinkedHashMap<>();
    if (!config.hasPath("queries")) {
      return queries;
    }
    Config queriesConfig = config.getConfig("queries");
    if (queriesConfig.root().isEmpty()) {
      throw new IllegalArgumentException("queries is empty. Omit it, or map at least one object API name to a SOQL statement.");
    }
    for (String key : queriesConfig.root().keySet()) {
      if (!Soql.isIdentifier(key)) {
        throw new IllegalArgumentException("queries key \"" + key + "\" is not a Salesforce object API name.");
      }
      String soql = queriesConfig.getString(key);
      if (soql == null || soql.isBlank()) {
        throw new IllegalArgumentException("queries." + key + " is blank.");
      }
      queries.put(key, soql.trim());
    }
    return queries;
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  private record CrawlStats(int published, int skipped) {
  }
}
