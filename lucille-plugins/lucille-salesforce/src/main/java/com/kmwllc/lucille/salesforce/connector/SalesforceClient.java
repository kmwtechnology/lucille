package com.kmwllc.lucille.salesforce.connector;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;

/**
 * Reads Salesforce object metadata and SOQL pages. {@link SalesforceRestClient} calls the REST API; tests supply a fake.
 */
interface SalesforceClient extends AutoCloseable {

  /** Authorizes the client. Later calls fail when this has not succeeded. */
  void connect() throws IOException;

  /** Instance the session reads from, available after {@link #connect()}. */
  String instanceUrl();

  List<SObjectSummary> listSObjects() throws IOException;

  List<FieldSummary> describeFields(String sobject) throws IOException;

  QueryPage query(String soql) throws IOException;

  QueryPage queryMore(String nextRecordsUrl) throws IOException;

  @Override
  void close() throws IOException;

  /** One row of the sObject list. {@code custom} is false for Account, Contact, and the other standard objects. */
  record SObjectSummary(String name, String label, boolean custom, boolean queryable, boolean deprecatedAndHidden,
      boolean interfaceType) {
  }

  /** One field from an sObject describe. {@code type} is the Salesforce type name, such as {@code string} or {@code datetime}. */
  record FieldSummary(String name, String type, boolean deprecatedAndHidden) {
  }

  /** One page of a SOQL result. {@code nextRecordsUrl} is the path Salesforce returns when {@code done} is false. */
  record QueryPage(int totalSize, boolean done, String nextRecordsUrl, List<JsonNode> records) {
  }
}
