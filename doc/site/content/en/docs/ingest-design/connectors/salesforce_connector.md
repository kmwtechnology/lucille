---
title: Salesforce Connector
weight: 6
date: 2026-11-09
description: A Connector that crawls Salesforce standard and custom objects and publishes each record as a Document.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-salesforce/src/main/java/com/kmwllc/lucille/salesforce/connector/SalesforceConnector.java)

`com.kmwllc.lucille.salesforce.connector.SalesforceConnector`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-salesforce</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

The Salesforce connector reads sObjects through the REST API and publishes each record as a Lucille Document. It crawls standard objects (Account, Contact, Opportunity, Lead, and the rest of the org) and custom objects (API names such as `Project__c`, and any other object Salesforce marks custom). Custom fields on those objects are included. Describe decides which fields can be selected: compound address and geolocation fields are skipped in favor of their components, and base64 fields are skipped unless `includeBinary` is true.

Name the objects in `sobjects`, or omit `sobjects` to crawl every queryable object the authenticated user can see. An object found that way which Salesforce refuses to scan without a filter is logged and skipped. An object you name in `sobjects` or `queries` fails the run instead, so a typo is not silent.

## Basic Configuration

```hocon
connectors: [
  {
    name: "salesforce"
    class: "com.kmwllc.lucille.salesforce.connector.SalesforceConnector"
    pipeline: "crm"
    docIdPrefix: "sf-"

    loginUrl: "https://login.salesforce.com"
    clientId: ${SALESFORCE_CLIENT_ID}
    clientSecret: ${SALESFORCE_CLIENT_SECRET}
    username: ${SALESFORCE_USERNAME}
    # Password with the security token appended, when the org requires one.
    password: ${SALESFORCE_PASSWORD}

    sobjects: ["Account", "Contact", "Opportunity", "Lead", "Project__c"]
  }
]
```

`pipeline` is required. The connector publishes every record it reads.

A Connected App (or External Client App) with the API scope has to exist for the org. The username-password flow above works against `https://login.salesforce.com`, or `https://test.salesforce.com` for a sandbox. To use the client-credentials flow, omit `username` and `password` and set `loginUrl` to the org's My Domain URL (`https://mycompany.my.salesforce.com`). To skip login entirely, set `accessToken` and `instanceUrl` from a session you already have.

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `accessToken` | String | No | Session token. When set, `instanceUrl` is required and no login is performed. Takes precedence over `clientId`. |
| `instanceUrl` | String | No | Instance the token belongs to, such as `https://mycompany.my.salesforce.com`. Required with `accessToken`. |
| `clientId` | String | No | Connected-app consumer key. Required unless `accessToken` is set. |
| `clientSecret` | String | No | Connected-app consumer secret. Required unless `accessToken` is set. |
| `username` | String | No | Username for the username-password flow. When set, `password` is required. When omitted, the client-credentials flow is used. |
| `password` | String | No | Password for the username-password flow. Append the Salesforce security token when the org requires one. |
| `loginUrl` | String | No | Host of the token endpoint. Default: `https://login.salesforce.com`. |
| `apiVersion` | String | No | REST API version, `67.0` or `v67.0`. Default: `67.0`. |
| `sobjects` | List\<String\> | No | API names to crawl. Standard and custom names can be mixed. Omit to discover queryable objects. |
| `excludeSobjects` | List\<String\> | No | API names skipped during discovery. Listing an object here and in `sobjects` or `queries` fails the run. |
| `includeStandard` | Boolean | No | When discovering, include standard objects. Default: `true`. Ignored when `sobjects` or `queries` names the objects. |
| `includeCustom` | Boolean | No | When discovering, include custom objects. Default: `true`. Ignored when `sobjects` or `queries` names the objects. |
| `queries` | Map\<String, String\> | No | Full SOQL statement per object API name, used instead of the generated SELECT. When `sobjects` is omitted, the keys are the objects to crawl. |
| `where` | String | No | SOQL condition appended to every generated query. A leading `WHERE` is optional. `ORDER BY` and `LIMIT` belong in `queries`, not here. Not applied to `queries`. |
| `idField` | String | No | Field whose value becomes the Document id. Default: `Id`. |
| `sobjectField` | String | No | Field set to the object API name on every document. Default: `salesforce_sobject`. A blank value disables it. |
| `ignoreFields` | List\<String\> | No | Field API names left out of generated queries. `Id` cannot be ignored. A field whose name is reserved (`id`, `run_id`, and the other reserved names) has to be listed here or the object fails. |
| `includeDeleted` | Boolean | No | Read with `queryAll`, so deleted and archived records are included. Default: `false`. Applies to generated queries and to `queries`. |
| `includeBinary` | Boolean | No | Select base64 fields in the SOQL query. Default: `false`. File bytes are downloaded by [FetchSalesforceFile]({{< relref "docs/ingest-design/stages/fetch_salesforce_file" >}}) instead. |
| `batchSize` | Integer | No | Records per page, sent as `Sforce-Query-Options`. Salesforce accepts 200 to 2000. Default: `2000`. |
| `limit` | Integer | No | `LIMIT` added to each generated query. Not applied to `queries`. |
| `requestTimeoutMs` | Integer | No | Per-request timeout in milliseconds. Default: `120000`. |
| `maxRetries` | Integer | No | Extra attempts after a timeout, a connection failure, or HTTP 408, 429, or 5xx. Default: `3`. A 401 on a token this connector requested is refreshed once, and that refresh does not consume a retry. A configured `accessToken` is not refreshed. |

`name`, `class`, `pipeline`, `docIdPrefix`, and `collapse` are the [common connector parameters]({{< relref "docs/ingest-design/connectors#common-parameters" >}}). `docIdPrefix` is prepended to the id taken from `idField`.

## Discovering every object

Omit `sobjects` and `queries` to crawl every queryable standard and custom object, skipping deprecated and interface objects:

```hocon
{
  name: "salesforce-all"
  class: "com.kmwllc.lucille.salesforce.connector.SalesforceConnector"
  pipeline: "crm"
  clientId: ${SALESFORCE_CLIENT_ID}
  clientSecret: ${SALESFORCE_CLIENT_SECRET}
  username: ${SALESFORCE_USERNAME}
  password: ${SALESFORCE_PASSWORD}
  where: "LastModifiedDate >= 2024-01-01T00:00:00Z"
  excludeSobjects: ["Task", "Event", "ContentDocumentLink"]
}
```

Objects the user cannot see are absent from the org's sObject list. Objects Salesforce will not scan without a filter are skipped with a warning. Set `includeStandard: false` or `includeCustom: false` to keep only one of the two groups.

## Supplying SOQL

`queries` replaces the generated SELECT for the objects it names. Describe is still read so datetime fields become instants. `where` and `limit` are not added to these statements.

```hocon
sobjects: ["Account", "Opportunity"]
queries: {
  Opportunity: "SELECT Id, Name, Amount, Account.Name FROM Opportunity WHERE IsClosed = false"
}
```

A relationship such as `Account.Name` is stored as a JSON object on the field `Account`.

## Document Mapping

- Each record becomes one Document. `idField` (default `Id`) becomes the Document id, with `docIdPrefix` applied. The original `Id` is also stored on the field `Id`. A record whose id is null or blank is skipped and counted in the run summary.
- `sobjectField` (default `salesforce_sobject`) is set to the object API name, so Account and `Project__c` records can be told apart. The run fails if that name is already a field on the object.
- Null fields are omitted. Booleans, integers, and decimals keep those types. Datetime values become instants. Date and time values stay strings. Nested JSON, including a subquery result, is stored as JSON. A JSON array of scalars becomes a multi-valued field.
- A field named with a reserved Document name fails that object. Add it to `ignoreFields`.
- An object whose field list does not fit in one request is read with several queries and merged on `Id` before the Document is published.

## Notes

- The authenticated user only returns objects and fields they can read. A named object that is missing or not queryable fails the run.
- Credentials belong in the environment. HOCON substitution (`${SALESFORCE_PASSWORD}`) keeps them out of the config file.
- `loginUrl` and `instanceUrl` must be `http` or `https`. Production orgs use `https`.
- Pages follow Salesforce's `nextRecordsUrl` until the result is done. `batchSize` applies to the first request of a query; Salesforce fixes the page size of the pages that follow.
- Uploaded files are `Attachment` records (legacy) or `ContentDocument` records (Salesforce Files). `ContentVersion` and the older `Document` object are files too. The connector publishes their fields and does not download the bytes. Add [FetchSalesforceFile]({{< relref "docs/ingest-design/stages/fetch_salesforce_file" >}}) and then [TextExtractor]({{< relref "docs/ingest-design/stages/text_extractor" >}}). Leave `includeBinary` false, and do not add `LatestPublishedVersionId` to `ignoreFields`: a ContentDocument is downloaded through that version id.
