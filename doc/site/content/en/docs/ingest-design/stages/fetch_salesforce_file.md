---
title: FetchSalesforceFile
weight: 352
date: 2026-11-10
description: Downloads Salesforce file bytes so TextExtractor can parse them.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-salesforce/src/main/java/com/kmwllc/lucille/salesforce/stage/FetchSalesforceFile.java)

`com.kmwllc.lucille.salesforce.stage.FetchSalesforceFile` *(requires `lucille-salesforce`)*

Downloads the bytes of a file stored in Salesforce and writes them to a byte array field. [TextExtractor]({{< relref "docs/ingest-design/stages/text_extractor" >}}) then parses Word documents, Excel files, PDFs, and any other uploaded file.

Put this stage after the [Salesforce connector]({{< relref "docs/ingest-design/connectors/salesforce_connector" >}}) and before TextExtractor. The connector publishes the file's fields and does not download the bytes. Leave the connector's `includeBinary` false.

The stage reads `sobjectField` (default `salesforce_sobject`, which the connector sets) and downloads:

| Object | Bytes | Record id used |
|---|---|---|
| `Attachment` | `Body` | `Id` |
| `Document` | `Body` | `Id` |
| `ContentVersion` | `VersionData` | `Id` |
| `ContentDocument` | `VersionData` of the latest published version | `LatestPublishedVersionId` |

Any other object, such as Account or Contact, is left unchanged, so the stage can sit on a pipeline that also carries those records. A file record with no id, or a ContentDocument with no version id, is left unchanged and a warning is logged. Do not add `LatestPublishedVersionId` to the connector's `ignoreFields`. A download that fails (including a file larger than `maxBytes`) fails that document and the pipeline continues with the next one.

The bytes go to `byteArrayField` (default `file_content`). The file name goes to `fileNameField` (default `file_name`), taken from `Name` on Attachment and Document, from the last segment of `PathOnClient` on ContentVersion, and from `Title` plus `FileExtension` on ContentDocument. The content type goes to `contentTypeField` (default `content_type`), taken from the record's `ContentType` when it has one and otherwise from the download response. A field that already has a value is not overwritten, and a document that already has `byteArrayField` is not downloaded again.

Authentication matches the connector. The stage opens its own session. Use the same `accessToken` and `instanceUrl`, or the same connected-app credentials.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-salesforce</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

TextExtractor is a separate dependency, `lucille-tika`.

```hocon
connectors: [
  {
    name: "salesforce"
    class: "com.kmwllc.lucille.salesforce.connector.SalesforceConnector"
    pipeline: "files"
    sobjects: ["ContentDocument", "Attachment"]
    clientId: ${SALESFORCE_CLIENT_ID}
    clientSecret: ${SALESFORCE_CLIENT_SECRET}
    username: ${SALESFORCE_USERNAME}
    password: ${SALESFORCE_PASSWORD}
  }
]

pipelines: [
  {
    name: "files"
    stages: [
      {
        name: "fetch-salesforce-file"
        class: "com.kmwllc.lucille.salesforce.stage.FetchSalesforceFile"
        clientId: ${SALESFORCE_CLIENT_ID}
        clientSecret: ${SALESFORCE_CLIENT_SECRET}
        username: ${SALESFORCE_USERNAME}
        password: ${SALESFORCE_PASSWORD}
      }
      {
        name: "extract-text"
        class: "com.kmwllc.lucille.tika.stage.TextExtractor"
        byteArrayField: "file_content"
        textField: "text"
        metadataHintFields: {
          resourceName: "file_name"
          "Content-Type": "content_type"
        }
      }
    ]
  }
]
```

`metadataHintFields` gives Tika the file name and content type. Without them Tika sees only the bytes.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `accessToken` | String | No | Session token. When set, `instanceUrl` is required and no login is performed. |
| `instanceUrl` | String | No | Instance the token belongs to, such as `https://mycompany.my.salesforce.com`. |
| `clientId` | String | No | Connected-app consumer key. Required unless `accessToken` is set. |
| `clientSecret` | String | No | Connected-app consumer secret. Required unless `accessToken` is set. |
| `username` | String | No | Username for the username-password flow. When set, `password` is required. When omitted, the client-credentials flow is used. |
| `password` | String | No | Password for the username-password flow, with the security token appended when the org requires one. |
| `loginUrl` | String | No | Token endpoint host. Default: `https://login.salesforce.com`. Use `https://test.salesforce.com` for a sandbox. The client-credentials flow needs the org's My Domain URL. |
| `apiVersion` | String | No | REST API version, such as `67.0` or `v67.0`. Default: `67.0`. |
| `sobjectField` | String | No | Field holding the object API name. Default: `salesforce_sobject`. |
| `objectType` | String | No | Download every document as this type instead of reading `sobjectField`. One of `Attachment`, `ContentDocument`, `ContentVersion`, `Document`. |
| `idField` | String | No | Field holding the record id. Default: `Id`. |
| `versionIdField` | String | No | ContentVersion id on a ContentDocument. Default: `LatestPublishedVersionId`. |
| `byteArrayField` | String | No | Field the file bytes are written to. Set TextExtractor's `byteArrayField` to the same name. Default: `file_content`. |
| `fileNameField` | String | No | Field the file name is written to. A blank value disables it. Default: `file_name`. |
| `contentTypeField` | String | No | Field the content type is written to. A blank value disables it. Default: `content_type`. |
| `maxBytes` | Long | No | Largest file that will be downloaded. A larger file fails that document. Must be from 1 to 2147483639. Default: `104857600` (100 MiB). |
| `requestTimeoutMs` | Integer | No | Per-request timeout in milliseconds. Default: `120000`. |
| `maxRetries` | Integer | No | Retries after a timeout, a connection failure, or HTTP 408, 429, or 5xx. Default: `3`. A 401 on a token this stage requested is refreshed once, and that refresh is not a retry. |

Salesforce often redirects a file download to a file host. The stage follows those redirects and sends the session token on each hop.
