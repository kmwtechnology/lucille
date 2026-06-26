---
title: QueryOpensearch
weight: 5
date: 2025-06-06
description: Execute an OpenSearch Template using information from a Document, and add the response to it.
---

`com.kmwllc.lucille.stage.QueryOpensearch`

### OpenSearch Templates

You can use templates in OpenSearch to repeatedly run a certain query using different parameters. For example,
if we have an index full of parks, and we want to search for a certain park, we might use a template like this:

```json
{
  "source": {
    "query": {
      "match_phrase": {
        "park_name": "{{park_to_search}}"
      }
    }
  }
}
```

In Opensearch, you could then call this template (providing it `park_to_search`) instead of writing out the full query each time you want to search.

Templates can also have default values. For example, if you want `park_to_search` to default to "Central Park" when a value is not provided,
it would be written as: `"park_name": "{{park_to_search}}{{^park_to_search}}Central Park{{/park_to_search}}"`

### QueryOpensearch Stage

The `QueryOpensearch` Stage executes a search template using certain fields from a Document as your parameters and adding OpenSearch's response to the Document.
You'll specify either `templateName`, the name of a search template you've saved, or `searchTemplate`, the template you want to execute, in your Config.

You'll also need to specify the names of parameters in your search template. These will need to match the names of fields on your Documents. 
If your names don't match, you can use the `RenameFields` Stage first. 

In particular, you have to specify which parameters are required and which are optional. If a required name in `requiredParamNames` is 
missing from a Document, an Exception will be thrown, and the template will not be executed. If an optional name in `optionalParamNames`
is missing they (naturally) won't be part of the template execution, so the default value will be used by OpenSearch.

If a parameter without a default value is missing, OpenSearch doesn't throw an Exception - it just returns an empty response with zero hits.
So, it is very important that `requiredParamNames` and `optionalParamNames` are defined very carefully!

### Connection and Authentication

The stage connects to OpenSearch via the same `opensearch` config block used by the OpenSearch Indexer. Authentication and TLS are handled by `OpenSearchUtils`:

- **Basic auth:** Embed credentials in the URL: `https://username:password@host:9200`
- **Self-signed certs:** Set `acceptInvalidCert: true` to skip certificate validation (development only).

```hocon
{
  name: "QueryOpensearch-Example"
  class: "com.kmwllc.lucille.stage.QueryOpensearch"

  opensearch {
    url: "https://admin:admin@localhost:9200"
    index: "my-index"
    acceptInvalidCert: true
  }

  templateName: "my-saved-template"
  requiredParamNames: ["park_to_search"]
  optionalParamNames: ["city"]
  destinationField: "search_results"
  opensearchResponsePath: "/hits/hits"
}
```

### Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `opensearch.url` | String | Yes | OpenSearch endpoint URL, optionally including credentials. |
| `opensearch.index` | String | Yes | Index to query against. |
| `opensearch.acceptInvalidCert` | Boolean | No | Accept invalid TLS certificates. Default: `false`. |
| `templateName` | String | One of these | Name of a saved search template in the cluster. |
| `searchTemplate` | String | is required | Inline template body (not saved to the cluster). |
| `requiredParamNames` | List\<String\> | No | Document fields required for the template. Missing fields cause a warning and an error field on the document. |
| `optionalParamNames` | List\<String\> | No | Document fields used if present; omitted from the query otherwise. |
| `opensearchResponsePath` | String | No | JsonPointer path into the response (e.g., `/hits/hits`). Defaults to the entire response. |
| `destinationField` | String | No | Field name to write the response value to. Default: `"response"`. |