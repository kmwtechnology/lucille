---
title: FetchUri
weight: 360
date: 2025-06-09
description: Fetches the content of a URL stored in a document field and stores the response as a byte array.
---

`com.kmwllc.lucille.stage.FetchUri`

Sends an HTTP GET request to the URL in `source` and writes the response body to `dest` as a byte array, overwriting any existing value. Documents where `source` is missing or empty are passed through unchanged. Only the first value of `source` is used.

After a response, the stage also sets `<source>_<statusSuffix>` to the HTTP status code and `<source>_<sizeSuffix>` to the number of bytes read. The body is stored for any status code, including errors, unless the code is in `statusCodeRetryList`. If the URL is malformed or the request fails, `<source>_<errorSuffix>` is set to the exception class and message, and `dest` is not set. The document is not failed in either case.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the URL to fetch. |
| `dest` | String | Yes | Field to write the response bytes to. |
| `headers` | Map\<String, String\> | No | HTTP headers to send with each request. |
| `maxSize` | Integer | No | Maximum number of bytes to read from the response body. A longer body is truncated without an error. Default: `2147483647`. |
| `sizeSuffix` | String | No | Suffix of the field holding the response size. Default: `size`. |
| `statusSuffix` | String | No | Suffix of the field holding the status code. Default: `status_code`. |
| `errorSuffix` | String | No | Suffix of the field holding the error message. Default: `error`. |
| `maxRetries` | Integer | No | Number of times a failed request is retried. Default: `0`. |
| `initialExpiryMs` | Integer | No | Delay in milliseconds before the first retry. The delay doubles on each further retry, up to `maxExpiryMs`. Default: `100`. |
| `maxExpiryMs` | Integer | No | Maximum delay in milliseconds between retries. Default: `10000`. |
| `statusCodeRetryList` | List\<String\> | No | Status codes that are treated as failures and retried, such as `429`. `x` is a wildcard digit: `5xx` matches all 5xx codes and `50x` matches 500 to 509. Values that are not codes from 100 to 599 are dropped with a warning. `200`, `20x` and `2xx` are not allowed and fail the stage at startup. With `maxRetries` of `0`, a matching response is not retried but is still treated as a failed request. Default: empty. |
| `connectionRequestTimeout` | Integer | No | Timeout in milliseconds for getting a connection from the connection pool. Default: `60000`. |
| `connectTimeout` | Integer | No | Timeout in milliseconds for establishing a connection. Default: `60000`. |
| `socketTimeout` | Integer | No | Timeout in milliseconds while waiting for data. Default: `60000`. |

```hocon
{
  class: "com.kmwllc.lucille.stage.FetchUri"
  source: "url"
  dest: "page_content"
  maxRetries: 3
  statusCodeRetryList: ["429", "5xx"]
  headers: { "User-Agent": "lucille" }
}
```
