---
title: RSS Connector
weight: 6
date: 2025-06-06
description: A Connector that publishes Documents representing items found in an RSS feed. 
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/RSSConnector.java)

`com.kmwllc.lucille.connector.RSSConnector`

The `RSSConnector` reads an RSS feed and publishes a Document for each item in it. Each Document contains the fields found on the RSS item, like the author, description, title, etc. By default, the Document ID is the item's `guid`, which should be a unique identifier for the RSS item.

You can configure the `RSSConnector` to only publish recent RSS items, based on the `pubDate` found on the items. It can also run incrementally, re-reading the feed every `refreshIncrement` until `runDuration` has passed. On each refresh, items that were already in the feed on the previous refresh are not published again. An item only counts as already seen if all of its fields (title, description, content, link, author, categories, guid and so on) and the feed's channel-level fields (such as its `pubDate` and `lastBuildDate`) are unchanged, so an edited item is published again, and a feed that changes its channel `lastBuildDate` on every refresh has all of its items published again. Items are compared only with the immediately previous refresh, so an item that leaves the feed and comes back is published again. If a refresh returns no items, the previous refresh's items are kept for the comparison.

## Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `rssURL` | String | Yes | URL of the RSS feed. A malformed URL fails at startup. |
| `useGuidForDocID` | Boolean | No | Use the item's `guid` as the Document ID. When `false`, or when an item has no `guid` or a blank one, a random UUID is used, so the same item gets a new ID each time it is published. Default: `true`. |
| `pubDateCutoff` | String | No | Duration (e.g., `"1h"`, `"2d"`). Only items with a `pubDate` within this period before the current refresh are published. Items without a `pubDate` are always published, with a warning. An invalid duration fails at startup. By default, all items are published. |
| `runDuration` | String | No | Duration. Total time to keep refreshing the feed. Must be set together with `refreshIncrement`, checked at startup. An invalid duration fails at startup. |
| `refreshIncrement` | String | No | Duration. Time to wait after one refresh finishes before starting the next. Must be set together with `runDuration`. An invalid duration fails at startup. |

Without `runDuration`/`refreshIncrement`, the feed is read once. In incremental mode, the connector stops after the first refresh that finishes once `runDuration` has elapsed, or when it is interrupted. An error reading the feed (for example an unreachable URL), on the first read or any later refresh, fails the run; there is no retry. The connector's `docIdPrefix` is not applied to the Document IDs.

```hocon
{
  name: "rss-connector"
  class: "com.kmwllc.lucille.connector.RSSConnector"
  pipeline: "my-pipeline"
  rssURL: "https://example.com/feed.xml"
  pubDateCutoff: "7d"
  runDuration: "1h"
  refreshIncrement: "5m"
}
```

## Document Fields

The Documents published may have any of the following fields, depending on how the RSS feed is structured. Fields missing from an item are not set.

* `author` (String)
* `categories` (List&lt;String&gt;)
* `comments` (String)
* `content` (String)
* `description` (String)
* `enclosures` (List&lt;JsonNode&gt;). Each JsonNode contains:
  * `type` (String)
  * `url` (String)
  * May contain `length` (Long)
* `guid` (String)
* `isPermaLink` (Boolean)
* `link` (String)
* `title` (String)
* `pubDate` (Instant)
