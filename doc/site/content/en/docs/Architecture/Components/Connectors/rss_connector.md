---
title: RSS Connector
date: 2025-06-06
description: A Connector that publishes Documents representing items found in an RSS feed. 
---

Wow, RSS... retro!

### The RSSConnector

The `RSSConnector` allows you to publish Documents representing the items found in an RSS feed of your choice. Each Document will 
(optionally) contain fields from the RSS items, like the author, description, title, etc. By default, the Document IDs will be the
item's `guid`, which should be a unique identifier for the RSS item. 

You can configure the `RSSConnector` to only publish recent RSS items, based on the `pubDate` found on the items. 
Also, it can run incrementally, refreshing the RSS feed after a certain amount of time until you manually stop it. The `RSSConnector`
will avoid publishing Documents for the same RSS item more than once. 

The Documents published may have any of the following fields, depending on how the RSS feed is structured:
* `author` (String)
* `categories` (List&lt;String&gt;)
* `comments` (List&lt;String&gt;)
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