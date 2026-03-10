---
title: RSS Cookbook
date: 2026-03-10
description: A guide to using the RSS Connector in Lucille.
---

### RSS to CSV

Let's say we wanted to read from an RSS feed into a CSV using Lucille. We can set this up in the following manner:

1. Create a .conf file to configure Lucille
2. Specify the RSS connector in the connectors section of the config:

```hocon
connectors: [
  {
    name: "RSSConnector"
    pipeline: "rssPipeline"
    class: "com.kmwllc.lucille.connector.RSSConnector"
    rssURL: "https://www.cnbc.com/id/15837362/device/rss/rss.html"
  }
]
```

There are a few additional configuration options that we won't use here, but are useful:

```hocon
    useGuidForDocID: true       # default; set false to use UUID as ID instead
    pubDateCutoff: "24h"        # only publish items from the last 24 hours
    runDuration: "1h"           # run incrementally for 1 hour total
    refreshIncrement: "5m"      # re-fetch the feed every 5 minutes
```

Your pipeline name can be whatever you want. For our URL, we chose CNBC's RSS feed.

3. Define what stages we would like to use to process our documents from the feed:

```hocon
pipelines: [
  {
    name: "rssPipeline"
    stages: [
      {
        name: "fetchURI",
        class: "com.kmwllc.lucille.stage.FetchUri"
        source: "link"
        dest: "content"
      }
      {
        name: "ApplyJSoup"
        class: "com.kmwllc.lucille.stage.ApplyJSoup"
        byteArrayField: "content"
        destinationFields: {
          paragraphTexts: {
            type: "text",
            selector: ".ArticleBody-articleBody p"
          }
          bulletPoints: {
            type: "text",
            selector: ".RenderKeyPoints-list li"
          }
          headline: {
            type: "text"
            selector: "h1"
          }
        }
      }
    ]
  }
]
```

News items in an RSS feed often have some article metadata, and then a link to the actual meat of the article in HTML as a field.

- The fetchURI stage allows us to grab the actual content of our associated news article.
- The ApplyJSoup stage parses that content into fields that will exist in addition to our article metadata from the RSS feed. These fields include the body, bullet points, and the header.

4. We can index these documents into whatever we'd like. Here, we might decide to just print them to a CSV:

```hocon
indexer: {
  type: "csv"
}

csv: {
  path: "./rss_results.csv"
  columns: ["id", "link", "title", "description", "paragraphTexts",
    "bulletPoints", "headline"]
}
```

Here is the full config file:

```hocon
connectors: [
  {
    name: "RSSConnector"
    pipeline: "rssPipeline"
    class: "com.kmwllc.lucille.connector.RSSConnector"
    rssURL: "https://www.cnbc.com/id/15837362/device/rss/rss.html"
  }
]

pipelines: [
  {
    name: "rssPipeline"
    stages: [
      {
        name: "fetchURI",
        class: "com.kmwllc.lucille.stage.FetchUri"
        source: "link"
        dest: "content"
      }
      {
        name: "ApplyJSoup"
        class: "com.kmwllc.lucille.stage.ApplyJSoup"
        byteArrayField: "content"
        destinationFields: {
          paragraphTexts: {
            type: "text",
            selector: ".ArticleBody-articleBody p"
          }
          bulletPoints: {
            type: "text",
            selector: ".RenderKeyPoints-list li"
          }
          headline: {
            type: "text"
            selector: "h1"
          }
        }
      }
    ]
  }
]

indexer: {
  type: "csv"
}

csv: {
  path: "./rss_results.csv"
  columns: ["id", "link", "title", "description", "paragraphTexts",
    "bulletPoints", "headline"]
}
```
The CSV file will thus be saved on disk.

### RSS to OpenSearch

We might also choose to index into another destination, like an OpenSearch index. Here's an example. Replace the config with this:

```hocon
connectors: [
  {
    name: "RSSConnector"
    pipeline: "rssPipeline"
    class: "com.kmwllc.lucille.connector.RSSConnector"
    rssURL: "https://www.cnbc.com/id/15837362/device/rss/rss.html"
    refreshIncrement: "60s"
    runDuration: "1h"
  }
]

pipelines: [
  {
    name: "rssPipeline"
    stages: []
  }
]

indexer: {
  type: "opensearch"
}

opensearch: {
  url: <Your OpenSearch URL>
  index: "rss-index"
  acceptInvalidCert: true
}
```
You'll notice we're using incremental mode now, with a `refreshIncrement` of 60s and a `runDuration` of 1h. This means that every 
item in the feed will be indexed on the initial run, and then Lucille will continue to take in new items that pop up every 60 
seconds for 1 hour total.


Run Lucille again. Here's what 3 of our documents look like after being indexed into OpenSearch:

```
GET /rss-index/_search
{
  "size": 3
}
```

```json
{
  "took": 15,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 30,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": [
      {
        "_index": "rss-index",
        "_id": "108275704",
        "_score": 1,
        "_source": {
          "id": "108275704",
          "guid": "108275704",
          "isPermaLink": false,
          "link": "https://www.cnbc.com/2026/03/09/watch-live-trump-press-conference-iran-war-oil-hormuz-doral.html",
          "title": "Watch live: Trump holds press conference as Iran war fallout roils oil market",
          "pubDate": "2026-03-09T21:13:25Z",
          "run_id": "cb234bd2-fdf7-4b88-be13-20de36cd059e"
        }
      },
      {
        "_index": "rss-index",
        "_id": "108275619",
        "_score": 1,
        "_source": {
          "id": "108275619",
          "description": "The OpenAI deal fallout exposes the fundamental danger of being the most leveraged player in a market where the chip cycle moves faster than the concrete dries.",
          "guid": "108275619",
          "isPermaLink": false,
          "link": "https://www.cnbc.com/2026/03/09/oracle-is-building-yesterdays-data-centers-with-tomorrows-debt.html",
          "title": "Oracle is building yesterday's data centers with tomorrow's debt",
          "pubDate": "2026-03-09T20:52:19Z",
          "run_id": "cb234bd2-fdf7-4b88-be13-20de36cd059e"
        }
      },
      {
        "_index": "rss-index",
        "_id": "108275649",
        "_score": 1,
        "_source": {
          "id": "108275649",
          "description": "U.S. stock market indexes rose on the heels of reported comments by President Donald Trump that the war against Iran could be over sooner than first expected.",
          "guid": "108275649",
          "isPermaLink": false,
          "link": "https://www.cnbc.com/2026/03/09/trump-iran-war-end.html",
          "title": "Trump says Iran 'war is very complete,' talks to Putin, reports say",
          "pubDate": "2026-03-09T21:32:43Z",
          "run_id": "cb234bd2-fdf7-4b88-be13-20de36cd059e"
        }
      }
    ]
  }
}
```

Using other indexers with the RSS connector follows much the same pattern.