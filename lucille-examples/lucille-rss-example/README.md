# RSS to CSV

This example allows you to connect to an RSS feed of your choice, publishing Documents for each item in the feed. It also
extracts text from the links referenced in the RSS items using `FetchUri` and `ApplyJSoup`.

From the `lucille-rss-example` folder, run `./scripts/run_ingest.sh` to run the RSSConnector once.

Run `./scripts/run_incremental.sh` to run the RSSConnector in incremental mode. It will check for new items every 60 seconds.
It will only publish a Document for an RSS item once.

The Documents will be indexed into a CSV, `rss_results.csv`, in the root of this folder.