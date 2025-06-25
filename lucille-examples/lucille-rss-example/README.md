## RSS to CSV

This example uses Lucille's RSSConnector, which connects to an RSS feed of your choice and publishes Documents for each item it
finds. This example connects to a CNBC RSS feed. In addition to collecting the information from the RSS items themselves, this
example also extracts text from the links referenced in the RSS items using `FetchUri` and `ApplyJSoup`.

From the `lucille-rss-example` folder, run `./scripts/run_ingest.sh` to run the RSSConnector once. Run `./scripts/run_incremental.sh`
to run the RSSConnector in incremental mode. It will check for new items every 60 seconds. It will only publish a Document for an RSS item once. 
The Connector will run for 2 hours, then it will terminate.

The Documents will be indexed into a CSV, `rss_results.csv`, in the root of this folder.
Columns "id", "link", "title", and "description" come from the RSS items.
Columns "paragraphTexts", "bulletPoints", "headline" come from `FetchUri` and `ApplyJSoup`.