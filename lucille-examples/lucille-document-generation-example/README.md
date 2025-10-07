# Document Generation For Testing

This example synthesizes documents with randomized fields (text, numbers, booleans, dates, nested JSON), then either indexes
them into Elasticsearch or performs a no-op "dry run" for pipeline testing.

## Requirements

- **Elasticsearch Server**: An instance of Elasticsearch set up locally if you want to actually index docs. For a dry run, use the No-Op Indexer.

## Setup Instructions

### 1. Choose Your Indexing Mode

- **Elasticsearch Mode**: keep the Elasticsearch indexer.
  - Keep the `indexer {}` block and `elasticsearch {}` block as provided.
  - Make sure Elasticsearch is running and reachable.    
- **Dry Run**: use the No-Op indexer (no network calls).
  - In `conf/test-documents.conf`, replace the `indexer {}` block with the other No-Op `indexer {}` block.
  - You can remove the `elasticsearch {}` block.
    
### 2. Configure Environment Variables

Set any optional overrides you want:
    
```bash
# Document count (defaults to 100).
export NUM_DOCS=1000

# Only needed if using Elasticsearch mode.
export ES_URL="http://localhost:9200"
export ES_INDEX="test_docs"
export ES_SEND_ENABLED=true
```
    
### 3. Run the Ingestion Script
Navigate to the example folder and execute the ingestion script:
```bash
./scripts/run_ingest.sh
```
