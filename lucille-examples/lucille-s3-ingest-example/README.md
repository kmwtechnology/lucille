# S3 Indexer to OpenSearch

This example ingests the contents of an S3 bucket, chunks the text into smaller pieces, and indexes them as RAG embeddings into OpenSearch.

## Requirements

- **S3 Bucket**: An S3 bucket with a folder containing the files to be ingested.
- **OpenSearch Server**: An instance of OpenSearch set up locally using Docker. Refer to the [OpenSearch Getting Started Guide](https://opensearch.org/docs/latest/getting-started/).
- **Embedding Index**: Set up an embedding index following the [Neural Search Tutorial](https://opensearch.org/docs/latest/search-plugins/neural-search-tutorial/).

## Setup Instructions

### 1. Access the OpenSearch Dev Tools

Use the OpenSearch Dev Tools dashboard to easily create and modify indices:

- Sign in with the username and password defined in the `docker.yaml` file when setting up OpenSearch.
- Alternatively, access the Dev Tools page directly by replacing `<port>` with the port your OpenSearch dashboard is running on:
    
    http://localhost:&lt;port&gt;/app/dev_tools#/
    
### 2. Configure Environment Variables

Set up the necessary environment variables for your ingestion process:
    
```bash
export AWS_REGION="us-east-2"
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export OPENSEARCH_URL=”https://username:password@localhost:9200/”
export OPENSEARCH_INDEX=”index1”    
export PATH_TO_STORAGE=”s3://my-test-bucket/folder1”
```
    
### 3. Run the Ingestion Script
Navigate to the example folder and execute the ingestion script:
```bash
./scripts/run_ingest.sh
```
