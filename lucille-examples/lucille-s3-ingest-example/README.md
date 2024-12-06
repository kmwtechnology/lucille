# S3 Indexer to OpenSearch

This example ingests the contents of a S3 bucket and chunks the text into smaller pieces for RAG embeddings.

### Requirements:

- An S3 bucket with a folder containing the files to be ingested.
- An instance of OpenSearch server set up locally using Docker. Here is a link to get you started: https://opensearch.org/docs/latest/getting-started/

### Setting up:

1. Head to the OpenSearch dashboard dev tools page. This would allow us to easily create and modify any indices that we will create. Sign in using the username and password set in the docker.yaml file when downloading OpenSearch. Or use the link to the dev tools page, replacing &lt;port&gt; with your port that is running the OpenSearch dashboard
    
    http://localhost:&lt;port&gt;/app/dev_tools#/
    
2. Set up your environment variables e.g.
    
```bash
export AWS_REGION="us-east-2"
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export OPENSEARCH_URL=”https://username:password@localhost:9200/”
export OPENSEARCH_INDEX=”index1”    
export PATH_TO_STORAGE=”s3://my-test-bucket/folder1”
```
    
4. Navigate to the OpenSearch ingest folder and run ./scripts/run_ingest.sh
