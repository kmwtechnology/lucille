# Enron Indexer to OpenSearch

This example makes emails searchable so that, for example, an investigator can attempt to uncover illegal activities that may have occurred within a company such as Enron.

This example configures Lucille to index the Enron email data into OpenSearch, where the emails could then be searched and visualized.

### Requirements:

- The Enron dataset, which can be found in this link: https://www.cs.cmu.edu/~enron/
- An instance of OpenSearch server set up locally using Docker. Here is a link to get you started: https://opensearch.org/docs/latest/getting-started/
- An index set up on OpenSearch

### Setting up:

1. Head to the OpenSearch dashboard dev tools page. This would allow us to easily create and modify any indices that we will create. Sign in using the username and password set in the docker.yaml file when downloading OpenSearch. Or use the link to the dev tools page, replacing &lt;port&gt; with your port that is running the OpenSearch dashboard
    
    [http://localhost](http://localhost/):&lt;port&gt;/app/dev_tools#/
    
2. copy the json under mappings folder, place that into the devtools and run it. You should get a response back on the right panel. This would create the enron index with its respective mappings and configurations. The command should look like this: \
PUT enron \
{json}
3. Navigate to the opensearch ingest folder and run ./scripts/run_ingest.sh
