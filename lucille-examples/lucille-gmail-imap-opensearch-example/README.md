# Gmail (IMAP) to OpenSearch

This example uses Lucille's `IMAPConnector` to crawl a Gmail mailbox over IMAP and index each email message
into an OpenSearch index named **`emailindex`** running on `localhost:9200`.

Each email becomes a Lucille `Document`. The connector populates fields such as `subject`, `from`, `to`, `cc`,
`reply_to`, `sent_date`, `received_date`, `folder`, `size`, the plain text body (`text`) and the HTML body (`html`),
along with every raw email header. The pipeline in this example renames `text` to `body` and drops `html` before
indexing.

## Requirements

- An OpenSearch instance reachable at `http://localhost:9200`. The quickest way is Docker — see the
  [OpenSearch getting started guide](https://opensearch.org/docs/latest/getting-started/).
- A Gmail account with **IMAP enabled** (Gmail settings → "Forwarding and POP/IMAP" → Enable IMAP).
- A Google **App Password**. Gmail does not allow your normal password over IMAP when 2-Step Verification is on;
  create one at https://myaccount.google.com/apppasswords and use it as `GMAIL_APP_PASSWORD`.

## Setting up

1. Create the index with a mapping. In the OpenSearch Dashboards Dev Tools console (or with curl), create the
   `emailindex` index using the mapping in [`mapping/opensearch_mappings.json`](mapping/opensearch_mappings.json):

   ```bash
   curl -X PUT "http://localhost:9200/emailindex" \
     -H 'Content-Type: application/json' \
     --data-binary @mapping/opensearch_mappings.json
   ```

2. Export your environment variables:

   ```bash
   export GMAIL_USER="you@gmail.com"
   export GMAIL_APP_PASSWORD="xxxx xxxx xxxx xxxx"
   # Optional overrides (defaults shown):
   # export OPENSEARCH_URL="http://localhost:9200"
   # export OPENSEARCH_INDEX="emailindex"
   # export GMAIL_FOLDER="INBOX"
   ```

   If your OpenSearch requires authentication, embed the credentials in the URL, e.g.
   `export OPENSEARCH_URL="https://admin:admin@localhost:9200"`.

3. Build the example (from the repository root). This compiles Lucille and copies all runtime dependencies —
   including the `lucille-imap` plugin and its `jakarta.mail` libraries — into `target/lib`:

   ```bash
   mvn clean package -pl lucille-examples/lucille-gmail-imap-opensearch-example -am
   ```

4. Run the ingest from this example's directory:

   ```bash
   cd lucille-examples/lucille-gmail-imap-opensearch-example
   ./scripts/run_ingest.sh
   ```

   On Windows (PowerShell):

   ```powershell
   java -Dconfig.file=conf/gmail-opensearch.conf -cp "target/lib/*" com.kmwllc.lucille.core.Runner
   ```

## Configuration

The full configuration lives in [`conf/gmail-opensearch.conf`](conf/gmail-opensearch.conf):

- **connector** — `IMAPConnector` pointed at `imap.gmail.com:993` over SSL, reading the `INBOX` folder. Set
  `recurse: true` to also crawl sub-folders / labels.
- **pipeline** — renames `text` → `body` and deletes `html`.
- **indexer** — `OpenSearch`, batching 100 docs at a time.
- **opensearch** — `url` and `index` (defaults to `emailindex`).

By default each document's ID is derived from the email's `Message-ID` header, so re-running the ingest updates
existing documents rather than creating duplicates.

## Notes

- The mailbox is always opened **read-only**; this example never modifies or deletes your email.
- Crawling a large mailbox can take a while and will fetch message bodies; start with a small folder to try it out.
