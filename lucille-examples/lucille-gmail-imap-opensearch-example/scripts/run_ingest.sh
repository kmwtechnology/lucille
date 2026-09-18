#!/bin/bash
# Run this script from the top level lucille-gmail-imap-opensearch-example directory via ./scripts/run_ingest.sh
# Make sure you have exported GMAIL_USER, GMAIL_APP_PASSWORD (and optionally OPENSEARCH_URL / OPENSEARCH_INDEX) first.
java -Dconfig.file=conf/gmail-opensearch.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
