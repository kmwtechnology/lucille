# Connectors: How to Configure

## VFSConnector

The ```VFSConnector``` leverages [Apache Commons VFS](https://commons.apache.org/proper/commons-vfs/index.html) provides
a single API for accessing various different file systems, including support for AWS S3 via the
[Amazon S3 driver for VFS](https://github.com/abashev/vfs-s3). The connector will accept
relative or absolute file paths (they are converted into ```<protocol://.*>``` format needed by VFS ) or one of
many [file systems](https://commons.apache.org/proper/commons-vfs/filesystems.html) that VFS supports.

Added features are the ability include/exclude regex file path patterns.

**Example Local File Path Config with no filters**

```yaml
connectors: [
  {
    name: "vfs-connector-1"
      pipeline: "pipeline1"
      class: "com.kmwllc.com.kmwllc.lucille.connector.VFSConnector"
      docIdPrefix: "file_"
      # accepts relative or absolute paths as well as supported protocols
      vfsPath: "./src/test/resources/VFSConnectorTest"
      docIdPrefix: "file_"
      includes: []
      excludes: []
  }              
]

```

**Example Absolute File Path Config**

```yaml
connectors: [
  {
    name: "vfs-connector-1"
      pipeline: "pipeline1"
      class: "com.kmwllc.com.kmwllc.lucille.connector.VFSConnector"
      docIdPrefix: "file_"
      # accepts relative or absolute paths as well as supported protocols
      vfsPath: "./src/test/resources/VFSConnectorTest"
      includes: [ ".*/localfs_filtered\\.conf$" ] # make it find a config file
      excludes: [ ".*/bogus/.*$" ] # make it ignore contents in bogus directory
  }  
]
```

**Example S3 Path Config**

```yaml
connectors: [
  {
    name: "s3-connector-1"
    pipeline: "pipeline1"
    class: "com.kmwllc.com.kmwllc.lucille.connector.VFSConnector"
    vfsPath: "s3://s3.us-east-1.amazonaws.com/fast-ai-nlp" # see S3 VFS docs about AWS creds support
    includes: [ ".*/wikitext-2\\.tgz$" ]
    excludes: [ ".*/logs/.*$" ]
  }
]
```

**Example Base64 Encoded Document produced by connector (docid is md5 hash of path)**

```json
{
  "id": "file_bb22c3105cb1a10f1e786641e91588df",
  "file_path": "file:///Users/aperson/testdir/localfs_filtered.conf",
  "file_modification_date": "2022-01-05T00:02:39.851Z",
  "file_creation_date": "2022-01-05T00:02:39.851Z",
  "file_size_bytes": 343,
  "file_content": "ewogIG5hbWU6ICJ2ZnMtY29ubmVjdG9yLTEiCiAgcGlwZWxpbmU6ICJwaXBlbGluZTEiCiAgY2xhc3M6ICJjb20ua213bGxjLmx1Y2lsbGUuY29ubmVjdG9yLlZGU0Nvbm5lY3RvciIKICBkb2NJZFByZWZpeDogImZpbGVfIgogICMgdGVzdCBhYmlsaXR5IHRvIHJlc29sdmUgcmVsYXRpdmUgbG9jYWwgZmlsZSBwYXRocwogIHZmc1BhdGg6ICIuL3NyYy90ZXN0L3Jlc291cmNlcy9WRlNDb25uZWN0b3JUZXN0IgogIGluY2x1ZGVzOiBbICIuKi9sb2NhbGZzX2ZpbHRlcmVkXFwuY29uZiQiIF0gIyBtYWtlIGl0IGZpbmQgdGhpcyBjb25maWcgZmlsZQogIGV4Y2x1ZGVzOiBbICIuKi9ib2d1cy8uKiQiIF0KfQ==",
  "run_id": "run"
}
```

## CSVConnector

...

## SolrConnector

...

## CSVConnector

...

