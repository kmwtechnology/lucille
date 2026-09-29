/** Starter JSON shown in the create-configuration editor. */
export const CONFIG_TEMPLATE = `{
  "connectors": [
    {
      "name": "connector1",
      "class": "com.kmwllc.lucille.connector.CSVConnector",
      "path": "conf/dummy.csv",
      "pipeline": "pipeline1"
    }
  ],
  "pipelines": [
    {
      "name": "pipeline1",
      "stages": []
    }
  ],
  "indexer": {
    "type": "CSV"
  }
}`
