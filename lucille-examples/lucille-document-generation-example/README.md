# Document Generation For Testing

This example synthesizes documents with randomized fields (text, numbers, booleans, dates, nested JSON), then indexes
them into Elasticsearch.

## Requirements

- **Elasticsearch Server**: An instance of Elasticsearch set up locally if you want to actually index docs. For a dry run, disable sendEnabled.

## Setup Instructions

### 1. Choose Your Indexing Mode

- **Elasticsearch Mode**: Keep the Elasticsearch indexer as is. Ensure that Elasticsearch is running and reachable. 
- **Dry Run**: Set sendEnabled to false. Elasticsearch does not have to be running or reachable.
    
### 2. Configure Environment Variables

Set any optional overrides you want:
    
```bash
# Document count (defaults to 100).
export NUM_DOCS=1000

# Elasticsearch variables
export ES_URL="http://localhost:9200"
export ES_INDEX="test_docs"
export ES_SEND_ENABLED=true
```
    
### 3. Run the Ingestion Script
Navigate to the example folder and execute the ingestion script:
```bash
./scripts/run_ingest.sh
```

### - Example Document -
```bash
"id": "2",
"run_id": "4a274429-50c2-45f7-b2a7-a947bad8dd1a",
"text_example": "exitance south-easterly subopaque psychopannychism moustached Vernen symptomless inring Fremontodendron biotas Nodarse Rhody seldomcy beshaming submental dichotomistic techiest inorderly",
"integer_example": 75947,
"double_example": 342.04429969638477,
"boolean_example": true,
"date_example": "2021-01-30T18:34:43.983Z",
"nested_example": [
    {
      "field_c": "pseudomucin Corum",
      "field_a": "fivebar acceptance's",
      "field_b": "microfibril"
    },
    {
      "field_c": "Tallbott",
      "field_a": "Campanulaceae clumsinesses",
      "field_b": "uncluttering Hartleyan inexpressibilities northupite"
    },
    {
      "field_c": "Bayminette tylostylar nonblameless",
      "field_a": "deafer hyperalgia freewheel",
      "field_b": "boners simple-toned Chumpivilca unfearingness"
    },
    {
      "field_c": "spearmen OSlav mediatingly pinkfishes Euprepia",
      "field_a": "synochal Doenitz wretcheder beknotted beliefs",
      "field_b": "tempest-proof counterorder sumitro cuffy"
    },
    {
      "field_c": "escots jittering antihormone nonpedagogically Mitridae",
      "field_a": "Pettisville lemonwood grabbles",
      "field_b": "reprepared gravidly bloodguiltiness"
    },
    {
      "field_c": "autoalarm quadragenarian",
      "field_a": "cuspids",
      "field_b": "unframed Heb woodworms"
    },
    {
      "field_c": "nonhumorousness flippancy politicization deflationary silkwoman",
      "field_a": "kalon myelocytosis linoleate Apoidea",
      "field_b": "Milanese"
    },
    {
      "field_c": "Nisan Garnes",
      "field_a": "consorting Ogata double-header liles back-stepping",
      "field_b": "allotriophagy bootlaces"
    },
    {
      "field_c": "spues four-times-accented Palaemon Nettion vinod",
      "field_a": "harries unbreaded quasi-productively",
      "field_b": "unsensed sixty-eight tigerfishes Neodesha"
    },
    {
      "field_c": "housesitting two-wheeler coombes unfeminize nonobsessive",
      "field_a": "handbow",
      "field_b": "should-be conscription"
    },
    {
      "field_c": "adeemed grillwork",
      "field_a": "Scorpii Hliod macrogametocyte beads",
      "field_b": "brawlie lamming transplacental silaginoid schizopod"
    },
    {
      "field_c": "street-sweeping",
      "field_a": "chronaxies",
      "field_b": "deblateration catarrhine agapanthuses jungliest milliards"
    },
    {
      "field_c": "Hilliary",
      "field_a": "nonconvertible",
      "field_b": "rippliest cross-fissured menognath chirpier encopresis"
    },
    {
      "field_c": "clownishnesses ill-neighboring hoaxee foamier convertor",
      "field_a": "inital Krutch upflower",
      "field_b": "noninoculative colonialised"
    },
    {
      "field_c": "spelling supernatural Kapaau Alphaea gluttonised",
      "field_a": "arizonians joyfullest",
      "field_b": "tersely euphoriant obversion"
    },
    {
      "field_c": "agrypnode",
      "field_a": "waggably outstared trustfully",
      "field_b": "Severini"
    }
]
```