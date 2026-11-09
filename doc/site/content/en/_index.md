---
title: Lucille
---

{{< blocks/cover title="Welcome to Lucille" image_anchor="top" height="full" >}}

<p class="lead mt-1"> The best practices in Search ETL, codified into a single solution.</p>
<p class="lead mt-7"> Lucille is an open source Search ETL solution created by people who work in search every day and who know the traditional pain points associated with getting your data processed, enriched and indexed into a search engine. </p>
<p class="lead mt-7"> Lucille is designed to reduce or eliminate these pain points.</p>

<div class="text-center mt-5">
  <a class="btn btn-lg btn-primary me-3 mb-4" href="{{< relref "docs/" >}}">
    Learn More <i class="fas fa-arrow-alt-circle-right ms-2"></i>
  </a>
  <a class="btn btn-lg btn-secondary me-3 mb-4" href="https://github.com/kmwtechnology/lucille">
    Github <i class="fab fa-github ms-2"></i>
  </a>
</div>

{{< /blocks/cover >}}

{{% blocks/lead color="primary" %}}
**Search ETL** is the work of turning data from databases, file shares, cloud storage and message streams into documents a search engine or vector database can index well.
It looks like ordinary ETL, but the destination is not a table, the transformation is not a type conversion, and the unit of work is not a row.

<a class="btn btn-lg btn-secondary text-white mt-3" href="{{< relref "docs/getting-started/what-is-search-etl" >}}">What is search ETL? <i class="fas fa-arrow-alt-circle-right ms-2"></i></a>
{{% /blocks/lead %}}

{{% blocks/section color="dark" type="row" %}}

{{% blocks/feature icon="fa-file-lines" title="Documents, not rows" %}}
One source document fans out into many chunks and child records, each with its own identity. Lucille tracks every child, keeps writes and deletes for the same ID in order, and deletes by ID or by query.
{{% /blocks/feature %}}

{{% blocks/feature icon="fa-gears" title="Enrichment is the work" %}}
Text extraction, OCR, entity recognition and embeddings are expensive and fragile. They belong in a pipeline you can scale, not in the search cluster.
{{% /blocks/feature %}}

{{% blocks/feature icon="fa-list-check" title="Every document accounted for" %}}
Bulk APIs fail per item and concurrent runs have no obvious end. Lucille tracks every document and child to a terminal state, so a run ends with exact counts.
{{% /blocks/feature %}}

{{% /blocks/section %}}




