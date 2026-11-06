"""
Layout-aware extraction with Docling, run by Lucille's ExternalPython stage.

Converts the file at the document's path and adds the Docling output, extracted tables and optionally
HybridChunker chunks. Options, all environment variables:

  DOCLING_FILE_PATH_FIELD   field holding the path or file:// URI (default file_path)
  DOCLING_OUTPUT_FIELD      destination for the converted document (default docling_markdown)
  DOCLING_OUTPUT_FORMAT     markdown, text or json (default markdown)
  DOCLING_EXTRACT_TABLES    true/false (default true)
  DOCLING_TABLES_FIELD      destination for tables as pages/caption/columns/rows (default docling_tables)
  DOCLING_CHUNK             true/false (default false)
  DOCLING_CHUNKS_FIELD      destination for chunks as text/headings/pages (default docling_chunks)
  DOCLING_CHUNK_TOKENIZER   HuggingFace tokenizer to size chunks with (default sentence-transformers/all-MiniLM-L6-v2)
  DOCLING_CHUNK_MAX_TOKENS  chunk token limit (default the tokenizer model's own limit)

Each enabled list field also gets a <field>_count field.
"""
import os
import re
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

from docling.document_converter import DocumentConverter

# ExternalPython gives the script no stage config, so options come from the environment
FILE_PATH_FIELD = os.environ.get("DOCLING_FILE_PATH_FIELD", "file_path")
OUTPUT_FIELD = os.environ.get("DOCLING_OUTPUT_FIELD", "docling_markdown")
OUTPUT_FORMAT = os.environ.get("DOCLING_OUTPUT_FORMAT", "markdown")
TABLES_FIELD = os.environ.get("DOCLING_TABLES_FIELD", "docling_tables")
EXTRACT_TABLES = os.environ.get("DOCLING_EXTRACT_TABLES", "true").lower() == "true"
CHUNK = os.environ.get("DOCLING_CHUNK", "false").lower() == "true"
CHUNKS_FIELD = os.environ.get("DOCLING_CHUNKS_FIELD", "docling_chunks")
# should match the downstream embedding model, which truncates anything past its own limit
CHUNK_TOKENIZER = os.environ.get("DOCLING_CHUNK_TOKENIZER", "sentence-transformers/all-MiniLM-L6-v2")
CHUNK_MAX_TOKENS = os.environ.get("DOCLING_CHUNK_MAX_TOKENS")

if OUTPUT_FORMAT not in ("markdown", "text", "json"):
    raise ValueError(f"DOCLING_OUTPUT_FORMAT must be markdown, text or json, got '{OUTPUT_FORMAT}'")

# loads layout/table models once; building one per document would dominate runtime
converter = DocumentConverter()

# only built when enabled: it downloads a HuggingFace tokenizer on first use
chunker = None
if CHUNK:
    from docling.chunking import HybridChunker
    from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer
    # max_tokens=None falls back to the model's own max_seq_length
    tokenizer = HuggingFaceTokenizer.from_pretrained(
        model_name=CHUNK_TOKENIZER,
        max_tokens=int(CHUNK_MAX_TOKENS) if CHUNK_MAX_TOKENS else None,
    )
    chunker = HybridChunker(tokenizer=tokenizer)

# "Crude oil ........ 71.85" style leaders; the chunker's triplets repeat the row label per cell, so on
# statistical tables these roughly tripled the tokens per chunk
DOT_LEADER = re.compile(r"\s*(?:\.\s?){3,}\s*")


def strip_dot_leaders(dl_doc):
    for table in dl_doc.tables:
        for cell in table.data.table_cells:
            cell.text = DOT_LEADER.sub(" ", cell.text).strip()


def export(dl_doc):
    if OUTPUT_FORMAT == "text":
        return dl_doc.export_to_text()
    if OUTPUT_FORMAT == "json":
        return dl_doc.export_to_dict()
    return dl_doc.export_to_markdown()


def extract_tables(dl_doc):
    tables = []
    for table in dl_doc.tables:
        df = table.export_to_dataframe(doc=dl_doc)
        tables.append({
            "pages": sorted({prov.page_no for prov in table.prov}),
            "caption": table.caption_text(dl_doc),
            "columns": [str(c) for c in df.columns],
            # astype(str) so numpy/NaN cells survive json.dumps in Py4jClient
            "rows": df.astype(str).to_dict(orient="records"),
        })
    return tables


# kept as a nested field on the parent: ExternalPython ignores ___children, so chunks can't be emitted as child docs
def chunk(dl_doc):
    chunks = []
    for i, c in enumerate(chunker.chunk(dl_doc=dl_doc)):
        chunks.append({
            "index": i,
            "text": c.text,
            # headings prepended; this is the text Docling recommends embedding
            "contextualized_text": chunker.contextualize(chunk=c),
            "headings": c.meta.headings or [],
            "pages": sorted({prov.page_no for item in c.meta.doc_items for prov in item.prov}),
        })
    return chunks


# ExternalPython drops any field missing from the returned dict, so mutate and return the whole doc
def process_document(doc):
    path = doc.get(FILE_PATH_FIELD)
    if not path:
        return doc

    # FileConnector emits file:// URIs, which Docling rejects; it only takes local paths or http(s) URLs
    parsed = urlparse(path)
    if parsed.scheme == "file":
        path = url2pathname(unquote(parsed.path))

    # raises on conversion failure so Lucille marks the document failed rather than indexing it empty
    dl_doc = converter.convert(path).document
    strip_dot_leaders(dl_doc)

    doc[OUTPUT_FIELD] = export(dl_doc)
    if EXTRACT_TABLES:
        tables = extract_tables(dl_doc)
        doc[TABLES_FIELD] = tables
        doc[f"{TABLES_FIELD}_count"] = len(tables)
    if CHUNK:
        chunks = chunk(dl_doc)
        doc[CHUNKS_FIELD] = chunks
        doc[f"{CHUNKS_FIELD}_count"] = len(chunks)
    return doc
