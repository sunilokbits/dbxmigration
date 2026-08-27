# Document Q&A RAG Setup — DBX Migration Studio

## Overview

This document describes the **Document Q&A** feature integrated into DBX Migration Studio's Genie AI assistant. It uses Databricks Vector Search with RAG (Retrieval Augmented Generation) to provide intelligent answers about the app's architecture, modules, configuration, and usage.

## Architecture

```
User Question (Genie Chat Panel)
        │
        ▼
┌─────────────────────────────────────┐
│  1. FAQ Check (instant match)     │
│  2. Vector Search RAG retrieval   │
│  3. Context enrichment            │
│  4. Forward to Genie API          │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  Vector Search Index              │
│  (doc_qa_chunks_index)            │
│  Model: databricks-gte-large-en   │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  Delta Table                      │
│  admin_source.migration_app       │
│  .doc_qa_chunks                   │
│  (26+ documentation chunks)       │
└─────────────────────────────────────┘
```

## Components

| Component | Name | Purpose |
|---|---|---|
| Delta Table | `admin_source.migration_app.doc_qa_chunks` | Stores documentation chunks with CDF enabled |
| Vector Search Endpoint | `dbx_migration_vs_endpoint` | Compute for similarity search |
| Vector Search Index | `admin_source.migration_app.doc_qa_chunks_index` | Delta Sync index with embeddings |
| Embedding Model | `databricks-gte-large-en` | Generates vector embeddings |
| Route Integration | `migration_utility/routes/genie.py` | `_retrieve_rag_context()` function |

## Setup (Automated)

Run the setup notebook to create everything:

```
src/notebooks/06_Setup_DocQA_RAG
```

This notebook:
1. Creates the Delta table with proper schema and CDF
2. Populates 26+ documentation chunks covering all modules
3. Creates the Vector Search endpoint (if not exists)
4. Creates the Vector Search index with managed embeddings
5. Waits for readiness and validates with test queries

## How It Works (Request Flow)

1. User types a question in the Genie chat panel
2. `genie.py` checks the FAQ knowledge base for instant matches
3. If no FAQ match, `_retrieve_rag_context(question)` is called:
   - Sends the question to the Vector Search index
   - Retrieves top 3 most similar documentation chunks
   - Formats them as context with section headers
4. The enriched prompt (APP_CONTEXT_PREAMBLE + RAG context + question) is sent to the Genie API
5. Genie responds with documentation-informed answer

## Integration Points in genie.py

```python
# Function: _retrieve_rag_context(question)
# - Calls Vector Search REST API
# - Returns formatted doc chunks or empty string on failure
# - Graceful degradation: if VS unavailable, chat still works

# Integrated in 3 routes:
# 1. start_conversation() — new conversations
# 2. send_message() (faq→fresh) — after FAQ, new real conversation
# 3. send_message() (follow-up) — normal follow-up messages
```

## Table Schema

| Column | Type | Description |
|---|---|---|
| chunk_id | INT | Sequential identifier (primary key for VS) |
| doc_title | STRING | Document title |
| section | STRING | Section heading (filterable) |
| content | STRING | Full chunk text (embedded by VS) |
| char_count | INT | Character count |
| source_file | STRING | Source file reference |
| created_at | STRING | Creation timestamp |

## Adding New Documentation

To add more content to the RAG knowledge base:

1. Insert new rows into `admin_source.migration_app.doc_qa_chunks`
2. Trigger a sync on the Vector Search index:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.vector_search_indexes.sync_index("admin_source.migration_app.doc_qa_chunks_index")
```

## Configuration (genie.py)

```python
_VS_INDEX_NAME = "admin_source.migration_app.doc_qa_chunks_index"
_VS_NUM_RESULTS = 3  # Number of chunks to retrieve
```

Adjust `_VS_NUM_RESULTS` to control how much context is injected:
- 2-3: Good balance of relevance and token usage
- 5+: More comprehensive but may hit token limits

## Troubleshooting

| Issue | Resolution |
|---|---|
| RAG returns empty | Check VS endpoint is ONLINE: `w.vector_search_endpoints.get_endpoint(...)` |
| Index not ready | Wait 5-10 min after creation, or trigger sync |
| Poor relevance | Add more specific chunks, reduce chunk size |
| Timeout on retrieval | Increase timeout in `_retrieve_rag_context()` (default 10s) |
| Token limit exceeded | Reduce `_VS_NUM_RESULTS` or chunk sizes |

## Deployment Checklist

- [ ] Run `06_Setup_DocQA_RAG` notebook
- [ ] Verify Vector Search endpoint is ONLINE
- [ ] Verify index is ready with test queries
- [ ] Ensure `genie.py` has `_retrieve_rag_context()` function
- [ ] Redeploy the Databricks App
- [ ] Test in Genie chat: ask "What is Migration Studio?"

---

*Last updated: July 2026*
