# Sycamore Plugin for OpenSearch Ingestion Pipeline

## Introduction
[Sycamore](https://github.com/aryn-ai/sycamore) is a powerful data processing engine specifically designed to tackle many challenges of dealing with unstructured documents such as PDFs, doc/docx and powerpoints. 
For users of OpenSearch who wish to ingest such data into an OpenSearch cluster and ask non-trivial questions, they can gain a significant boost in the quality of search results by using Sycamore to not only extract text, but also detect images and tables.  
For vector search, proper segmentation of documents and accurate labeling of detected components within documents can lead to chunking strategies that suit the needs of different use cases.

We are releasing a plugin that can used in OpenSearch Ingestion Pipelines that exposes Sycamore functionality.  The initial release makes use of the [Aryn Partitioning Service](https://www.aryn.ai/post/announcing-the-aryn-partitioning-service), 
the default document partitioner used by Sycamore, to parse and extract text, images and tables from PDFs (and other document formats supported by the APS).

## How to use the plugin

## Releases

| OpenSearch  | Plugin    | Release date    |
|-------------|-----------|-----------------|
| 2.17.0      | 2.17.0.0  | October 5, 2024 | 