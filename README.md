# Aryn Plugin for OpenSearch Ingestion Pipeline

## Introduction
Aryn DocParse is a compound AI system for parsing, chunking, enriching, and storing unstructured documents at scale. It uses a set of purpose-built AI models for document segmentation, optical character recognition (OCR), and extracting tables, images, metadata, and more.

For users of OpenSearch who wish to ingest unstructured data into an OpenSearch cluster and ask non-trivial questions, they can gain a significant boost in the quality of search results by using Sycamore to not only extract text, but also detect images and tables.  
For vector search, proper segmentation of documents and accurate labeling of detected components within documents can lead to chunking strategies that suit the needs of different use cases.

We are releasing a plugin that can used in OpenSearch Ingestion Pipelines that exposes DocParse functionality.  The initial release makes use of [Aryn DocParse](https://docs.aryn.ai/docparse/introduction) to extract text as well as properties/metadata from documents.

## How to use the plugin

### Installing and removing the plugin
You can follow the [official instructions](https://opensearch.org/docs/latest/install-and-configure/plugins/) from OpenSearch.

Download the plugin zip from the release page on GitHub and use the command for installing from zip files:
https://opensearch.org/docs/latest/install-and-configure/plugins/#install-a-plugin-from-a-zip-file

### Processor parameters
| Name             | Description                                                                                                                             | Default   |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------|-----------|
| input_field      | The field that contains base64 encoded document data                                                                                    | data      |
| output_field     | The field that will contain all extracted text                                                                                          | extracted |
| aryn_api_key     | Aryn API key                                                                                                                            | NA        |
| ignore_missing   | true to ignore documents not having the input_field, false to throw an exception                                                        | false     |
| threshold        | Threshold for bounding box detection.  More detail [here](https://sycamore.readthedocs.io/en/stable/aryn_cloud/specifying_options.html) | auto      |
| summarize_images | true to enable use of a vision model to generate summarizes of images                                                                   | false     |
| extract_images   | true to enable image extraction                                                                                                         | false     |
| text_mode        | One of "auto", "ocr_standard" or "ocr_vision"                                                                                           | auto      |
| table_mode       | One of "none", "standard" or "vision"                                                                                                   | none      |
| schema           | Schema string to use for property extraction.                                                                                           | none      |

You can obtain a free Aryn API key from [here](https://www.aryn.ai/get-started)

### Example
`aryn_pipeline.json`:
```
{"description": "example",
    "processors": [
        {
            "aryn_ingest": {
                "input_field": "data",
                "output_field": "extracted",
                "aryn_api_key": "<your Aryn API key>",
                "ignore_missing": true,
                "threshold": "auto",
                "extract_images": true,
                "text_mode": "auto",
                "table_mode": "standard",
                "schema": "{\"properties\": [{\"name\": \"property_name\", \"type\": {\"type\": \"string\"}}]}"
            }
        }
    ]
}
```

### Create a pipeline
```
curl -X PUT localhost:9200/_ingest/pipeline/aryn_docparse --data '@aryn_pipeline.json' -H "Content-Type:application/json"
```

### Ingest a simple doc
```
file_data = $(base64 -w 0 <file>)
curl -X PUT -H "Content-type:application/json" --data '{"data","$file_data"}' localhost:9200/example-index/_doc/1?pipeline=aryn_docparse
```

For large PDFs, use `ingest-large-doc.sh` in the `scripts` folder.

### Search
```
curl -X GET -H "Content-type:application/json" localhost:9200/example-index/_search
{
  "_source": {"excludes": ["data"]}, 
  "query": {
    "match": {
      "extracted": "example"
    }
  },
  "size": 5
}
```

### Property extraction
You can specify a schema for property extraction using the `schema` parameter.  The schema should be a JSON string that follows the format described in the [Aryn DocParse documentation](https://docs.aryn.ai/docparse/processing_options#property-extraction).


## Releases

| OpenSearch | Plugin   | Release date      |
|------------|----------|-------------------|
| 2.19.2     | 2.19.2.0 | December 19, 2025 | 
| 3.4.0      | 3.4.0.0  | December 19, 2025 |
