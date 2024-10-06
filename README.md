# Sycamore Plugin for OpenSearch Ingestion Pipeline

## Introduction
[Sycamore](https://github.com/aryn-ai/sycamore) is a powerful data processing engine specifically designed to tackle many challenges of dealing with unstructured documents such as PDFs, doc/docx and powerpoints. 
For users of OpenSearch who wish to ingest such data into an OpenSearch cluster and ask non-trivial questions, they can gain a significant boost in the quality of search results by using Sycamore to not only extract text, but also detect images and tables.  
For vector search, proper segmentation of documents and accurate labeling of detected components within documents can lead to chunking strategies that suit the needs of different use cases.

We are releasing a plugin that can used in OpenSearch Ingestion Pipelines that exposes Sycamore functionality.  The initial release makes use of the [Aryn Partitioning Service](https://www.aryn.ai/post/announcing-the-aryn-partitioning-service), 
the default document partitioner used by Sycamore, to parse and extract text, images and tables from PDFs (and other document formats supported by the APS).

## How to use the plugin

### Installing and removing the plugin
You can follow the [official instructions](https://opensearch.org/docs/latest/install-and-configure/plugins/) from OpenSearch.

Download the plugin zip from the release page on GitHub and use the command for installing from zip files:
https://opensearch.org/docs/latest/install-and-configure/plugins/#install-a-plugin-from-a-zip-file

### Processor parameters
| Name | Description                                                                                                                             | Default   |
|------|-----------------------------------------------------------------------------------------------------------------------------------------|-----------|
| input_field | The field that contains base64 encoded document data                                                                                    | data      |
| output_field | The field that will contain all extracted text                                                                                          | extracted |
| aryn_api_key | Aryn API key                                                                                                                            | NA        |
| ignore_missing | true to ignore documents not having the input_field, false to throw an exception                                                        | false     |
| threshold | Threshold for bounding box detection.  More detail [here](https://sycamore.readthedocs.io/en/stable/aryn_cloud/specifying_options.html) | auto      |
| use_ocr | true to enable use of OCR                                                                                                               | false     |
| extract_images | true to enable image extraction                                                                                                         | false     |
| extract_table_structure | true to enable table detection and extraction                                                                                           | false     |

You can obtain a free Aryn API key from [here](https://www.aryn.ai/get-started)

### Example
`sycamore_pipeline.json`:
```
{"description": "example",
    "processors": [
        {
            "sycamore_ingest": {
                "input_field": "data",
                "output_field": "extracted",
                "aryn_api_key": "<your API key>",
                "ignore_missing": true,
                "threshold": "auto",
                "use_ocr": true,
                "extract_images": true,
                "extract_table_structure": true
            }
        }
    ]
}
```

### Create a pipeline
```
curl -X PUT localhost:9200/_ingest/pipeline/sycamore --data '@sycamore_pipeline.json' -H "Content-Type:application/json"
```

### Ingest a simple doc
```
file_data = $(base64 -w 0 <file>)
curl -X PUT -H "Content-type:application/json" --data '{"data","$file_data"}' localhost:9200/example-index/_doc/1?pipeline=sycamore
```

For large PDFs, use `ingest-large-doc.sh` in the `scripts` folder.

### Search
```
curl -X GET -H "Content-type:application/json" localhost:9200/example-index/_search
{
  "_source": {"excludes": ["partitioner_output", "data"]}, 
  "query": {
    "match": {
      "extracted": "example"
    }
  },
  "size": 5
}
```

### Partitioner outputs
In addition to putting all extracted text in the `output_field`, the plugin stores the partitioner output in the `partitioner_output` field.

The explanation of this output is documented here: https://sycamore.readthedocs.io/en/stable/aryn_cloud/aps_output.html

## Releases

| OpenSearch | Plugin     | Release date    |
|------------|------------|-----------------|
| 2.17.1     | 2.17.1.0   | October 6, 2024 | 
| 2.17.0     | 2.17.0.0   | October 5, 2024 |
