/*
 * Copyright 2024 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.aryn.sycamore.ingest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Ignore;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class SycamoreIngestProcessorTests extends OpenSearchTestCase {
    public void testCallAPS() throws Exception {
    }

    public void testGetOptionFile() throws Exception {
        String threshold = "0.01";

        DocParseOptions options = DocParseOptions.builder()
                .input_field("input")
                .output_field("output")
                .ignore_missing(false)
                .aryn_api_key("apiKey")
                .threshold(threshold)
                .use_ocr(false)
                .ocr_language("english")
                .extract_images(false)
                .extract_table_structure(false)
                .summarize_images(false)
                .chunking_options(Map.of())
                .build();
        SycamoreIngestProcessor processor = new SycamoreIngestProcessor(
                "tag", "desc", options);
        File actual = processor.getOptionFile();
        String fileContent = Files.readString(actual.toPath());
        Gson gson = new GsonBuilder().create();
        JsonObject json = gson.fromJson(fileContent, JsonElement.class).getAsJsonObject();
        assertThat(json.get("threshold").getAsDouble(), is(Double.parseDouble(threshold)));

        threshold = "auto";
        DocParseOptions options2 = DocParseOptions.builder()
                .input_field("input")
                .output_field("output")
                .ignore_missing(false)
                .aryn_api_key("apiKey")
                .threshold(threshold)
                .use_ocr(false)
                .ocr_language("english")
                .extract_images(false)
                .extract_table_structure(false)
                .summarize_images(false)
                .chunking_options(Map.of())
                .build();
        processor = new SycamoreIngestProcessor(
                "tag", "desc", options2);
        actual = processor.getOptionFile();
        fileContent = Files.readString(actual.toPath());
        gson = new GsonBuilder().create();
        json = gson.fromJson(fileContent, JsonElement.class).getAsJsonObject();
        assertThat(json.get("threshold").getAsString(), is(threshold));
    }

    @Ignore
    public void testExecute() throws Exception {
        String threshold = "0.01";
        DocParseOptions options = DocParseOptions.builder()
                .input_field("input")
                .output_field("output")
                .ignore_missing(false)
                .aryn_api_key("apiKey")
                .threshold(threshold)
                .use_ocr(false)
                .ocr_language("english")
                .extract_images(false)
                .extract_table_structure(false)
                .summarize_images(false)
                .chunking_options(Map.of())
                .build();
        String key = System.getenv("ARYN_API_KEY");
        SycamoreIngestProcessor processor = new SycamoreIngestProcessor(
                "tag", "desc", options);

        IngestDocument doc = new IngestDocument("test-index", null, null, null, null, Map.of("input", "foo"));
        IngestDocument output = processor.execute(doc);
    }

}
