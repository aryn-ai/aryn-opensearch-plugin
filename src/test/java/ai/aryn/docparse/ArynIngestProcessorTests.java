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
package ai.aryn.docparse;

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

public class ArynIngestProcessorTests extends OpenSearchTestCase {
    public void testCallAPS() throws Exception {
    }

    public void testGetOptionFile() throws Exception {
        String threshold = "0.01";
        ArynIngestProcessor processor = new ArynIngestProcessor("tag", "desc", "input",
                "output", "apiKey", false, threshold, false,
                false, "auto", "standard", null, null);
        byte[] actual = processor.buildOptionJson(threshold, "auto", "standard", false, false, null);
        String fileContent = new String(actual);
        Gson gson = new GsonBuilder().create();
        JsonObject json = gson.fromJson(fileContent, JsonElement.class).getAsJsonObject();
        assertThat(json.get("threshold").getAsDouble(), is(Double.parseDouble(threshold)));

        threshold = "auto";
        processor = new ArynIngestProcessor("tag", "desc", "input",
                "output", "apiKey", false, threshold, false,
                false, "auto", "standard", null, null);
        actual = processor.buildOptionJson(threshold, "auto", "standard", false, false, null);
        fileContent = new String(actual);
        gson = new GsonBuilder().create();
        json = gson.fromJson(fileContent, JsonElement.class).getAsJsonObject();
        assertThat(json.get("threshold").getAsString(), is(threshold));
    }

    @Ignore
    public void testExecute() throws Exception {
        String threshold = "0.01";
        String key = System.getenv("ARYN_TOKEN");
        ArynIngestProcessor processor = new ArynIngestProcessor("tag", "desc", "input",
                "output", key, false, threshold, false,
                false, "auto", "standard", null, null);

        IngestDocument doc = new IngestDocument("test-index", null, null, null, null, Map.of("input", "foo"));
        IngestDocument output = processor.execute(doc);
    }

}
