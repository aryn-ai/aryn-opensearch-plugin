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
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ai.aryn.docparse.ArynIngestProcessorFactory.readStringOrDoubleProperty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ArynIngestProcessorFactoryTests extends OpenSearchTestCase {

    public void testReadStringOrDoubleProperty() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("threshold", 0.01);
        String actual = readStringOrDoubleProperty("type", "", config, "threshold", "dummy");
        assertEquals(actual, "0.01");
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        Double threshold = 0.01;
        config.put("aryn_api_key", "key");
        config.put("threshold", threshold);
        ArynIngestProcessorFactory factory = new ArynIngestProcessorFactory();
        ArynIngestProcessor processor = (ArynIngestProcessor) factory.create(Collections.emptyMap(), "tag", "desc", config);
        byte[] actual = processor.buildOptionJson("0.01", "auto", "standard", false, false, null);
        String fileContent = new String(actual);
        Gson gson = new GsonBuilder().create();
        JsonObject json = gson.fromJson(fileContent, JsonElement.class).getAsJsonObject();
        assertThat(json.get("threshold").getAsDouble(), is(threshold));
    }
}
