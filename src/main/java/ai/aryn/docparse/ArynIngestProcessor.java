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

import ai.aryn.docparse.api.PartitionApi;
import ai.aryn.docparse.auth.HttpBearerAuth;
import ai.aryn.docparse.model.Element;
import ai.aryn.docparse.model.PartitionerResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.log4j.Log4j2;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Log4j2
public class ArynIngestProcessor extends AbstractProcessor {

    public static final String TYPE = "aryn_ingest";
    private static final Gson gson = new GsonBuilder().create();

    public static final String USER_AGENT = "ArynOpenSearchPlugin_v0.1.0";
    public static final String ARYN_CALL_ID = "x-aryn-call-id";
    public static final String ARYN_API_VERSION = "x-aryn-api-version";
    private final String inputField;
    private final String outputField;
    private final boolean ignoreMissing;
    private final String threshold;
    private final boolean extractImages;
    private final boolean summarizeImages;
    private final String textMode;
    private final String tableMode;
    private final String schema;
    private final String schemaPath;

    final HttpBearerAuth HTTPBearer;
    final PartitionApi api;

    final ApiClient defaultClient = Configuration.getDefaultApiClient();

    protected ArynIngestProcessor(String tag, String description,
                                  String inputField, String outputField,
                                  String apiKey, boolean ignoreMissing, String threshold,
                                  boolean extractImages, boolean summarizeImages, String textMode, String tableMode,
                                  String schema, String schemaPath) {
        super(tag, description);
        this.inputField = inputField;
        this.outputField = outputField;
        this.ignoreMissing = ignoreMissing;
        this.threshold = threshold;
        this.extractImages = extractImages;
        this.summarizeImages = summarizeImages;
        this.textMode = textMode;
        this.tableMode = tableMode;
        this.schema = schema;
        this.schemaPath = schemaPath;

        defaultClient.setBasePath("https://api.aryn.ai");
        // Configure HTTP bearer authorization: HTTPBearer
        HTTPBearer = (HttpBearerAuth) defaultClient.getAuthentication("HTTPBearer");
        HTTPBearer.setBearerToken(apiKey);
        api = new PartitionApi(defaultClient);
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        super.execute(ingestDocument, handler);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        byte[] input = ingestDocument.getFieldValueAsBytes(inputField, ignoreMissing);

        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + inputField + "] is null, cannot parse.");
        }

        try {
            Path inputFile = Files.createTempFile("aryn-ingest", ".tmp");
            Files.write(inputFile, input);
            byte[] options = buildOptionJson(
                    this.threshold,
                    this.textMode,
                    this.tableMode,
                    this.extractImages,
                    this.summarizeImages,
                    this.schema,
                    this.schemaPath);
            PartitionerResponse res = partition(inputFile, options);
            if (res == null) {
                return ingestDocument;
            }
            List<Element> elements = res.getElements();
            if (!elements.isEmpty()) {
                String text = joinAllTextRepresentations(elements);
                ingestDocument.setFieldValue(this.outputField, text);
            }
            Object properties = res.getProperties();
            if (properties != null) {
                Map<String, ?> propsMap = gson.fromJson(gson.toJson(properties), Map.class);
                for (Map.Entry<String, ?> entry : propsMap.entrySet()) {
                    log.debug("Property: {} = {}", entry.getKey(), entry.getValue());
                    ingestDocument.appendFieldValue(entry.getKey(), entry.getValue());
                }
            }
        } catch (IOException e) {
            log.error("Unable to process document: {}", e, e);
            throw new RuntimeException(e);
        }

        return ingestDocument;
    }

    private PartitionerResponse partition(Path input, byte[] options) throws Exception {
        Object res = AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> {

            try {
                PartitionerResponse response = api.partition(input, null, options, USER_AGENT, Collections.emptyMap());
                Map<String, List<String>> responseHeaders = this.defaultClient.getResponseHeaders();
                String arynCallId = responseHeaders.get(ARYN_CALL_ID).get(0);
                String arynVersion = responseHeaders.get(ARYN_API_VERSION).get(0);
                log.info("aryn_call_id: {}, aryn_version: {}", arynCallId, arynVersion);
                return response;
            } catch (ApiException e) {
                // TODO add retries
                log.error("Call to Aryn Partitioner failed: {}", e, e);
                throw new RuntimeException(e);
            }
        });

        return res == null ? null : (PartitionerResponse) res;
    }

    @VisibleForTesting
    byte[] buildOptionJson(
            String threshold,
            String textMode,
            String tableMode,
            boolean extractImages,
            boolean summarizeImages,
            String schemaStr,
            String schemaFilePath) {
        Map<String, Object> optionsMap = new HashMap<>();
        if (threshold.equals("auto")) {
            optionsMap.put("threshold", "auto");
        } else {
            optionsMap.put("threshold", Double.valueOf(threshold));
        }
        if (textMode != null) {
            optionsMap.put("text_mode", textMode);
        }
        if (tableMode != null) {
            optionsMap.put("table_mode", tableMode);
        }
        optionsMap.put("extract_images", extractImages);
        optionsMap.put("summarize_images", summarizeImages);
        ObjectMapper objectMapper = new ObjectMapper();
        String schema = schemaStr;
        try {
            if (schema == null && schemaFilePath != null) {
                schema = getSchemaAsString(schemaFilePath);
            }
            if (schema != null) {
                optionsMap.put("property_extraction_options", Map.of("schema", schema));
            }
            String optionsJson = objectMapper.writeValueAsString(optionsMap);
            return optionsJson.getBytes(StandardCharsets.UTF_8); // tempFile.toFile();
        } catch (IOException ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    String getSchemaAsString(String filePath) throws IOException {
        try (Reader reader = new FileReader(filePath)) {
            // Use JsonParser.parseReader to get a JsonElement
            // Then cast it to a JsonObject
            JsonObject jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
            return jsonObject.toString();
        } catch (Exception e) {
            log.error(e, e);
        }
        return null;
    }

    private String joinAllTextRepresentations(List<Element> elements) {
        StringBuilder builder = new StringBuilder();
        for (Element element : elements) {
            String text = element.getTextRepresentation();
            if (text != null && !text.isEmpty()) {
                builder.append(String.format(Locale.ROOT, "%s%n", text));
            }
        }
        return builder.toString();
    }

    @Override
    public void batchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        super.batchExecute(ingestDocumentWrappers, handler);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
