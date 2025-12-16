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
import ai.aryn.docparse.model.BodySyncPartitionDocumentV1Options;
import ai.aryn.docparse.model.Element;
import ai.aryn.docparse.model.PartitionerResponse;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;

import java.io.File;
import java.io.IOException;
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

    public static final String USER_AGENT = "ArynOpenSearchPlugin_v0.1.0";
    public static final String ARYN_CALL_ID = "x-aryn-call-id";
    public static final String ARYN_API_VERSION = "x-aryn-api-version";
    private final String inputField;
    private final String outputField;
    private final boolean ignoreMissing;
    private final String threshold;
    private final boolean useOcr;
    private final boolean extractImages;
    private final boolean extractTableStructure;
    final HttpBearerAuth HTTPBearer;
    final PartitionApi api;

    final ApiClient defaultClient = Configuration.getDefaultApiClient();

    protected ArynIngestProcessor(String tag, String description,
                                  String inputField, String outputField,
                                  String apiKey, boolean ignoreMissing, String threshold,
                                  boolean useOcr, boolean extractImages, boolean extractTableStructure) {
        super(tag, description);
        this.inputField = inputField;
        this.outputField = outputField;
        this.ignoreMissing = ignoreMissing;
        this.threshold = threshold;
        this.useOcr = useOcr;
        this.extractImages = extractImages;
        this.extractTableStructure = extractTableStructure;

        defaultClient.setBasePath("https://api.aryn.ai");
        //String apiKey = System.getenv("ARYN_API_KEY");
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
        Map<String, Object> additionalFields = new HashMap<>();

        byte[] input = ingestDocument.getFieldValueAsBytes(inputField, ignoreMissing);

        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + inputField + "] is null, cannot parse.");
        }

        Path tempFile = null;

        try {
            tempFile = Files.createTempFile(null, null);
            Files.write(tempFile, input);
            // File options = getOptionFile(this.threshold, this.useOcr, this.extractImages, this.extractTableStructure);
            BodySyncPartitionDocumentV1Options options = new BodySyncPartitionDocumentV1Options();
            List<Element> res = partition(tempFile.toFile(), options);
            if (res != null) {
                String text = joinAllTextRepresentations(res);
                ingestDocument.setFieldValue(this.outputField, text);
                // ingestDocument.setFieldValue("aryn_output", res);
            }
        } catch (IOException e) {
            log.error("Unable to process document: {}", e, e);
            throw new RuntimeException(e);
        } finally {
            if (tempFile != null) {
                Files.delete(tempFile);
            }
        }

        return ingestDocument;
    }

    private List<Element> partition(File input, BodySyncPartitionDocumentV1Options options) throws Exception {
        Object list = AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> {

            try {
                PartitionerResponse res = api.syncPartitionDocumentV1(USER_AGENT, input, null, options, Collections.emptyMap());
                Map<String, List<String>> responseHeaders = this.defaultClient.getResponseHeaders();
                String arynCallId = responseHeaders.get(ARYN_CALL_ID).get(0);
                String arynVersion = responseHeaders.get(ARYN_API_VERSION).get(0);
                log.info("aryn_call_id: {}, aryn_version: {}", arynCallId, arynVersion);
                return res.getElements();
            } catch (ApiException e) {
                // TODO add retries
                log.error("Call to Aryn Partitioner failed: {}", e, e);
                throw new RuntimeException(e);
            }
        });

        return list == null ? null : (List) list;
    }

    @VisibleForTesting
    File getOptionFile(String threshold, boolean useOcr, boolean extractImages, boolean extractTableStructure) {
        String options = threshold.equals("auto") ? String.format(Locale.ROOT,
                "{\"threshold\": \"%s\", \"use_ocr\": \"%s\", \"extract_images\": \"%s\", \"extract_table_structure\": \"%s\"}",
                threshold,
                String.valueOf(useOcr).toLowerCase(Locale.ROOT),
                String.valueOf(extractImages).toLowerCase(Locale.ROOT),
                String.valueOf(extractTableStructure).toLowerCase(Locale.ROOT))
                : String.format(Locale.ROOT,
                        "{\"threshold\": %.3f, \"use_ocr\": \"%s\", \"extract_images\": \"%s\", \"extract_table_structure\": \"%s\"}",
                        Double.valueOf(threshold),
                        String.valueOf(useOcr).toLowerCase(Locale.ROOT),
                        String.valueOf(extractImages).toLowerCase(Locale.ROOT),
                        String.valueOf(extractTableStructure).toLowerCase(Locale.ROOT));
        try {
            Path tempFile = Files.createTempFile(null, null);
            Files.writeString(tempFile, options);
            return tempFile.toFile();
        } catch (IOException ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
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

    // TODO
    private String getImageDescription(byte[] data) {
        String encoded = Base64.getEncoder().encodeToString(data);
        return null;
    }

    // TODO
    private String getTableDescription(Object table) {
        return null;
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
