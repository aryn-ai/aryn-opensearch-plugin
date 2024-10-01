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

import ai.aryn.partitioner.ApiClient;
import ai.aryn.partitioner.ApiException;
import ai.aryn.partitioner.Configuration;
import ai.aryn.partitioner.api.DefaultApi;
import ai.aryn.partitioner.auth.HttpBearerAuth;
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
public class SycamoreIngestProcessor extends AbstractProcessor {

    public static final String TYPE = "sycamore_ingest";

    public static final String USER_AGENT = "SycamoreIngestPlugin_v0.1.0";

    private final String inputField;
    private final String outputField;
    private final boolean ignoreMissing;
    private final boolean useOcr;
    private final boolean extractImages;
    private final boolean extractTableStructure;

    final ApiClient defaultClient = Configuration.getDefaultApiClient();
    final DefaultApi apiInstance;

    protected SycamoreIngestProcessor(String tag, String description,
                                      String inputField, String outputField,
                                      String apiKey, boolean ignoreMissing,
                                      boolean useOcr, boolean extractImages, boolean extractTableStructure) {
        super(tag, description);
        this.inputField = inputField;
        this.outputField = outputField;
        this.ignoreMissing = ignoreMissing;
        this.useOcr = useOcr;
        this.extractImages = extractImages;
        this.extractTableStructure = extractTableStructure;

        defaultClient.setBasePath("https://api.aryn.cloud");

        // Configure HTTP bearer authorization: HTTPBearer
        HttpBearerAuth HTTPBearer = (HttpBearerAuth) defaultClient.getAuthentication("HTTPBearer");
        HTTPBearer.setBearerToken(apiKey);

        apiInstance = new DefaultApi(defaultClient);
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
            File options = getOptionFile(this.useOcr, this.extractImages, this.extractTableStructure);
            List res = partition(tempFile.toFile(), options);
            if (res != null) {
                String text = joinAllTextRepresentations(res);
                ingestDocument.setFieldValue(this.outputField, text);
                ingestDocument.setFieldValue("partitioner_output", res);
            }
            options.delete();
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

    private List partition(File input, File options) throws Exception {
        Object list = AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> {

            Object elements = null;
            try {
                Object result = apiInstance.partition(input, USER_AGENT, options);
                // System.out.println(result);
                if (!(result instanceof Map)) {
                    log.error("Unexpected response from APS: {}", result.toString());
                    return null;
                }
                elements = ((Map) result).get("elements");
                if (elements == null) {
                    log.warn("APS response does not contain any elements: {}", result.toString());
                    return null;
                }
                if (!(elements instanceof List)) {
                    log.error("The elements field in the APS response is not a List!!! {}", elements.toString());
                    return null;
                }

                /*
                 * for debugging
                for (Object element : ((List) elements)) {
                    assert element instanceof Map;
                    Map map = (Map) element;
                    assert map.containsKey("type");
                    System.out.println(map.get("type"));
                    if (map.containsKey("text_representation")) {
                        System.out.println(map.get("text_representation"));
                    }
                }*/
            } catch (ApiException e) {
                // TODO add retries
                log.error("Call to Aryn Partitioner failed: {}", e, e);
                throw new RuntimeException(e);
            }

            return elements;
        });

        return list == null ? null : (List) list;
    }

    private File getOptionFile(boolean useOcr, boolean extractImages, boolean extractTableStructure) {
        String options = String.format(Locale.ROOT, "{\"use_ocr\": \"%s\", \"extract_images\": \"%s\", \"extract_table_structure\": \"%s\"}",
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

    private String joinAllTextRepresentations(List list) {
        StringBuilder builder = new StringBuilder();
        for (Object obj : list) {
            Map map = (Map) obj;
            if (map.containsKey("text_representation")) {
                builder.append(String.format(Locale.ROOT, "%s%n", (String) map.get("text_representation")));
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

    // TODO
    static class Element {}
    static class TextElement extends Element {}
    static class ImageElement extends Element {}
    static class TableElement extends Element {}
}
