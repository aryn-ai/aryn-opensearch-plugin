/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.sycamore.ingest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.*;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ml.common.model.MLModelState;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.hamcrest.Matchers.containsString;

// @ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SycamoreIngestPluginIT extends OpenSearchRestTestCase {
    protected final ClassLoader classLoader = this.getClass().getClassLoader();
    private static final String TEST_DATA_ROOT = "org/opensearch/sycamore/ingest/test_data/";
    private static final Map<String, String> PIPELINE_CONFIGS_BY_NAME = Map.of("simple", TEST_DATA_ROOT + "pipeline_configuration.json");
    protected static final Locale LOCALE = Locale.ROOT;
    public static final String DEFAULT_USER_AGENT = "sycamore-ingest-integ-test";
    private static final String INDEX_NAME = "test_index";
    public static final int MAX_TIME_OUT_INTERVAL = 3000;
    public static final int MAX_RETRY = 5;
    //@Override
    //protected Collection<Class<? extends Plugin>> nodePlugins() {
    //    return Collections.singletonList(SycamoreIngestPlugin.class);
    //}

    public void testPluginInstalled() throws IOException, ParseException {
        Response response = client().performRequest(new Request("GET", "/_cat/plugins"));
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        logger.info("response body: {}", body);
        assertThat(body, containsString("sycamore-ingest"));
    }

    @SneakyThrows
    public void testSycamoreIngestProcessor() {
        String pipelineName = "simple";
        try {
            createPipelineProcessor(pipelineName);
            createIndex(INDEX_NAME, pipelineName);
            ingestDocument(TEST_DATA_ROOT + "national_parks_images_table.pdf");

            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("Yosemite");
            expectedPassages.add("Yellowstone");  // contains a single paragraph, two sentences and 24 tokens by ");
            // expectedPassages.add("standard tokenizer in OpenSearch.");
            validateIndexIngestResults(INDEX_NAME, "extracted", expectedPassages);
        } finally {
            wipeOffTestResources(INDEX_NAME, pipelineName, null, null);
        }
    }


    /*********************************************************************************
     *   Borrowed from neural-search test fixtures
     ***********************************************************************************/


    private void createPipelineProcessor(String pipelineName) throws Exception {
        URL pipelineURLPath = classLoader.getResource(PIPELINE_CONFIGS_BY_NAME.get(pipelineName));
        Objects.requireNonNull(pipelineURLPath);
        String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
        String arynApiKey = System.getenv("ARYN_TOKEN");
        requestBody = requestBody.replaceAll("__ARYN_TOKEN__", arynApiKey);
        createPipelineProcessor(requestBody, pipelineName, "", null);
    }

    protected void createPipelineProcessor(
            final String requestBody,
            final String pipelineName,
            final String modelId,
            final Integer batchSize
    ) throws Exception {
        Response pipelineCreateResponse = makeRequest(
                client(),
                "PUT",
                "/_ingest/pipeline/" + pipelineName,
                null,
                toHttpEntity(String.format(LOCALE, requestBody, modelId, batchSize == null ? 1 : batchSize)),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(pipelineCreateResponse.getEntity()),
                false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    protected static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            HttpEntity entity,
            List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    protected static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            HttpEntity entity,
            List<Header> headers,
            boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.forEach(request::addParameter);
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    protected static HttpEntity toHttpEntity(String jsonString) {
        return new StringEntity(jsonString, ContentType.APPLICATION_JSON);
    }

    private void createIndex(String indexName, String pipelineName) throws Exception {
        URL indexSettingsURLPath = classLoader.getResource(TEST_DATA_ROOT + "index_settings.json");
        Objects.requireNonNull(indexSettingsURLPath);
        createIndexWithConfiguration(indexName, Files.readString(Path.of(indexSettingsURLPath.toURI())), pipelineName);
    }

    protected void createIndexWithConfiguration(final String indexName, String indexConfiguration, final String pipelineName)
            throws Exception {
        if (StringUtils.isNotBlank(pipelineName)) {
            indexConfiguration = String.format(LOCALE, indexConfiguration, pipelineName);
        }
        Response response = makeRequest(
                client(),
                "PUT",
                indexName,
                null,
                toHttpEntity(indexConfiguration),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(response.getEntity()),
                false
        );
        assertEquals("true", node.get("acknowledged").toString());
        assertEquals(indexName, node.get("index").toString());
    }

    private void ingestDocument(String documentPath) throws Exception {
        URL documentURLPath = classLoader.getResource(documentPath);
        Objects.requireNonNull(documentURLPath);
        // String document = Files.readString(Path.of(documentURLPath.toURI()));
        byte[] docBytes = FileUtils.readFileToByteArray(Path.of(classLoader.getResource(documentPath).toURI()).toFile());
        String docContent = Base64.getEncoder().encodeToString(docBytes);
        String requestBody = String.format(LOCALE, "{ \"data\": \"%s\"}", docContent);
        Response response = makeRequest(
                client(),
                "POST",
                INDEX_NAME + "/_doc?refresh",
                null,
                new InputStreamEntity(new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)), ContentType.APPLICATION_JSON),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "sycamore-ingest-it"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(response.getEntity()),
                false
        );
        assertEquals("created", map.get("result"));
    }

    private void validateIndexIngestResults(String indexName, String fieldName, Object expected) {
        assertEquals(1, getDocCount(indexName));
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        Map<String, Object> searchResults = search(indexName, query, 10);
        assertNotNull(searchResults);
        Map<String, Object> document = getFirstInnerHit(searchResults);
        assertNotNull(document);
        Object documentSource = document.get("_source");
        assert (documentSource instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> documentSourceMap = (Map<String, Object>) documentSource;
        assert (documentSourceMap).containsKey(fieldName);
        Object ingestOutputs = documentSourceMap.get(fieldName);
        assertEquals(expected, ingestOutputs);
    }

    @SneakyThrows
    protected int getDocCount(final String indexName) {
        Request request = new Request("GET", "/" + indexName + "/_count");
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        return (Integer) responseMap.get("count");
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getFirstInnerHit(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hits2List = (List<Object>) hits1map.get("hits");
        assertTrue(hits2List.size() > 0);
        return (Map<String, Object>) hits2List.get(0);
    }

    protected Map<String, Object> search(final String index, final QueryBuilder queryBuilder, final int resultSize) {
        return search(index, queryBuilder, null, resultSize);
    }

    @SneakyThrows
    protected Map<String, Object> search(
            final String index,
            final QueryBuilder queryBuilder,
            final QueryBuilder rescorer,
            final int resultSize
    ) {
        return search(index, queryBuilder, rescorer, resultSize, Map.of());
    }

    @SneakyThrows
    protected Map<String, Object> search(
            final String index,
            final QueryBuilder queryBuilder,
            final QueryBuilder rescorer,
            final int resultSize,
            final Map<String, String> requestParams
    ) {
        return search(index, queryBuilder, rescorer, resultSize, requestParams, null);
    }

    @SneakyThrows
    protected Map<String, Object> search(
            String index,
            QueryBuilder queryBuilder,
            QueryBuilder rescorer,
            int resultSize,
            Map<String, String> requestParams,
            List<Object> aggs
    ) {
        return search(index, queryBuilder, rescorer, resultSize, requestParams, aggs, null, null, false, null, 0);
    }

    @SneakyThrows
    protected Map<String, Object> search(
            String index,
            QueryBuilder queryBuilder,
            QueryBuilder rescorer,
            int resultSize,
            Map<String, String> requestParams,
            List<Object> aggs,
            QueryBuilder postFilterBuilder,
            List<SortBuilder<?>> sortBuilders,
            boolean trackScores,
            List<Object> searchAfter,
            int from
    ) {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("from", from);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        if (rescorer != null) {
            builder.startObject("rescore").startObject("query").field("query_weight", 0.0f).field("rescore_query");
            rescorer.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject().endObject();
        }
        if (Objects.nonNull(aggs)) {
            builder.startObject("aggs");
            for (Object agg : aggs) {
                builder.value(agg);
            }
            builder.endObject();
        }
        if (Objects.nonNull(postFilterBuilder)) {
            builder.field("post_filter");
            postFilterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        if (Objects.nonNull(sortBuilders) && !sortBuilders.isEmpty()) {
            builder.startArray("sort");
            for (SortBuilder sortBuilder : sortBuilders) {
                sortBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray();
        }

        if (trackScores) {
            builder.field("track_scores", trackScores);
        }
        if (searchAfter != null && !searchAfter.isEmpty()) {
            builder.startArray("search_after");
            for (Object searchAfterEntry : searchAfter) {
                builder.value(searchAfterEntry);
            }
            builder.endArray();
        }

        builder.endObject();

        Request request = new Request("GET", "/" + index + "/_search?timeout=1000s");
        request.addParameter("size", Integer.toString(resultSize));
        if (requestParams != null && !requestParams.isEmpty()) {
            requestParams.forEach(request::addParameter);
        }
        logger.info("Sorting request  " + builder.toString());
        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        logger.info("Response  " + responseBody);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    protected void wipeOffTestResources(
            final String indexName,
            final String ingestPipeline,
            final String modelId,
            final String searchPipeline
    ) throws IOException {
        if (ingestPipeline != null) {
            deletePipeline(ingestPipeline);
        }
        if (searchPipeline != null) {
            deleteSearchPipeline(searchPipeline);
        }
        if (modelId != null) {
            try {
                deleteModel(modelId);
            } catch (AssertionError e) {
                // sometimes we have flaky test that the model state doesn't change after call undeploy api
                // for this case we can call undeploy api one more time
                deleteModel(modelId);
            }
        }
        if (indexName != null) {
            deleteIndex(indexName);
        }
    }

    @SneakyThrows
    protected Map<String, Object> deletePipeline(final String pipelineName) {
        Request request = new Request("DELETE", "/_ingest/pipeline/" + pipelineName);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        return responseMap;
    }

    @SneakyThrows
    protected void deleteSearchPipeline(final String pipelineId) {
        makeRequest(
                client(),
                "DELETE",
                String.format(LOCALE, "/_search/pipeline/%s", pipelineId),
                null,
                toHttpEntity(""),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SneakyThrows
    protected void deleteModel(String modelId) {
        // need to undeploy first as model can be in use
        makeRequest(
                client(),
                "POST",
                String.format(LOCALE, "/_plugins/_ml/models/%s/_undeploy", modelId),
                null,
                toHttpEntity(""),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        // wait for model undeploy to complete.
        // Sometimes the undeploy action results in a DEPLOY_FAILED state. But this does not block the model from being deleted.
        // So set both UNDEPLOYED and DEPLOY_FAILED as exit state.
        pollForModelState(modelId, Set.of(MLModelState.UNDEPLOYED, MLModelState.DEPLOY_FAILED));

        makeRequest(
                client(),
                "DELETE",
                String.format(LOCALE, "/_plugins/_ml/models/%s", modelId),
                null,
                toHttpEntity(""),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    protected void pollForModelState(String modelId, Set<MLModelState> exitModelStates) throws InterruptedException {
        MLModelState currentState = null;
        for (int i = 0; i < MAX_RETRY; i++) {
            Thread.sleep(MAX_TIME_OUT_INTERVAL);
            currentState = getModelState(modelId);
            if (exitModelStates.contains(currentState)) {
                return;
            }
        }
        fail(
                String.format(
                        LOCALE,
                        "Model state does not reached exit states %s after %d attempts with interval of %d ms, latest model state: %s.",
                        StringUtils.join(exitModelStates, ","),
                        MAX_RETRY,
                        MAX_TIME_OUT_INTERVAL,
                        currentState
                )
        );
    }

    @SneakyThrows
    protected MLModelState getModelState(String modelId) {
        Response getModelResponse = makeRequest(
                client(),
                "GET",
                String.format(LOCALE, "/_plugins/_ml/models/%s", modelId),
                null,
                toHttpEntity(""),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> getModelResponseJson = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(getModelResponse.getEntity()),
                false
        );
        String modelState = (String) getModelResponseJson.get("model_state");
        return MLModelState.valueOf(modelState);
    }
}
