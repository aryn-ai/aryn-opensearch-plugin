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

import org.opensearch.ingest.Processor;

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.*;
import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;

public class ArynIngestProcessorFactory implements Processor.Factory {

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config)
        throws Exception {
        String inputField = readStringProperty(ArynIngestProcessor.TYPE, tag, config, "input_field", "data");
        String outputtField = readStringProperty(ArynIngestProcessor.TYPE, tag, config, "output_field", "parsed_data");
        // List<String> propertyNames = readOptionalList(ArynIngestProcessor.TYPE, processorTag, config, "properties");
        boolean ignoreMissing = readBooleanProperty(ArynIngestProcessor.TYPE, tag, config, "ignore_missing", false);
        String apiKey = readStringProperty(ArynIngestProcessor.TYPE, tag, config, "aryn_api_key");
        String threshold = readStringOrDoubleProperty(ArynIngestProcessor.TYPE, tag, config, "threshold", "auto");
        boolean extractImages = readBooleanProperty(ArynIngestProcessor.TYPE, tag, config, "extract_images", false);
        String textMode = readOptionalStringProperty(ArynIngestProcessor.TYPE, tag, config, "text_mode");
        String tableMode = readOptionalStringProperty(ArynIngestProcessor.TYPE, tag, config, "table_mode");
        boolean summarizeImages = readBooleanProperty(ArynIngestProcessor.TYPE, tag, config, "summarize_images", false);
        String schema = readOptionalStringProperty(ArynIngestProcessor.TYPE, tag, config, "schema");
        //String schemaPath = readOptionalStringProperty(ArynIngestProcessor.TYPE, tag, config, "schema_path");
        String arynUrl = readStringProperty(ArynIngestProcessor.TYPE, tag, config, "aryn_url", "https://api.aryn.ai");

        return new ArynIngestProcessor(tag, description, inputField, outputtField, apiKey, ignoreMissing,
                threshold, extractImages, summarizeImages, textMode, tableMode, schema, arynUrl);
    }

    static String readStringOrDoubleProperty(String processorType, String processorTag, Map<String, Object> configuration, String propertyName, String defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        } else {
            return readStringOrDouble(processorType, processorTag, propertyName, value);
        }
    }

    private static String readStringOrDouble(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String)value;
        } else if (value instanceof Double) {
            return String.valueOf(value);
        } else {
            throw newConfigurationException(processorType, processorTag, propertyName, "property isn't a string or double, but of type [" + value.getClass().getName() + "]");
        }
    }
}
