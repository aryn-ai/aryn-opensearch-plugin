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

import org.opensearch.ingest.Processor;

import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.*;
import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;

public class SycamoreIngestProcessorFactory implements Processor.Factory {

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config)
        throws Exception {
        String inputField = readStringProperty(SycamoreIngestProcessor.TYPE, tag, config, "input_field", "data");
        String outputtField = readStringProperty(SycamoreIngestProcessor.TYPE, tag, config, "output_field", "parsed_data");
        // List<String> propertyNames = readOptionalList(SycamoreIngestProcessor.TYPE, processorTag, config, "properties");
        boolean ignoreMissing = readBooleanProperty(SycamoreIngestProcessor.TYPE, tag, config, "ignore_missing", false);
        String apiKey = readStringProperty(SycamoreIngestProcessor.TYPE, tag, config, "aryn_api_key");
        String threshold = readStringOrDoubleProperty(SycamoreIngestProcessor.TYPE, tag, config, "threshold", "auto");
        boolean useOcr = readBooleanProperty(SycamoreIngestProcessor.TYPE, tag, config, "use_ocr", false);
        boolean extractImages = readBooleanProperty(SycamoreIngestProcessor.TYPE, tag, config, "extract_images", false);
        boolean extractTableStructure = readBooleanProperty(SycamoreIngestProcessor.TYPE, tag, config, "extract_table_structure", false);
        boolean summarizeImages = readBooleanProperty(SycamoreIngestProcessor.TYPE, tag, config, "summarize_images", false);

        // Apply Tika?

        return new SycamoreIngestProcessor(tag, description, inputField, outputtField, apiKey, ignoreMissing,
                threshold, useOcr, extractImages, extractTableStructure, summarizeImages);
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
