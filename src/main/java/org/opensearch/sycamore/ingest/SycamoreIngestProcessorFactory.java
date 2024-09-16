/*
 * Copyright 2023 Aryn
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
package org.opensearch.sycamore.ingest;

import org.opensearch.ingest.Processor;

import java.util.Map;

public class SycamoreIngestProcessorFactory implements Processor.Factory {

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config)
        throws Exception {
        return null;
    }
}
