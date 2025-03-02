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
import lombok.Builder;
import lombok.Getter;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


@Builder
public class DocParseOptions {
    private static Gson gson = new GsonBuilder().create();
    @Getter
    String input_field;
    @Getter
    String output_field;
    @Getter
    boolean ignore_missing;
    @Getter
    String aryn_api_key;
    String threshold;
    boolean use_ocr;
    String ocr_language;
    boolean extract_images;
    boolean extract_table_structure;
    boolean summarize_images;
    Map<String, Object> chunking_options;


    public String toJsonString() throws Exception {
        String tmp = AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(this));
        Map<String, Object> json = gson.fromJson(tmp, Map.class);
        Map<String, Object> ret = new HashMap<>();
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            Object val = entry.getValue();
            if (val == null) {
                continue;
            }
            if ((val instanceof Collection) && ((Collection) val).isEmpty()) {
                continue;
            }
            ret.put(entry.getKey(), val);
        }
        return AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(ret));
    }
}
