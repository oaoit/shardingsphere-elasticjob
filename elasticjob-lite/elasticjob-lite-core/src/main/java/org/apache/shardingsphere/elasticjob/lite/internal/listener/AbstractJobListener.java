/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.lite.internal.listener;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.nio.charset.StandardCharsets;

/**
 * Job Listener.
 */
public abstract class AbstractJobListener implements TreeCacheListener {

    @Override
    public final void childEvent(final CuratorFramework client, final TreeCacheEvent event) {
        if (null == event || null == event.getData()) {
            return;
        }
        String path = event.getData().getPath();
        byte[] data = event.getData().getData();
        if (StringUtils.isBlank(path)) {
            return;
        }
        dataChanged(path, event.getType(), null == data ? "" : new String(data, StandardCharsets.UTF_8));
    }

    protected abstract void dataChanged(String path, Type eventType, String data);
}
