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

package org.apache.shardingsphere.elasticjob.cloud.scheduler.state.disable.job;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.producer.ProducerManager;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * Cloud job disable listener.
 */
public final class CloudJobDisableListener implements TreeCacheListener {

    private final CoordinatorRegistryCenter regCenter;

    private final ProducerManager producerManager;

    public CloudJobDisableListener(final CoordinatorRegistryCenter regCenter, final ProducerManager producerManager) {
        this.regCenter = regCenter;
        this.producerManager = producerManager;
    }

    @Override
    public void childEvent(final CuratorFramework client, final TreeCacheEvent event) throws Exception {
        String path = event.getData().getPath();
        if (Type.NODE_ADDED == event.getType() && isJobDisableNode(path)) {
            String jobName = path.substring(DisableJobNode.ROOT.length() + 1);
            if (Objects.nonNull(jobName)) {
                producerManager.unschedule(jobName);
            }
        } else if (Type.NODE_REMOVED == event.getType() && isJobDisableNode(path)) {
            String jobName = path.substring(DisableJobNode.ROOT.length() + 1);
            if (Objects.nonNull(jobName)) {
                producerManager.reschedule(jobName);
            }
        }
    }

    private boolean isJobDisableNode(final String path) {
        return path.startsWith(DisableJobNode.ROOT) && path.length() > DisableJobNode.ROOT.length();
    }

    /**
     * Start the listener service of the cloud job service.
     */
    public void start() {
        getCache().getListenable().addListener(this, Executors.newSingleThreadExecutor());
    }

    /**
     * Stop the listener service of the cloud job service.
     */
    public void stop() {
        getCache().getListenable().removeListener(this);
    }

    private TreeCache getCache() {
        TreeCache result = (TreeCache) regCenter.getRawCache(DisableJobNode.ROOT);
        if (null != result) {
            return result;
        }
        regCenter.addCacheData(DisableJobNode.ROOT);
        return (TreeCache) regCenter.getRawCache(DisableJobNode.ROOT);
    }
}
