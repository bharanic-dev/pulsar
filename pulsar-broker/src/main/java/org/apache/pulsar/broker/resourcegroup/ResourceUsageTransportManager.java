/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.resourcegroup;

import static org.apache.pulsar.client.api.CompressionType.SNAPPY;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsageInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUsageTransportManager implements AutoCloseable {

    private class ResourceUsageWriterTask implements Runnable, AutoCloseable {
        private final Producer<byte[]> producer;
        private final ScheduledFuture<?> resourceUsagePublishTask;

        private Producer<byte[]> createProducer() throws PulsarClientException {
            final int publishDelayMilliSecs = 10;
            final int sendTimeoutSecs = 10;

            return pulsarClient.newProducer()
                    .topic(pulsarService.getConfig().getResourceUsagePublishTopicName())
                    .batchingMaxPublishDelay(publishDelayMilliSecs, TimeUnit.MILLISECONDS)
                    .sendTimeout(sendTimeoutSecs, TimeUnit.SECONDS)
                    .blockIfQueueFull(false)
                    .compressionType(SNAPPY)
                    .create();
        }

        public ResourceUsageWriterTask() throws PulsarClientException {
            producer = createProducer();
            resourceUsagePublishTask = pulsarService.getExecutor().scheduleAtFixedRate(
                    this,
                    pulsarService.getConfig().getResourceUsagePublishIntervalInSecs(),
                    pulsarService.getConfig().getResourceUsagePublishIntervalInSecs(),
                    TimeUnit.SECONDS);
        }

        @Override
        public void run() {
            if (!publisherMap.isEmpty()) {
                ResourceUsageInfo rUsageInfo = new ResourceUsageInfo();
                rUsageInfo.setBroker(pulsarService.getBrokerServiceUrl());

                publisherMap.forEach((key, item) -> item.fillResourceUsage(rUsageInfo.addUsageMap()));

                ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer(rUsageInfo.getSerializedSize());
                rUsageInfo.writeTo(buf);

                byte[] bytes = buf.array();
                producer.sendAsync(bytes).whenComplete((id, ex) -> {
                    if (null != ex) {
                        LOG.error("Resource usage publisher: sending message ID {} error {}", id, ex);
                    }
                    buf.release();
                });
            }
        }

        @Override
        public void close() throws Exception {
            resourceUsagePublishTask.cancel(true);
            producer.close();
        }
    }

    private class ResourceUsageReader implements ReaderListener<byte[]>, AutoCloseable {
        private final ResourceUsageInfo recdUsageInfo = new ResourceUsageInfo();
        private final Reader<byte[]> consumer;

        public ResourceUsageReader() throws PulsarClientException {
            consumer =  pulsarClient.newReader()
                    .topic(pulsarService.getConfig().getResourceUsagePublishTopicName())
                    .startMessageId(MessageId.latest)
                    .readerListener(this)
                    .create();
            }

        @Override
        public void close() throws Exception {
            consumer.close();
        }

        @Override
        public void received(Reader<byte[]> reader, Message<byte[]> msg) {
            try {
                recdUsageInfo.parseFrom(Unpooled.wrappedBuffer(msg.getData()), msg.getData().length);

                recdUsageInfo.getUsageMapsList().forEach(ru -> {
                    ResourceUsageConsumer owner = consumerMap.get(ru.getOwner());
                    if (owner != null) {
                        owner.resourceUsageListener(recdUsageInfo.getBroker(), ru);
                    }
                });

            } catch (IllegalStateException exception) {
                LOG.error("Resource usage reader: Error parsing incoming message {}", exception);
            } catch (Exception exception) {
                LOG.error("Resource usage reader: Unknown exception while parsing message {}", exception);
            }
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(ResourceUsageTransportManager.class);
    private final PulsarService pulsarService;
    private final PulsarClient pulsarClient;
    private final ResourceUsageWriterTask pTask;
    private final ResourceUsageReader consumer;
    private final Map<String, ResourceUsagePublisher>
            publisherMap = new ConcurrentHashMap<String, ResourceUsagePublisher>();
    private final Map<String, ResourceUsageConsumer>
            consumerMap = new ConcurrentHashMap<String, ResourceUsageConsumer>();

    private void createTenantAndNamespace() throws PulsarServerException, PulsarAdminException {
        // Create a public tenant and default namespace
        TopicName topicName = TopicName.get(pulsarService.getConfig().getResourceUsagePublishTopicName());

        PulsarAdmin admin = pulsarService.getAdminClient();
        ServiceConfiguration config = pulsarService.getConfig();
        String cluster = config.getClusterName();

        final String tenant = topicName.getTenant();
        final String namespace = topicName.getNamespace();

        List<String> tenantList =  admin.tenants().getTenants();
        if (!tenantList.contains(tenant)) {
            admin.tenants().createTenant(tenant,
                    new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
        }
        List<String> nsList = admin.namespaces().getNamespaces(tenant);
        if (!nsList.contains(namespace)) {
            admin.namespaces().createNamespace(namespace);
        }
    }

    public ResourceUsageTransportManager(PulsarService pulsarService) throws Exception {
        this.pulsarService = pulsarService;
        this.pulsarClient = pulsarService.getClient();

        try {
            createTenantAndNamespace();
            consumer = new ResourceUsageReader();
            pTask = new ResourceUsageWriterTask();
        } catch (Exception ex) {
            LOG.error("Error initializing resource usage transport manager: {}", ex);
            throw ex;
        }
    }

    /*
     * Register a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage publisher
     */
    public void registerResourceUsagePublisher(ResourceUsagePublisher r) {
        publisherMap.put(r.getID(), r);
    }

    /*
     * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage publisher
     */
    public void unregisterResourceUsageConsumer(ResourceUsagePublisher r) {
        publisherMap.remove(r.getID());
    }

    /*
     * Register a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage consumer
     */
    public void registerResourceUsageConsumer(ResourceUsageConsumer r) {
        consumerMap.put(r.getID(), r);
    }

    /*
     * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage consumer
     */
    public void unregisterResourceUsageConsumer(ResourceUsageConsumer r) {
        consumerMap.remove(r.getID());
    }

    @Override
    public void close() throws Exception {
        try {
            pTask.close();
            consumer.close();
        } catch (Exception ex1) {
            LOG.error("Error closing producer/consumer for resource-usage topic {}", ex1);
        }
    }
}