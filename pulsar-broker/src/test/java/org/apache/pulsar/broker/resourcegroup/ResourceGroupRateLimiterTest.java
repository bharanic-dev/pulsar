package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.common.collect.Sets;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResourceGroupRateLimiterTest extends ProducerConsumerBase {

    final String rgName = "testRG";
    org.apache.pulsar.common.policies.data.ResourceGroup testAddRg =
    new org.apache.pulsar.common.policies.data.ResourceGroup();
    final String tenantName = "test-tenant";
    final String namespaceName = "test-tenant/test-namespace";
    final String clusterName = "test";
    final String topicString = "persistent://test-tenant/test-namespace/test-topic";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        prepareData();

    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public void createResourceGroup(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rg) throws PulsarAdminException {
        admin.resourcegroups().createResourceGroup(rgName, rg);

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = pulsar
                .getResourceGroupServiceManager().resourceGroupGet(rgName);
            assertNotNull(resourceGroup);
            assertEquals(rgName, resourceGroup.resourceGroupName);
        });

    }

    public void deleteResourceGroup(String rgName) throws PulsarAdminException {
        admin.resourcegroups().deleteResourceGroup(rgName);
        Awaitility.await().atMost(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName)));
    }

    @Test
    private void testResourceGroupRateLimit() throws Exception {

        createResourceGroup(rgName, testAddRg);

        admin.tenants().createTenant(tenantName,
            new TenantInfoImpl(Sets.newHashSet("fake-admin-role"), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceResourceGroup(namespaceName, rgName);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            assertNotNull(pulsar
                .getResourceGroupServiceManager()
                .getNamespaceResourceGroup(namespaceName)));

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
          assertNotNull(pulsar
            .getResourceGroupServiceManager()
            .resourceGroupGet(rgName).getResourceGroupPublishLimiter()));

        Producer<byte[]> producer = null;
        try {
            producer = pulsarClient.newProducer()
                .topic(topicString)
                .create();
        } catch (PulsarClientException p) {
            final String errMesg = String.format("Got exception while building producer: ex={}", p.getMessage());
            Assert.assertTrue(false, errMesg);
        }

        MessageId messageId = null;
        try {
            // first will be success
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
            // second will be success
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
        } catch (TimeoutException e) {
            // No-op
        }
        Thread.sleep(1000);
        try {
            messageId = producer.sendAsync(new byte[10]).get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNotNull(messageId);
        producer.close();

        admin.namespaces().removeNamespaceResourceGroup(namespaceName);
        admin.namespaces().deleteNamespace(namespaceName);
        deleteResourceGroup(rgName);
    }

    private void prepareData() throws PulsarAdminException {
        testAddRg.setPublishRateInBytes(10);
        testAddRg.setPublishRateInMsgs(1);
        testAddRg.setDispatchRateInMsgs(20000);
        testAddRg.setDispatchRateInBytes(200);
    }
}
