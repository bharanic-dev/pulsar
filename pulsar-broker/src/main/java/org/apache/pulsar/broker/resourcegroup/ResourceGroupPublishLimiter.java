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

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;

public class ResourceGroupPublishLimiter implements PublishRateLimiter  {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    private RateLimiter publishRateLimiterOnMessage;
    private RateLimiter publishRateLimiterOnByte;
    private final RateLimitFunction rateLimitFunction;

    public ResourceGroupPublishLimiter(ResourceGroup resourceGroup, RateLimitFunction rateLimitFunction) {
        this.rateLimitFunction = rateLimitFunction;
        update(resourceGroup);
    }

    @Override
    public void checkPublishRate() {
        // No-op
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        // No-op
    }

    @Override
    public boolean resetPublishCount() {
    return true;
  }

    @Override
    public boolean isPublishRateExceeded() {
    return false;
  }

    @Override
    public void update(Policies policies, String clusterName) {
      // No-op
    }

    @Override
    public void update(PublishRate maxPublishRate) {
      // No-op
    }

    public void update(ResourceGroup resourceGroup) {
        if (resourceGroup != null
            && (resourceGroup.getDispatchRateInMsgs() > 0 || resourceGroup.getDispatchRateInBytes() > 0)) {
            this.publishThrottlingEnabled = true;
            this.publishMaxMessageRate = Math.max(resourceGroup.getDispatchRateInMsgs(), 0);
            this.publishMaxByteRate = Math.max(resourceGroup.getDispatchRateInBytes(), 0);
            if (this.publishMaxMessageRate > 0) {
                publishRateLimiterOnMessage =
                    new RateLimiter(publishMaxMessageRate, 1, TimeUnit.SECONDS, rateLimitFunction);
            }
            if (this.publishMaxByteRate > 0) {
                publishRateLimiterOnByte = new RateLimiter(publishMaxByteRate, 1, TimeUnit.SECONDS);
            }
        } else {
            this.publishMaxMessageRate = 0;
            this.publishMaxByteRate = 0;
            this.publishThrottlingEnabled = false;
            publishRateLimiterOnMessage = null;
            publishRateLimiterOnByte = null;
        }
    }

    public boolean tryAcquire(int numbers, long bytes) {
        return (publishRateLimiterOnMessage == null || publishRateLimiterOnMessage.tryAcquire(numbers))
            && (publishRateLimiterOnByte == null || publishRateLimiterOnByte.tryAcquire(bytes));
    }
}