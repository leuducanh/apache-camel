/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.atomix3;

import java.util.UUID;

import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.core.profile.Profile;
import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.test.AvailablePortFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtomixInstanceMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomixInstanceMain.class);

    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();

        try {
            AtomixConfiguration configuration = new AtomixConfiguration();
            configuration.setClusterId("test");
            configuration.setMemberAddress("localhost:" + AvailablePortFinder.getNextAvailable());
            configuration.setMemberId(UUID.randomUUID().toString());
            configuration.setMembershipProvider(new MulticastDiscoveryProvider());
            configuration.setProfiles(Profile.client());

            AtomixInstance instance = new AtomixInstance(configuration, context);

            instance.start();

            LOGGER.info("==== started ====");

            Thread.sleep(1000 * 30);
            instance.stop();
        } finally {
            context.stop();
        }
    }
}
