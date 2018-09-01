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

import com.google.common.base.Splitter;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryBuilder;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.utils.net.Address;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.function.ThrowingConsumer;
import org.apache.camel.util.function.ThrowingFunction;

public class AtomixInstance extends ServiceSupport implements CamelContextAware {
    private final AtomixConfiguration configuration;

    private volatile Atomix atomix;
    private volatile CamelContext context;

    public AtomixInstance(AtomixConfiguration configuration) {
       this(configuration, null);
    }

    public AtomixInstance(AtomixConfiguration configuration, CamelContext context) {
        this.configuration = configuration;
        this.context = context;
        this.atomix = null;
    }

    @Override
    public void setCamelContext(CamelContext camelContext) {
        this.context = context;
    }

    @Override
    public CamelContext getCamelContext() {
        return this.context;
    }

    @Override
    protected synchronized void doStart() throws Exception {
        if (this.atomix == null) {
            this.atomix = getOrCreateAtomixInstance();
            this.atomix.start().get();
        }
    }

    @Override
    protected synchronized void doStop() throws Exception {
        if (this.atomix != null && this.atomix != configuration.getAtomix()) {
            this.atomix.stop().get();
        }
    }

    public <T extends Throwable> void execute(ThrowingConsumer<Atomix, T> consumer) throws T {
        if (atomix == null) {
            throw new IllegalStateException("Atomix has not yet been created");
        }

        consumer.accept(this.atomix);
    }

    public <R, T extends Throwable> R execute(ThrowingFunction<Atomix, R, T> consumer) throws T {
        if (atomix == null) {
            throw new IllegalStateException("Atomix has not yet been created");
        }

        return consumer.apply(this.atomix);
    }

    // *****************************************
    //
    // *****************************************

    public Atomix getOrCreateAtomixInstance() throws Exception {
        Atomix atomix = configuration.getAtomix();

        if (atomix == null) {
            if (ObjectHelper.isNotEmpty(configuration.getConfigurationFile())) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                String path = configuration.getConfigurationFile();

                if (ObjectHelper.isNotEmpty(this.context)) {
                    cl = context.getApplicationContextClassLoader();
                    path = context.resolvePropertyPlaceholders(configuration.getConfigurationFile());
                }

                atomix = Atomix.builder(path, cl).build();
            } else {
                AtomixBuilder builder = Atomix.builder();

                ObjectHelper.ifNotEmpty(configuration.getClusterId(), builder::withClusterId);
                ObjectHelper.ifNotEmpty(configuration.getMemberId(), builder::withMemberId);
                ObjectHelper.ifNotEmpty(configuration.getMemberAddress(), builder::withAddress);
                ObjectHelper.ifNotEmpty(configuration.getProfiles(), builder::withProfiles);

                if (ObjectHelper.isNotEmpty(configuration.getMembershipProvider())) {
                    builder.withMembershipProvider(configuration.getMembershipProvider());

                    if (configuration.getMembershipProvider() instanceof MulticastDiscoveryProvider) {
                        builder.withMulticastEnabled(true);
                    }
                } else {
                    boolean ma = ObjectHelper.isNotEmpty(configuration.getMulticastAddress());
                    boolean me = configuration.isMulticastEnabled() || ma;

                    builder.withMulticastEnabled(me);

                    if (ma) {
                        builder.withMulticastAddress(Address.from(configuration.getMulticastAddress()));
                    }

                    if (!me && ObjectHelper.isNotEmpty(configuration.getNodes())) {
                        BootstrapDiscoveryBuilder discoveryBuilder = new BootstrapDiscoveryBuilder();

                        for (String nodes: configuration.getNodes()) {
                            //
                            // You can define multiple servers in the same string
                            // like:
                            //
                            //   id1@host:port1,id2@host:port2
                            //
                            for (String node: Splitter.on(',').split(nodes)) {
                                String[] items = node.split("@", -1);

                                //
                                // Expecting each node to be in the form:
                                //
                                //   id@host:port
                                //
                                if (items.length != 2) {
                                    throw new IllegalArgumentException("Unknown node format: " + node);
                                }

                                discoveryBuilder.withNodes(
                                    Node.builder().withId(items[0]).withAddress(items[1]).build()
                                );
                            }
                        }

                        builder.withMembershipProvider(discoveryBuilder.build());
                    }
                }

                atomix = builder.build();
            }
        }

        return atomix;
    }
}
