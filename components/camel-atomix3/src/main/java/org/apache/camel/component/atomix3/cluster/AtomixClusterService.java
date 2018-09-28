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
package org.apache.camel.component.atomix3.cluster;

import java.util.List;

import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import org.apache.camel.CamelContext;
import org.apache.camel.component.atomix3.AtomixConfiguration;
import org.apache.camel.component.atomix3.AtomixInstance;
import org.apache.camel.impl.cluster.AbstractCamelClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AtomixClusterService extends AbstractCamelClusterService<AtomixClusterView> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomixClusterService.class);

    private final AtomixConfiguration configuration;
    private final AtomixInstance atomix;

    public AtomixClusterService() {
        this(null, new AtomixConfiguration());
    }

    public AtomixClusterService(CamelContext camelContext, AtomixConfiguration configuration) {
        super(null, camelContext);

        this.configuration = configuration.copy();
        this.atomix = new AtomixInstance(this.configuration);
    }

    // **********************************
    // Properties
    // **********************************

    public Atomix getAtomix() {
        return configuration.getAtomix();
    }

    public void setAtomix(Atomix client) {
        configuration.setAtomix(client);
    }

    public String getMemberAddress() {
        return configuration.getMemberAddress();
    }

    public void setMemberAddress(String memberAddress) {
        configuration.setMemberAddress(memberAddress);
    }

    public void setMemberAddress(String host, int port) {
        configuration.setMemberAddress(host, port);
    }

    public List<String> getNodes() {
        return configuration.getNodes();
    }

    public void setNodes(List<String> nodes) {
        configuration.setNodes(nodes);
    }

    public void setNodes(String... nodes) {
        configuration.setNodes(nodes);
    }

    public void addNodes(String... nodes) {
        configuration.addNodes(nodes);
    }

    public NodeDiscoveryProvider getMembershipProvider() {
        return configuration.getMembershipProvider();
    }

    public void setMembershipProvider(NodeDiscoveryProvider membershipProvider) {
        configuration.setMembershipProvider(membershipProvider);
    }

    public String getConfigurationFile() {
        return configuration.getConfigurationFile();
    }

    public void setConfigurationFile(String configurationFile) {
        configuration.setConfigurationFile(configurationFile);
    }

    public String getMemberId() {
        return configuration.getMemberId();
    }

    public void setMemberId(String memberId) {
        configuration.setMemberId(memberId);
    }

    public String getClusterId() {
        return configuration.getClusterId();
    }

    public void setClusterId(String clusterId) {
        configuration.setClusterId(clusterId);
    }

    public boolean isMulticastEnabled() {
        return configuration.isMulticastEnabled();
    }

    public void setMulticastEnabled(boolean multicastEnabled) {
        configuration.setMulticastEnabled(multicastEnabled);
    }

    public String getMulticastAddress() {
        return configuration.getMulticastAddress();
    }

    public void setMulticastAddress(String multicastAddress) {
        configuration.setMulticastAddress(multicastAddress);
    }

    public List<Profile> getProfiles() {
        return configuration.getProfiles();
    }

    public void setProfiles(List<Profile> profiles) {
        configuration.setProfiles(profiles);
    }

    public void setProfiles(Profile... profiles) {
        configuration.setProfiles(profiles);
    }

    public void addProfiles(Profile... profiles) {
        configuration.addProfiles(profiles);
    }

    public ManagedPartitionGroup getSystemPartitionGroup() {
        return configuration.getSystemPartitionGroup();
    }

    public void setSystemPartitionGroup(ManagedPartitionGroup systemPartitionGroup) {
        configuration.setSystemPartitionGroup(systemPartitionGroup);
    }

    public List<ManagedPartitionGroup> getPrimitivePartitionGroups() {
        return configuration.getPrimitivePartitionGroups();
    }

    public void setPrimitivePartitionGroups(List<ManagedPartitionGroup> primitivePartitionGroups) {
        configuration.setPrimitivePartitionGroups(primitivePartitionGroups);
    }

    public void setPrimitivePartitionGroups(ManagedPartitionGroup... primitivePartitionGroups) {
        configuration.setPrimitivePartitionGroups(primitivePartitionGroups);
    }


    // *********************************************
    // Lifecycle
    // *********************************************

    @Override
    protected void doStart() throws Exception {
        LOGGER.debug("Joining atomix cluster {}", configuration.getClusterId());
        this.atomix.start();

        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();

        LOGGER.debug("Leaving atomix cluster {}", configuration.getClusterId());
        atomix.stop();
    }

    @Override
    protected AtomixClusterView createView(String namespace) throws Exception {
        return new AtomixClusterView(this, namespace, atomix, configuration);
    }
}
