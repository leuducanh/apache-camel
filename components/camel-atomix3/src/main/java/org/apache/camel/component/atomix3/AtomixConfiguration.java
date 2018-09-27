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

import java.util.ArrayList;
import java.util.List;

import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import org.apache.camel.spi.UriParam;

public class AtomixConfiguration implements Cloneable {
    @UriParam(javaType = "io.atomix.core.Atomix")
    private Atomix atomix;
    @UriParam
    private String memberAddress;
    @UriParam(defaultValue = "false")
    private boolean multicastEnabled;
    @UriParam
    private String multicastAddress;
    @UriParam
    private List<String> nodes;
    @UriParam
    private NodeDiscoveryProvider membershipProvider;
    @UriParam
    private String configurationFile;
    @UriParam
    private String memberId;
    @UriParam
    private String clusterId;
    @UriParam(label = "advanced")
    private List<Profile> profiles;
    @UriParam(label = "advanced")
    private ManagedPartitionGroup systemPartitionGroup;
    @UriParam(label = "advanced")
    private List<ManagedPartitionGroup> primitivePartitionGroups;

    // *****************************************
    // Properties
    // *****************************************

    public Atomix getAtomix() {
        return atomix;
    }

    /**
     * The Atomix instance to use
     */
    public void setAtomix(Atomix client) {
        this.atomix = client;
    }

    public String getMemberAddress() {
        return memberAddress;
    }

    /**
     * Sets the member address.
     */
    public void setMemberAddress(String memberAddress) {
        this.memberAddress = memberAddress;
    }

    /**
     * Sets the member address.
     */
    public void setMemberAddress(String host, int port) {
        this.memberAddress = host + ":" +port;
    }

    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Sets the bootstrap nodes in the form id@host:port.
     */
    public void setNodes(List<String> nodes) {
        this.nodes = new ArrayList<>(nodes);
    }

    /**
     * Sets the bootstrap nodes in the form id@host:port.
     */
    public void setNodes(String... nodes) {
        if (this.nodes == null) {
            this.nodes = new ArrayList<>();
        }

        this.nodes.clear();

        for (String node: nodes) {
            this.nodes.add(node);
        }
    }

    public void addNodes(String... nodes) {
        if (this.nodes == null) {
            this.nodes = new ArrayList<>();
        }

        for (String node: nodes) {
            this.nodes.add(node);
        }
    }

    public NodeDiscoveryProvider getMembershipProvider() {
        return membershipProvider;
    }

    /**
     * Sets the cluster membership provider.
     */
    public void setMembershipProvider(NodeDiscoveryProvider membershipProvider) {
        this.membershipProvider = membershipProvider;
    }

    public String getConfigurationFile() {
        return configurationFile;
    }

    /**
     * Sets the Atomix configuration file.
     */
    public void setConfigurationFile(String configurationFile) {
        this.configurationFile = configurationFile;
    }

    public String getMemberId() {
        return memberId;
    }

    /**
     * Sets the local member identifier.
     */
    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getClusterId() {
        return clusterId;
    }

    /**
     * Sets the cluster identifier used to verify intra-cluster communication is
     * taking place between nodes that are intended to be part of the same cluster.
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public boolean isMulticastEnabled() {
        return multicastEnabled;
    }

    public void setMulticastEnabled(boolean multicastEnabled) {
        this.multicastEnabled = multicastEnabled;
    }

    public String getMulticastAddress() {
        return multicastAddress;
    }

    /**
     * Sets the multicast address.
     */
    public void setMulticastAddress(String multicastAddress) {
        this.multicastAddress = multicastAddress;
    }

    public List<Profile> getProfiles() {
        return profiles;
    }

    public void setProfiles(List<Profile> profiles) {
        this.profiles = new ArrayList<>(profiles);
    }

    public void setProfiles(Profile... profiles) {
        if (this.profiles == null) {
            this.profiles = new ArrayList<>();
        } else {
            this.profiles.clear();
        }

        for (Profile profile: profiles) {
            this.profiles.add(profile);
        }
    }

    public void addProfiles(Profile... profiles) {
        if (this.profiles == null) {
            this.profiles = new ArrayList<>();
        }

        for (Profile profile: profiles) {
            this.profiles.add(profile);
        }
    }

    public ManagedPartitionGroup getSystemPartitionGroup() {
        return systemPartitionGroup;
    }

    /**
     * Sets the system partition groups.
     */
    public void setSystemPartitionGroup(ManagedPartitionGroup systemPartitionGroup) {
        this.systemPartitionGroup = systemPartitionGroup;
    }

    public List<ManagedPartitionGroup> getPrimitivePartitionGroups() {
        return primitivePartitionGroups;
    }

    /**
     * Sets the primitive partition groups.
     */
    public void setPrimitivePartitionGroups(List<ManagedPartitionGroup> primitivePartitionGroups) {
        this.primitivePartitionGroups = new ArrayList<>(primitivePartitionGroups);
    }

    public void setPrimitivePartitionGroups(ManagedPartitionGroup... primitivePartitionGroups) {
        if (this.primitivePartitionGroups == null) {
            this.primitivePartitionGroups = new ArrayList<>();
        }

        this.primitivePartitionGroups.clear();

        for (ManagedPartitionGroup group: primitivePartitionGroups) {
            this.primitivePartitionGroups.add(group);
        }
    }

    // *****************************************
    // Copy
    // *****************************************

    public AtomixConfiguration copy() {
        try {
            return (AtomixConfiguration) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
