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
import java.util.Arrays;
import java.util.List;

import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.profile.Profile;
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

    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Sets the bootstrap nodes in the form id@host:port.
     */
    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    /**
     * Sets the bootstrap nodes in the form id@host:port.
     */
    public void setNodes(String... nodes) {
        this.nodes = Arrays.asList(nodes);
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
        this.profiles = profiles;
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
