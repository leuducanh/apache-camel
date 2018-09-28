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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.core.Atomix;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import org.apache.camel.cluster.CamelClusterMember;
import org.apache.camel.cluster.CamelClusterService;
import org.apache.camel.component.atomix3.AtomixConfiguration;
import org.apache.camel.component.atomix3.AtomixInstance;
import org.apache.camel.impl.cluster.AbstractCamelClusterView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AtomixClusterView extends AbstractCamelClusterView {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomixClusterView.class);

    private final AtomixInstance atomix;
    private final AtomixLocalMember localMember;
    private final AtomixConfiguration configuration;
    private final LeadershipListener leadershipListener;
    private final MembershipListener membershipListener;

    private LeaderElection<String> election;
    private ClusterMembershipService membershipService;

    AtomixClusterView(CamelClusterService cluster, String namespace, AtomixInstance atomix, AtomixConfiguration configuration) {
        super(cluster, namespace);

        this.atomix = atomix;
        this.configuration = configuration;
        this.localMember = new AtomixLocalMember();

        this.membershipListener = new MembershipListener();
        this.leadershipListener = new LeadershipListener();
    }

    @Override
    public Optional<CamelClusterMember> getLeader() {
        return Optional.of(new AtomixClusterMember(null));
    }

    @Override
    public CamelClusterMember getLocalMember() {
        return localMember;
    }

    @Override
    public List<CamelClusterMember> getMembers() {
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doStart() throws Exception {
        election = atomix.execute(this::getLeaderElection);
        membershipService = atomix.execute(Atomix::getMembershipService);

        LOGGER.debug("Listen for election events");
        election.addListener(this.leadershipListener);

        LOGGER.debug("Listen for membership events");
        membershipService.addListener(this.membershipListener);

        election.run(localMember.getId());
    }

    @Override
    protected void doStop() throws Exception {
        if (localMember.isLeader()) {
            election.withdraw(localMember.getId());
        }

        election.removeListener(this.leadershipListener);
        election.close();

        membershipService.removeListener(this.membershipListener);
    }

    // ***********************************************
    //
    // ***********************************************

    private LeaderElection<String> getLeaderElection(Atomix atomix) {
        return atomix.getLeaderElection(getNamespace());
    }

    // ***********************************************
    //
    // ***********************************************

    private final class LeadershipListener implements LeadershipEventListener<String> {
        @Override
        public void event(LeadershipEvent<String> event) {
            if (isRunAllowed()) {
                fireLeadershipChangedEvent(Optional.of(new AtomixClusterMember(event.newLeadership().leader().id())));
            }
        }
    }

    private final class MembershipListener implements ClusterMembershipEventListener {
        @Override
        public void event(ClusterMembershipEvent event) {
            if (isRunAllowed()) {
                if (event.type() == ClusterMembershipEvent.Type.MEMBER_ADDED) {
                    fireMemberAddedEvent(new AtomixClusterMember(event.subject().id().id()));
                }
                if (event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED) {
                    fireMemberRemovedEvent(new AtomixClusterMember(event.subject().id().id()));
                }
            }
        }
    }

    // ***********************************************
    //
    // ***********************************************

    private final class AtomixClusterMember implements CamelClusterMember {
        private final String memberId;

        AtomixClusterMember(String member) {
            this.memberId = member;
        }

        @Override
        public String getId() {
            return memberId;
        }

        @Override
        public boolean isLeader() {
            if (memberId == null) {
                return false;
            }

            Leader<String> leader = election.getLeadership().leader();
            if (leader == null) {
                return false;
            }

            return leader.id().equals(memberId);
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }

    private final class AtomixLocalMember implements CamelClusterMember {
        @Override
        public String getId() {
            return membershipService.getLocalMember().id().id();
        }

        @Override
        public boolean isLeader() {
            if (!isRunAllowed()) {
                return false;
            }

            Leader<String> leader = election.getLeadership().leader();
            if (leader == null) {
                return false;
            }

            final String leaderId = leader.id();
            final String localId = membershipService.getLocalMember().id().id();

            return Objects.nonNull(leader)
                && Objects.nonNull(localId)
                && Objects.equals(leaderId, localId);
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }
}
