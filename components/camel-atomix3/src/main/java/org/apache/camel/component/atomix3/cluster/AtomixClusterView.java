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
import java.util.Optional;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.Leadership;
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

    private LeaderElection<MemberId> election;
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
        this.election = atomix.execute(this::getLeaderElection);
        this.membershipService = atomix.execute(Atomix::getMembershipService);

        LOGGER.debug("Listen for election events");
        election.addListener(this.leadershipListener);

        LOGGER.debug("Listen for membership events");
        membershipService.addListener(this.membershipListener);

        localMember.join();
    }

    @Override
    protected void doStop() throws Exception {
        localMember.leave();

        election.removeListener(this.leadershipListener);
        membershipService.removeListener(this.membershipListener);
    }

    // ***********************************************
    //
    // ***********************************************

    private LeaderElection<MemberId> getLeaderElection(Atomix atomix) throws Exception {
        return atomix.getLeaderElection(getNamespace());
    }

    // ***********************************************
    //
    // ***********************************************

    private final class LeadershipListener implements LeadershipEventListener<MemberId> {
        @Override
        public void event(LeadershipEvent<MemberId> event) {
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
                    fireMemberAddedEvent(new AtomixClusterMember(event.subject().id()));
                }
                if (event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED) {
                    fireMemberRemovedEvent(new AtomixClusterMember(event.subject().id()));
                }
            }
        }
    }

    // ***********************************************
    //
    // ***********************************************

    private class AtomixClusterMember implements CamelClusterMember {
        private final MemberId member;

        AtomixClusterMember(MemberId member) {
            this.member = member;
        }

        @Override
        public String getId() {
            return member.id();
        }

        @Override
        public boolean isLeader() {
            if (member == null) {
                return false;
            }

            return election.getLeadership().leader().id().equals(member);
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }


    private class AtomixLocalMember implements CamelClusterMember {
        private MemberId member;
        private Leadership<MemberId> leadership;

        AtomixLocalMember() {
            this.member = null;
            this.leadership = null;
        }

        @Override
        public String getId() {
            return member.id();
        }

        @Override
        public boolean isLeader() {
            if (this.leadership == null || member == null) {
                return false;
            }

            return leadership.leader().equals(member.id());
        }

        @Override
        public boolean isLocal() {
            return true;
        }

        AtomixLocalMember join() {
            if (this.member == null) {
                this.member = membershipService.getLocalMember().id();
            }

            this.leadership = election.run(this.member);

            return this;
        }

        AtomixLocalMember leave() {
            if (this.member != null) {
                election.evict(this.member);
            }

            this.leadership = null;

            return this;
        }
    }
}
