@serial(etcd-cluster)
Feature: Lease failover
  A lease kept alive through an instance with all three endpoints outlives the
  loss of the leader: the keepalive reconnects by itself, and the new leader
  gives every lease its full TTL again. The cluster is the one of
  `failover.feature`, and the file belongs to the same `@serial` chain.

  Background:
    Given set variable "dir" to "data-<<unique()>>"
    And set variable "n1" to "http://127.0.0.1:25001"
    And set variable "all" to "<<n1>>,http://127.0.0.1:25002,http://127.0.0.1:25003"
    And set variable "cluster" to "--initial-cluster n1=http://127.0.0.1:25011,n2=http://127.0.0.1:25012,n3=http://127.0.0.1:25013 --initial-cluster-token <<unique()>> --heartbeat-interval 100 --election-timeout 1000"
    And I start the "n1" process running "etcd --name n1 --data-dir <<dir>>/n1 --listen-client-urls <<n1>> --advertise-client-urls <<n1>> --listen-peer-urls http://127.0.0.1:25011 --initial-advertise-peer-urls http://127.0.0.1:25011 <<cluster>>"
    And I start the "n2" process running "etcd --name n2 --data-dir <<dir>>/n2 --listen-client-urls http://127.0.0.1:25002 --advertise-client-urls http://127.0.0.1:25002 --listen-peer-urls http://127.0.0.1:25012 --initial-advertise-peer-urls http://127.0.0.1:25012 <<cluster>>"
    And I start the "n3" process running "etcd --name n3 --data-dir <<dir>>/n3 --listen-client-urls http://127.0.0.1:25003 --advertise-client-urls http://127.0.0.1:25003 --listen-peer-urls http://127.0.0.1:25013 --initial-advertise-peer-urls http://127.0.0.1:25013 <<cluster>>"
    And set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/lease-failover/"
    # The cluster is up once it accepts a write, which needs an elected leader.
    And I expect the next assertion to pass within 30 seconds
    And etcd should accept a put of key "<<prefix>>ready" with value "1"
    # move-leader asks every node, so every node must be serving first.
    And I use "n2" etcd
    And I expect the next assertion to pass within 30 seconds
    And etcd should accept a read of key "<<prefix>>ready" serializable
    And I use "n3" etcd
    And I expect the next assertion to pass within 30 seconds
    And etcd should accept a read of key "<<prefix>>ready" serializable
    # Node 1 leads, by request: the status of the node proves it.
    And I run the command "etcdctl --endpoints=<<all>> move-leader $(etcdctl --endpoints=<<n1>> member list | awk -F', ' '$3 == "n1" {print $1}')"
    And the command exit code is 0
    And I use "n1" etcd
    And I expect the next assertion to pass within 30 seconds
    And etcd node should be the leader

  Scenario: A lease kept alive through every endpoint survives the death of the leader
    The lease is asked by its decimal ID through another node, since the name
    belongs to the instance it was granted on. Twice the TTL passes with the
    leader dead, so only the keepalive can have kept the lease. The end of the
    scenario revokes it, best effort, on a cluster that is already stopped.

    Given I use "all" etcd
    And I grant etcd lease "session" with TTL 5 seconds
    And I put etcd key "<<prefix>>owned" with value "mine" under lease "session"
    And I keep etcd lease "session" alive
    And set variable "id" to "<<etcd_lease_session>>"
    When I stop the "n1" process
    And I run the command "sleep 11"
    And I use "n2" etcd
    Then I expect the next assertion to pass within 30 seconds
    And etcd lease with ID <<id>> should be alive
    And etcd key "<<prefix>>owned" should exist
    # Every node preallocates 64 MB of log: do not leave the data directories behind.
    And I stop the "n2" process
    And I stop the "n3" process
    And I run the command "rm -rf <<dir>>"
