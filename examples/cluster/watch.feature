@serial(etcd-cluster)
Feature: Watch through a failover
  A watch on a node that is killed loses no event. The watch is on node 1
  alone (one endpoint), and node 1 is the node that dies: a watch on a
  follower that stays in the quorum is not broken by a change of leader. The
  plugin opens the stream again by itself from the revision after its last
  event, and `should have been resumed` proves that the stream did break.
  This file shares its ports with failover.feature, so it carries the same
  `@serial` tag. Node 1 leads when it is killed, which is the harshest case.

  Background:
    Given set variable "dir" to "data-<<unique()>>"
    And set variable "n1" to "http://127.0.0.1:25001"
    And set variable "all" to "<<n1>>,http://127.0.0.1:25002,http://127.0.0.1:25003"
    And set variable "cluster" to "--initial-cluster n1=http://127.0.0.1:25011,n2=http://127.0.0.1:25012,n3=http://127.0.0.1:25013 --initial-cluster-token <<unique()>> --heartbeat-interval 100 --election-timeout 1000"
    And set variable "n1cmd" to "etcd --name n1 --data-dir <<dir>>/n1 --listen-client-urls <<n1>> --advertise-client-urls <<n1>> --listen-peer-urls http://127.0.0.1:25011 --initial-advertise-peer-urls http://127.0.0.1:25011 <<cluster>>"
    And I start the "n1" process running "<<n1cmd>>"
    And I start the "n2" process running "etcd --name n2 --data-dir <<dir>>/n2 --listen-client-urls http://127.0.0.1:25002 --advertise-client-urls http://127.0.0.1:25002 --listen-peer-urls http://127.0.0.1:25012 --initial-advertise-peer-urls http://127.0.0.1:25012 <<cluster>>"
    And I start the "n3" process running "etcd --name n3 --data-dir <<dir>>/n3 --listen-client-urls http://127.0.0.1:25003 --advertise-client-urls http://127.0.0.1:25003 --listen-peer-urls http://127.0.0.1:25013 --initial-advertise-peer-urls http://127.0.0.1:25013 <<cluster>>"
    And set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/watch/"
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

  Scenario: Every write is received although the watched node was killed and restarted
    Given I use "n1" etcd
    And I watch etcd keys with prefix "<<prefix>>" as "w"
    And I put etcd key "<<prefix>>before" with value "written under the watch"
    And I expect the next assertion to pass within 10 seconds
    And etcd watch "w" should have received a PUT of "<<prefix>>before" with value "written under the watch"
    When I stop the "n1" process
    # The survivors write while the watch has no stream. A put under polling may
    # be written more than once, so the key is asserted with `a PUT of` and
    # not with an exact list.
    And I use "n2" etcd
    And I expect the next assertion to pass within 30 seconds
    And etcd should accept a put of key "<<prefix>>during" with value "written while the watched node was down"
    # A process name is used once per scenario, so the same node comes back as "n1-again".
    And I start the "n1-again" process running "<<n1cmd>>"
    And I use "n1" etcd
    Then I expect the next assertion to pass within 60 seconds
    And etcd watch "w" should have been resumed 1 times
    And I expect the next assertion to pass within 30 seconds
    And etcd watch "w" should have received a PUT of "<<prefix>>during" with value "written while the watched node was down"
    # Every node preallocates 64 MB of log: do not leave the data directories behind.
    And I stop the "n1-again" process
    And I stop the "n2" process
    And I stop the "n3" process
    And I run the command "rm -rf <<dir>>"
