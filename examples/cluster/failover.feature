@serial(etcd-cluster)
Feature: Failover
  Three etcd nodes that the exec plugin starts, and then breaks. The leader is
  chosen with `etcdctl move-leader`, never by waiting for an election to
  settle, and every wait for the cluster is a polled assertion. The files of a
  cluster belong in one `@serial` chain: they share its ports.

  Background:
    Given set variable "dir" to "data-<<unique()>>"
    And set variable "n1" to "http://127.0.0.1:25001"
    And set variable "all" to "<<n1>>,http://127.0.0.1:25002,http://127.0.0.1:25003"
    And set variable "cluster" to "--initial-cluster n1=http://127.0.0.1:25011,n2=http://127.0.0.1:25012,n3=http://127.0.0.1:25013 --initial-cluster-token <<unique()>> --heartbeat-interval 100 --election-timeout 1000"
    And I start the "n1" process running "etcd --name n1 --data-dir <<dir>>/n1 --listen-client-urls <<n1>> --advertise-client-urls <<n1>> --listen-peer-urls http://127.0.0.1:25011 --initial-advertise-peer-urls http://127.0.0.1:25011 <<cluster>>"
    And I start the "n2" process running "etcd --name n2 --data-dir <<dir>>/n2 --listen-client-urls http://127.0.0.1:25002 --advertise-client-urls http://127.0.0.1:25002 --listen-peer-urls http://127.0.0.1:25012 --initial-advertise-peer-urls http://127.0.0.1:25012 <<cluster>>"
    And I start the "n3" process running "etcd --name n3 --data-dir <<dir>>/n3 --listen-client-urls http://127.0.0.1:25003 --advertise-client-urls http://127.0.0.1:25003 --listen-peer-urls http://127.0.0.1:25013 --initial-advertise-peer-urls http://127.0.0.1:25013 <<cluster>>"
    And set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/failover/"
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

  Scenario: Another node leads once the leader is killed
    Given I put etcd key "<<prefix>>kept" with value "written under node 1"
    When I stop the "n1" process
    # Writes work again once the survivors have elected a leader.
    And I use "n2" etcd
    And I expect the next assertion to pass within 30 seconds
    And etcd should accept a put of key "<<prefix>>after" with value "written by the survivors"
    # Which of node 2 and node 3 won is theirs to decide, so ask each for its own status.
    And I run the command "for n in 2 3; do etcdctl --endpoints=http://127.0.0.1:2500$n endpoint status --write-out=json | grep -oE '"(member_id|leader)":[0-9]+' | cut -d: -f2 | uniq -d | grep -q . && echo n$n; done"
    And extract "(?m)^(n[23])$" from the command output as "leader"
    And I use "<<leader>>" etcd
    Then I expect the next assertion to pass within 30 seconds
    And etcd node should be the leader
    And etcd key "<<prefix>>kept" should have value "written under node 1"
    And etcd key "<<prefix>>after" should have value "written by the survivors"
    # Every node preallocates 64 MB of log: do not leave the data directories behind.
    And I stop the "n2" process
    And I stop the "n3" process
    And I run the command "rm -rf <<dir>>"

  Scenario: Without a quorum writes are refused and serializable reads still answer
    Given I put etcd key "<<prefix>>kept" with value "written with all three nodes up"
    When I stop the "n2" process
    And I stop the "n3" process
    # The survivor refuses with the server's own `Unavailable`; until it has
    # noticed the lost quorum a request just waits, which is not a refusal and
    # is asked again.
    Then I expect the next assertion to pass within 30 seconds
    And etcd should refuse a put of key "<<prefix>>refused" with "Unavailable"
    And etcd should accept a read of key "<<prefix>>kept" serializable
    And I get etcd key "<<prefix>>kept" serializable
    And variable "etcd_value" should be equal to "written with all three nodes up"
    And I stop the "n1" process
    And I run the command "rm -rf <<dir>>"
