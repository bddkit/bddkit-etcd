Feature: Users and permissions
  A server with authentication on, started by the exec plugin and set up with
  `etcdctl`: the user `root` (the configured one) and a user `reader` who may
  read under one prefix and nothing else. `I authenticate` switches the
  selected instance to another user until the scenario ends, and a refused
  login changes nothing.

  Background:
    Given set variable "dir" to "data-<<unique()>>"
    And set variable "endpoint" to "http://127.0.0.1:25021"
    And set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/auth/"
    And set variable "root_password" to "root-example-5c1e"
    And set variable "reader_password" to "reader-example-9d2b"
    And I start the "etcd" process running "etcd --name auth --data-dir <<dir>> --listen-client-urls <<endpoint>> --advertise-client-urls <<endpoint>> --listen-peer-urls http://127.0.0.1:25031 --initial-advertise-peer-urls http://127.0.0.1:25031 --initial-cluster auth=http://127.0.0.1:25031"
    # Users and roles first, authentication last: once it is on, etcdctl needs the root login.
    And I run the command "for i in $(seq 100); do etcdctl --endpoints=<<endpoint>> endpoint health && break; sleep 0.2; done; etcdctl --endpoints=<<endpoint>> endpoint health"
    And the command exit code is 0
    And I run the command "etcdctl --endpoints=<<endpoint>> role add root && etcdctl --endpoints=<<endpoint>> user add root --new-user-password=<<root_password>> && etcdctl --endpoints=<<endpoint>> user grant-role root root"
    And the command exit code is 0
    And I run the command "etcdctl --endpoints=<<endpoint>> role add reader && etcdctl --endpoints=<<endpoint>> role grant-permission reader read <<prefix>> --prefix && etcdctl --endpoints=<<endpoint>> user add reader --new-user-password=<<reader_password>> && etcdctl --endpoints=<<endpoint>> user grant-role reader reader && etcdctl --endpoints=<<endpoint>> auth enable"
    And the command exit code is 0

  Scenario: A read-only user reads what root wrote and is refused a write
    Given I put etcd key "<<prefix>>cfg" with value "written by root"
    When I authenticate to etcd as "reader" with password "<<reader_password>>"
    Then etcd key "<<prefix>>cfg" should have value "written by root"
    And etcd should refuse a put of key "<<prefix>>cfg" with "PermissionDenied"
    # The refused put changed nothing.
    And etcd key "<<prefix>>cfg" should have value "written by root"
    And I stop the "etcd" process
    And I run the command "rm -rf <<dir>>"

  Scenario: A wrong password is refused, and the configured user is back with the next scenario
    A refused login changes nothing: the steps that follow still run as root.
    The code of the refusal is the server's own, `InvalidArgument` on etcd and
    another one on some compatible servers.

    When etcd should refuse authentication as "reader" with password "wrong-example-0000" with "InvalidArgument"
    Then etcd should accept a put of key "<<prefix>>cfg" with value "still root"
    And I authenticate to etcd as "reader" with password "<<reader_password>>"
    And etcd should refuse a put of key "<<prefix>>cfg" with "PermissionDenied"
    And I stop the "etcd" process
    And I run the command "rm -rf <<dir>>"
