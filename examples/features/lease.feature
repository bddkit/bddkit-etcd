Feature: Lease
  A lease is granted by name, belongs to the instance that was selected, and
  ends with the scenario: the reset revokes it, best effort. Its ID is
  decimal, in the variable `etcd_lease_` plus the name and in the `lease`
  field of a table, while `etcdctl` prints it in hex. A lease is gone when
  the server answers a TTL below zero or `NotFound`: a TTL of 0 is under a
  second left, still alive.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/lease/"

  Scenario: Revoking a lease deletes the keys put under it
    Given I grant etcd lease "session" with TTL 60 seconds
    And I put etcd key "<<prefix>>a" with value "1" under lease "session"
    And I put etcd key "<<prefix>>b" with value "2" under lease "session"
    And etcd lease "session" should hold 2 keys
    And etcd key "<<prefix>>a" should have:
      | field | value                  |
      | lease | <<etcd_lease_session>> |
    When I revoke etcd lease "session"
    Then etcd key "<<prefix>>a" should not exist
    And etcd key "<<prefix>>b" should not exist
    And etcd lease "session" should have expired

  Scenario: A lease expires once nobody keeps it alive
    A kept lease lives as long as the keepalive does. After `I stop keeping …
    alive` it lives its TTL out, and the server may have raised a TTL below
    its minimum, so wait for the expiry with a polled assertion and never for
    a number of seconds.

    Given I grant etcd lease "session" with TTL 3 seconds
    And I put etcd key "<<prefix>>owned" with value "mine" under lease "session"
    And I keep etcd lease "session" alive
    And etcd lease "session" should be alive
    When I stop keeping etcd lease "session" alive
    Then I expect the next assertion to pass within 30 seconds
    And etcd lease "session" should have expired
    And etcd key "<<prefix>>owned" should not exist
