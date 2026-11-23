Feature: Two servers
  `I use "<name>" etcd` switches the instance for the rest of the scenario. The
  next scenario starts on the default again, which `--default etcd=<name>` picks.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/instances/"

  Scenario: A key written to one server is not on the other
    Given I use "main" etcd
    When I put etcd key "<<prefix>>only-here" with value "main"
    And I use "second" etcd
    Then etcd key "<<prefix>>only-here" should not exist
    When I put etcd key "<<prefix>>only-here" with value "second"
    Then etcd key "<<prefix>>only-here" should have value "second"
    When I use "main" etcd
    Then etcd key "<<prefix>>only-here" should have value "main"

  Scenario: Clean up on both servers
    When I use "main" etcd
    And I delete etcd keys with prefix "<<prefix>>"
    And I use "second" etcd
    And I delete etcd keys with prefix "<<prefix>>"
    Then there should be 0 etcd keys with prefix "<<prefix>>"
