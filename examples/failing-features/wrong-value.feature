Feature: A wrong value
  The server answered and the check did not hold: class `assertion`.

  Scenario: The value differs
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/failing/"
    When I put etcd key "<<prefix>>timeout" with value "30s"
    Then etcd key "<<prefix>>timeout" should have value "60s"

  Scenario: Clean up what this run wrote
    When I delete etcd keys with prefix "/bddkit-etcd/examples/<<run_id>>/"
