Feature: A missing key
  `I get` of a key that does not exist fails the step.

  Scenario: Read a key nobody wrote
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/failing/"
    When I get etcd key "<<prefix>>never-written"
