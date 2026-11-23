Feature: Keys
  The key space of one run lives under a prefix built from <<run_id>>, so files
  running at the same time never see each other's keys, and the last scenario
  removes exactly what this run wrote.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/keys/"

  Scenario: Write a key and read it back
    When I put etcd key "<<prefix>>timeout" with value "30s"
    And I get etcd key "<<prefix>>timeout"
    Then variable "etcd_value" should be equal to "30s"
    And variable "etcd_version" should be equal to "1"
    And variable "etcd_lease" should be equal to "0"

  Scenario: A write sets the revision of the whole server
    The revision belongs to the server, so other files move it too: compare with
    the revision of your own write, never with "it grew by one".

    When I put etcd key "<<prefix>>counter" with value "1"
    And set variable "first" to "<<etcd_revision>>"
    And I put etcd key "<<prefix>>counter" with value "2"
    Then etcd key "<<prefix>>counter" should have:
      | field           | value            |
      | version         | 2                |
      | create_revision | <<first>>        |
      | mod_revision    | <<etcd_revision>> |

  Scenario: A value of several lines is a doc string
    When I put etcd key "<<prefix>>config" with value:
      """
      {
        "retries": 3,
        "region": "eu"
      }
      """
    And I get etcd key "<<prefix>>config"
    Then variable "etcd_value" should contain JSON:
      """
      { "retries": 3 }
      """

  Scenario: Delete one key, then a whole prefix
    Given I put etcd key "<<prefix>>tmp/a" with value "1"
    And I put etcd key "<<prefix>>tmp/b" with value "2"
    And I put etcd key "<<prefix>>tmp/c" with value "3"
    When I delete etcd key "<<prefix>>tmp/a"
    Then variable "etcd_deleted" should be equal to "1"
    When I delete etcd keys with prefix "<<prefix>>tmp/"
    Then variable "etcd_deleted" should be equal to "2"
    And there should be 0 etcd keys with prefix "<<prefix>>tmp/"

  Scenario: Clean up what this run wrote
    When I delete etcd keys with prefix "<<prefix>>"
    Then there should be 0 etcd keys with prefix "<<prefix>>"
