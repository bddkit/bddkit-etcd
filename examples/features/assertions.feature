Feature: Assertions over keys
  Every assertion reads the server again on each attempt, so it can be polled.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/assertions/"

  Scenario: A key and its value
    When I put etcd key "<<prefix>>user" with value:
      """
      { "name": "Ada", "roles": ["admin", "dev"], "active": true }
      """
    Then etcd key "<<prefix>>user" should exist
    And etcd key "<<prefix>>missing" should not exist
    And etcd key "<<prefix>>user" value should match "name.: .Ada"
    And etcd key "<<prefix>>user" value should contain JSON:
      """
      { "name": "Ada", "roles": ["dev"] }
      """
    And etcd key "<<prefix>>user" value should equal JSON:
      """
      { "name": "Ada", "roles": ["admin", "dev"], "active": "@variableType(bool)" }
      """

  Scenario: A text value, exactly
    When I put etcd key "<<prefix>>mode" with value "strict"
    Then etcd key "<<prefix>>mode" should have value "strict"
    When I put etcd key "<<prefix>>notes" with value:
      """
      line 1
      line 2
      """
    Then etcd key "<<prefix>>notes" should have value:
      """
      line 1
      line 2
      """

  Scenario: Several keys under a prefix
    Given I put etcd key "<<prefix>>list/b" with value "2"
    And I put etcd key "<<prefix>>list/a" with value "1"
    Then there should be 2 etcd keys with prefix "<<prefix>>list/"
    And etcd keys with prefix "<<prefix>>list/" should be:
      | key               | value |
      | <<prefix>>list/a  | 1     |
      | <<prefix>>list/b  | 2     |
    And etcd keys with prefix "<<prefix>>list/" should contain:
      | key               | value |
      | <<prefix>>list/b  | 2     |

  Scenario: Clean up what this run wrote
    When I delete etcd keys with prefix "<<prefix>>"
    Then there should be 0 etcd keys with prefix "<<prefix>>"
