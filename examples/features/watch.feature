@serial(etcd-compaction)
Feature: Watch
  A watch is opened by name, keeps every event it receives until the scenario
  ends, and is asserted on while the writes happen. The assertions never
  consume events and never ask the server, so isolation comes from a key
  prefix that carries <<run_id>>.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/watch/"

  Scenario: The whole history of a prefix, in order, with the previous values
    Given I watch etcd keys with prefix "<<prefix>>" as "cfg"
    When I put etcd key "<<prefix>>a" with value "1"
    And set variable "first" to "<<etcd_revision>>"
    And I put etcd key "<<prefix>>a" with value "2"
    And set variable "second" to "<<etcd_revision>>"
    And I delete etcd key "<<prefix>>a"
    Then I expect the next assertion to pass within 10 seconds
    And etcd watch "cfg" should have received events:
      | type   | key         | value | prev_value | mod_revision      |
      | PUT    | <<prefix>>a | 1     |            | <<first>>         |
      | PUT    | <<prefix>>a | 2     | 1          | <<second>>        |
      | DELETE | <<prefix>>a |       | 2          | <<etcd_revision>> |

  Scenario: No event is proved by a marker written after the quiet period
    "Nothing arrived" is a race on a live stream, so write a marker key into
    the watched prefix and assert the exact list with the marker last: events
    come in revision order, so a write that matched the watch and was made
    before the marker is in the history when the marker is. The write outside
    the prefix does not match, and the table proves its absence.

    Given I watch etcd keys with prefix "<<prefix>>quiet/" as "quiet"
    When I put etcd key "<<prefix>>elsewhere" with value "not watched"
    And I put etcd key "<<prefix>>quiet/marker" with value "last"
    Then I expect the next assertion to pass within 10 seconds
    And etcd watch "quiet" should have received events:
      | type | key                    | value |
      | PUT  | <<prefix>>quiet/marker | last  |

  Scenario: A watch from a revision replays the writes it missed, and a cancelled one keeps its history
    Given I put etcd key "<<prefix>>late" with value "before the watch"
    And set variable "missed" to "<<etcd_revision>>"
    And I watch etcd key "<<prefix>>late" as "replay" from revision <<missed>>
    When I put etcd key "<<prefix>>late" with value "after the watch"
    And I delete etcd key "<<prefix>>late"
    Then I expect the next assertion to pass within 10 seconds
    And etcd watch "replay" should have received 3 events
    And etcd watch "replay" should have received a PUT of "<<prefix>>late" with value "before the watch"
    And etcd watch "replay" should have received a DELETE of "<<prefix>>late"
    When I cancel etcd watch "replay"
    Then etcd watch "replay" should have received 3 events

  Scenario: Clean up what this run wrote
    When I delete etcd keys with prefix "<<prefix>>"
    Then there should be 0 etcd keys with prefix "<<prefix>>"
