@serial(etcd-compaction)
Feature: History and compaction
  A key can be read as it was at an earlier revision, until a compaction
  removes that history. A compaction acts on the whole server, so every file
  that compacts, and every file that replays a watch from a revision
  (`watch.feature`), carries the same `@serial` tag and runs one after another.
  The compaction here stops at a revision this file wrote itself, so it takes
  nothing from a file that is not in the chain.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/history/"

  Scenario: A key is read as it was, until the history is compacted
    Given I put etcd key "<<prefix>>cfg" with value "v1"
    And set variable "first" to "<<etcd_revision>>"
    And I put etcd key "<<prefix>>cfg" with value "v2"
    And set variable "second" to "<<etcd_revision>>"
    When I get etcd key "<<prefix>>cfg" at revision <<first>>
    Then variable "etcd_value" should be equal to "v1"
    # The revision of the value is etcd_mod_revision. etcd_revision is the
    # server's revision when it answered, which other files keep moving.
    And variable "etcd_mod_revision" should be equal to "<<first>>"
    # Compacting up to the second write drops the history before it.
    When I compact etcd up to revision <<second>>
    Then etcd should refuse a read of key "<<prefix>>cfg" at revision <<first>> with "OutOfRange"
    And etcd should refuse a compaction up to revision <<first>> with "OutOfRange"
    # The revision compacted to is still readable.
    When I get etcd key "<<prefix>>cfg" at revision <<second>>
    Then variable "etcd_value" should be equal to "v2"
    And I delete etcd keys with prefix "<<prefix>>"
