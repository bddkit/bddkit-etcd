Feature: Transaction
  A transaction is written the way `etcdctl txn` reads it: the compares, an
  empty line, the operations when they hold, an empty line, the operations
  when they do not. A compare that does not hold is not a failure of the
  step: `the etcd transaction should have succeeded` / `should have failed`
  say which branch ran.

  Background:
    Given set variable "prefix" to "/bddkit-etcd/examples/<<run_id>>/txn/"

  Scenario: A lock is taken once
    A key that was never created has a create revision of 0, so the first
    commit takes the lock and the second one finds it taken.

    When I commit an etcd transaction:
      """
      create("<<prefix>>lock") = "0"

      put "<<prefix>>lock" "alice"

      get "<<prefix>>lock"
      """
    Then the etcd transaction should have succeeded
    When I commit an etcd transaction:
      """
      create("<<prefix>>lock") = "0"

      put "<<prefix>>lock" "bob"

      get "<<prefix>>lock"
      """
    Then the etcd transaction should have failed
    And etcd key "<<prefix>>lock" should have value "alice"

  Scenario: A value from a variable goes between double quotes
    The doc string is read after the variables are put in, so the value
    becomes text of the transaction. Between quotes it is one token; a value
    with a double quote, a backslash or a line break would break the syntax.

    Given I put etcd key "<<prefix>>counter" with value "1"
    And set variable "seen" to "1"
    When I commit an etcd transaction:
      """
      value("<<prefix>>counter") = "<<seen>>"

      put "<<prefix>>counter" "2"
      """
    Then the etcd transaction should have succeeded
    And etcd key "<<prefix>>counter" should have value "2"

  Scenario: Compares that name a stale value leave the key alone
    The failure branch is optional: a transaction whose compares do not hold
    and that has none applies nothing.

    Given I put etcd key "<<prefix>>stale" with value "new"
    When I commit an etcd transaction:
      """
      value("<<prefix>>stale") = "old"

      put "<<prefix>>stale" "lost update"
      """
    Then the etcd transaction should have failed
    And etcd key "<<prefix>>stale" should have value "new"

  Scenario: A transaction with no compares starts with an empty line
    One line break at the start of a doc string is not part of the
    transaction, so an empty compares section is an empty first line, and the
    operations that follow it are the success branch.

    When I commit an etcd transaction:
      """

      put "<<prefix>>plain" "written"
      """
    Then the etcd transaction should have succeeded
    And etcd key "<<prefix>>plain" should have value "written"
