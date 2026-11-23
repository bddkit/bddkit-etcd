Feature: A server that is not there
  The server was not reached: class `db_connection`.

  Scenario: Use the instance that is down
    Given I use "down" etcd
    When I get etcd key "/anything"
