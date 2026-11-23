Feature: A password stays out of every output
  The step below writes the password as a value on purpose, to show that the
  console, the event stream and the evidence all print it masked.

  Scenario: Use the instance with a password the server refuses
    Given I use "guarded" etcd
    When I put etcd key "/anything" with value "example-password-7f3a"
