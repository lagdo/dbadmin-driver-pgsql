Feature: Database server
  As a database user, I want to query a database server

  Scenario: Read the database names
    Given The default server is connected
    When I read the database list
    Then The read database list query is executed

  Scenario: Read the size of an unknown database
    Given The default server is connected
    And The next request returns false
    When I read the database unknown size
    Then The get database size query is executed on unknown
    Then The size of the database is 0

  Scenario: Read the size of an existing database
    Given The default server is connected
    And The next request returns database size of 1000
    When I read the database test size
    Then The get database size query is executed on test
    Then The size of the database is 1000
