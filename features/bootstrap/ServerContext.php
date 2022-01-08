<?php

use Lagdo\DbAdmin\Driver\PgSql\Tests\Driver;

use Behat\Behat\Context\Context;
use PHPUnit\Framework\Assert;

class ServerContext implements Context
{
    /**
     * @var Driver
     */
    protected $driver;

    /**
     * @var int
     */
    protected $dbSize;

    /**
     * The constructor
     */
    public function __construct()
    {
        $this->driver = new Driver();
    }

    /**
     * @Given The default server is connected
     */
    public function connectToTheDefaultServer()
    {
        // Nothing to do
    }

    /**
     * @When I read the database list
     */
    public function getTheDatabaseList()
    {
        $this->driver->databases(true);
    }

    /**
     * @Then The read database list query is executed
     */
    public function checkIfTheReadDatabaseListQueryIsExecuted()
    {
        $queries = $this->driver->queries();
        Assert::assertGreaterThan(0, count($queries));
        $query = "SELECT datname FROM pg_database WHERE has_database_privilege(datname, 'CONNECT') ORDER BY datname";
        Assert::assertEquals($queries[0]['query'], $query);
    }

    /**
     * @Given The next request returns :status
     */
    public function setTheNextDatabaseRequestStatus(bool $status)
    {
        $this->driver->connection()->setNextResultStatus($status);
    }

    /**
     * @Given The next request returns database size of :size
     */
    public function setTheNextDatabaseRequestValueOfSize(int $size)
    {
        $this->driver->connection()->setNextResultValues([['size' => $size]]);
    }

    /**
     * @When I read the database :database size
     */
    public function getTheDatabaseSize(string $database)
    {
        $this->dbSize = $this->driver->databaseSize($database);
    }

    /**
     * @Then The size of the database is :size
     */
    public function checkTheDatabaseSize(int $size)
    {
        Assert::assertEquals($size, $this->dbSize);
    }

    /**
     * @Then The get database size query is executed on :database
     */
    public function checkIfTheGetDatabaseSizeQueryIsExecuted(string $database)
    {
        $queries = $this->driver->queries();
        $count = count($queries);
        Assert::assertGreaterThan(0, $count);
        $query = "SELECT pg_database_size($database)";
        Assert::assertEquals($queries[$count - 1]['query'], $query);
    }
}
