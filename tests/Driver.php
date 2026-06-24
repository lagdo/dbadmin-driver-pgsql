<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Tests;

use Lagdo\DbAdmin\Driver\Sql\Connection\AbstractConnection;
use Lagdo\DbAdmin\Driver\Tests\Db\Fake\DriverTrait;
use Lagdo\DbAdmin\Driver\Tests\Db\Fake\Translator;
use Lagdo\DbAdmin\Driver\Tests\Db\Fake\Connection;
use Lagdo\DbAdmin\Driver\PgSql\Engine as PgSqlDriver;
use Lagdo\DbAdmin\Driver\Utils\Str;
use Lagdo\DbAdmin\Driver\Utils\Utils;

class Driver extends PgSqlDriver
{
    use DriverTrait;

    /**
     * The constructor
     */
    public function __construct()
    {
        $utils = new Utils(new Translator(), new Str());
        parent::__construct($utils, []);
    }

    /**
     * @inheritDoc
     */
    public function createConnection(array $options): AbstractConnection|null
    {
        $this->connection = new Connection($this, $this->statement(), $this->utils, $options, 'test');
        $this->testConnection = $this->connection;

        return $this->connection;
    }
}
