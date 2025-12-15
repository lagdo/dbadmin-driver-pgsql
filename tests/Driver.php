<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Tests;

use Lagdo\DbAdmin\Driver\Db\AbstractConnection;
use Lagdo\DbAdmin\Driver\Fake\DriverTrait;
use Lagdo\DbAdmin\Driver\Fake\Translator;
use Lagdo\DbAdmin\Driver\Fake\Connection;
use Lagdo\DbAdmin\Driver\PgSql\Driver as PgSqlDriver;
use Lagdo\DbAdmin\Driver\Utils\Str;
use Lagdo\DbAdmin\Driver\Utils\Utils;
use Lagdo\DbAdmin\Driver\Utils\Input;

class Driver extends PgSqlDriver
{
    use DriverTrait;

    /**
     * The constructor
     */
    public function __construct()
    {
        $utils = new Utils(new Translator(), new Input(), new Str());
        parent::__construct($utils, []);
    }

    /**
     * @inheritDoc
     */
    public function createConnection(array $options): AbstractConnection|null
    {
        $this->connection = new Connection($this, $this->utils, $options, 'test');
        $this->testConnection = $this->connection;

        return $this->connection;
    }
}
