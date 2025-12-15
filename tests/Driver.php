<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Tests;

use Lagdo\DbAdmin\Driver\Db\Connection;
use Lagdo\DbAdmin\Driver\Fake\DriverTrait;
use Lagdo\DbAdmin\Driver\Fake\Translator;
use Lagdo\DbAdmin\Driver\Fake\Connection as FakeConnection;
use Lagdo\DbAdmin\Driver\PgSql\Db\Server;
use Lagdo\DbAdmin\Driver\PgSql\Db\Database;
use Lagdo\DbAdmin\Driver\PgSql\Db\Table;
use Lagdo\DbAdmin\Driver\PgSql\Db\Query;
use Lagdo\DbAdmin\Driver\PgSql\Db\Grammar;
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
    public function createConnection(array $options): Connection|null
    {
        $this->testConnection = new FakeConnection($this, $this->utils, $options, 'test');
        $this->connection = $this->testConnection;
        $this->server = new Server($this, $this->utils);
        $this->database = new Database($this, $this->utils);
        $this->table = new Table($this, $this->utils);
        $this->query = new Query($this, $this->utils);
        $this->grammar = new Grammar($this, $this->utils);

        return $this->connection;
    }
}
