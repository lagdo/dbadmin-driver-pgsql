<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Tests;

use Lagdo\DbAdmin\Driver\Tests\Connection;
use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\PgSql\Driver as PgSqlDriver;
use Lagdo\DbAdmin\Driver\PgSql\Db\Server;
use Lagdo\DbAdmin\Driver\PgSql\Db\Database;
use Lagdo\DbAdmin\Driver\PgSql\Db\Table;
use Lagdo\DbAdmin\Driver\PgSql\Db\Query;
use Lagdo\DbAdmin\Driver\PgSql\Db\Grammar;

class Driver extends PgSqlDriver
{
    /**
     * @inheritDoc
     */
    public function createConnection()
    {
        $this->connection = new Connection($this, $this->util, $this->trans, 'test');
        $this->server = new Server($this, $this->util, $this->trans, $this->connection);
        $this->database = new Database($this, $this->util, $this->trans, $this->connection);
        $this->table = new Table($this, $this->util, $this->trans, $this->connection);
        $this->query = new Query($this, $this->util, $this->trans, $this->connection);
        $this->grammar = new Grammar($this, $this->util, $this->trans, $this->connection);

        return $this->connection;
    }

    /**
     * @inheritDoc
     */
    public function connect(string $database, string $schema)
    {
        AbstractDriver::connect($database, $schema);
    }
}
