<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Tests;

use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Input;
use Lagdo\DbAdmin\Driver\Fake\DriverTrait;
use Lagdo\DbAdmin\Driver\Fake\Translator;
use Lagdo\DbAdmin\Driver\Fake\Admin;
use Lagdo\DbAdmin\Driver\Fake\Connection;

use Lagdo\DbAdmin\Driver\PgSql\Driver as PgSqlDriver;
use Lagdo\DbAdmin\Driver\PgSql\Db\Server;
use Lagdo\DbAdmin\Driver\PgSql\Db\Database;
use Lagdo\DbAdmin\Driver\PgSql\Db\Table;
use Lagdo\DbAdmin\Driver\PgSql\Db\Query;
use Lagdo\DbAdmin\Driver\PgSql\Db\Grammar;

class Driver extends PgSqlDriver
{
    use DriverTrait;

    /**
     * The constructor
     */
    public function __construct()
    {
        $input = new Input();
        $trans = new Translator();
        $admin = new Admin($trans, $input);
        parent::__construct($admin, $trans, []);
    }

    /**
     * @inheritDoc
     */
    protected function createConnection()
    {
        $this->testConnection = new Connection($this, $this->admin, $this->trans, 'test');
        $this->connection = $this->testConnection;
        $this->server = new Server($this, $this->admin, $this->trans);
        $this->database = new Database($this, $this->admin, $this->trans);
        $this->table = new Table($this, $this->admin, $this->trans);
        $this->query = new Query($this, $this->admin, $this->trans);
        $this->grammar = new Grammar($this, $this->admin, $this->trans);

        return $this->connection;
    }

    /**
     * @inheritDoc
     */
    public function connect(string $database, string $schema = '')
    {
        AbstractDriver::connect($database, $schema);
    }
}
