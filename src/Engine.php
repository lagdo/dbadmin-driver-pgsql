<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\AbstractEngine;

class Engine extends AbstractEngine
{
    /**
     * @var Engine\Server|null
     */
    private Engine\Server|null $server = null;

    /**
     * @var Engine\Database|null
     */
    private Engine\Database|null $database = null;

    /**
     * @var Engine\Table|null
     */
    private Engine\Table|null $table = null;

    /**
     * @var Engine\Query|null
     */
    private Engine\Query|null $query = null;

    /**
     * @return Engine\Server
     */
    protected function _server(): Engine\Server
    {
        return $this->server ??= new Engine\Server($this, $this->_statement(), $this->_utils());
    }

    /**
     * @return Engine\Database
     */
    protected function _database(): Engine\Database
    {
        return $this->database ??= new Engine\Database($this, $this->_statement(), $this->_utils());
    }

    /**
     * @return Engine\Table
     */
    protected function _table(): Engine\Table
    {
        return $this->table ??= new Engine\Table($this, $this->_statement(), $this->_utils());
    }

    /**
     * @return Engine\Query
     */
    protected function _query(): Engine\Query
    {
        return $this->query ??= new Engine\Query($this, $this->_statement(), $this->_utils());
    }

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return "PostgreSQL";
    }
}
