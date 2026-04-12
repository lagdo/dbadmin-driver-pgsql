<?php

namespace Lagdo\DbAdmin\Support\PgSql;

use Lagdo\DbAdmin\Support\AbstractDriver;

class Driver extends AbstractDriver
{
    /**
     * @var Grammar|null;
     */
    private Grammar|null $grammar = null;

    /**
     * @var Driver\Server|null
     */
    private Driver\Server|null $server = null;

    /**
     * @var Driver\Database|null
     */
    private Driver\Database|null $database = null;

    /**
     * @var Driver\Table|null
     */
    private Driver\Table|null $table = null;

    /**
     * @var Driver\Query|null
     */
    private Driver\Query|null $query = null;

    /**
     * @return Grammar
     */
    protected function _grammar(): Grammar
    {
        return $this->grammar ??= new Grammar($this, $this->_utils());
    }

    /**
     * @return Driver\Server
     */
    protected function _server(): Driver\Server
    {
        return $this->server ??= new Driver\Server($this, $this->_grammar(), $this->_utils());
    }

    /**
     * @return Driver\Database
     */
    protected function _database(): Driver\Database
    {
        return $this->database ??= new Driver\Database($this, $this->_grammar(), $this->_utils());
    }

    /**
     * @return Driver\Table
     */
    protected function _table(): Driver\Table
    {
        return $this->table ??= new Driver\Table($this, $this->_grammar(), $this->_utils());
    }

    /**
     * @return Driver\Query
     */
    protected function _query(): Driver\Query
    {
        return $this->query ??= new Driver\Query($this, $this->_grammar(), $this->_utils());
    }

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return "PostgreSQL";
    }
}
