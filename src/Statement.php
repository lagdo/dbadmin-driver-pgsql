<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\AbstractStatement;

class Statement extends AbstractStatement
{
    /**
     * @var Statement\Syntax|null
     */
    private Statement\Syntax|null $syntax = null;

    /**
     * @var Statement\Database|null
     */
    private Statement\Database|null $database = null;

    /**
     * @var Statement\Table|null
     */
    private Statement\Table|null $table = null;

    /**
     * @var Statement\Query|null
     */
    private Statement\Query|null $query = null;

    /**
     * @return Statement\Syntax
     */
    protected function _syntax(): Statement\Syntax
    {
        return $this->syntax ??= new Statement\Syntax($this->_engine(), $this, $this->_utils());
    }

    /**
     * @return Statement\Database
     */
    protected function _database(): Statement\Database
    {
        return $this->database ??= new Statement\Database($this->_engine(), $this, $this->_utils());
    }

    /**
     * @return Statement\Table
     */
    protected function _table(): Statement\Table
    {
        return $this->table ??= new Statement\Table($this->_engine(), $this, $this->_utils());
    }

    /**
     * @return Statement\Query
     */
    protected function _query(): Statement\Query
    {
        return $this->query ??= new Statement\Query($this->_engine(), $this, $this->_utils());
    }
}
