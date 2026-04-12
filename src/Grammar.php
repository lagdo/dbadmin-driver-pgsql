<?php

namespace Lagdo\DbAdmin\Support\PgSql;

use Lagdo\DbAdmin\Support\AbstractGrammar;

class Grammar extends AbstractGrammar
{
    /**
     * @var Grammar\Syntax|null
     */
    private Grammar\Syntax|null $syntax = null;

    /**
     * @var Grammar\Database|null
     */
    private Grammar\Database|null $database = null;

    /**
     * @var Grammar\Table|null
     */
    private Grammar\Table|null $table = null;

    /**
     * @var Grammar\Query|null
     */
    private Grammar\Query|null $query = null;

    /**
     * @return Grammar\Syntax
     */
    protected function _syntax(): Grammar\Syntax
    {
        return $this->syntax ??= new Grammar\Syntax($this->_driver(), $this, $this->_utils());
    }

    /**
     * @return Grammar\Database
     */
    protected function _database(): Grammar\Database
    {
        return $this->database ??= new Grammar\Database($this->_driver(), $this, $this->_utils());
    }

    /**
     * @return Grammar\Table
     */
    protected function _table(): Grammar\Table
    {
        return $this->table ??= new Grammar\Table($this->_driver(), $this, $this->_utils());
    }

    /**
     * @return Grammar\Query
     */
    protected function _query(): Grammar\Query
    {
        return $this->query ??= new Grammar\Query($this->_driver(), $this, $this->_utils());
    }
}
