<?php

namespace Lagdo\DbAdmin\Support\PgSql;

use Lagdo\DbAdmin\Support\AbstractGrammar;
use Lagdo\DbAdmin\Support\Utils\Utils;

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
     * @param Driver $driver
     * @param Utils $utils
     */
    public function __construct(protected Driver $driver, protected Utils $utils)
    {}

    /**
     * @return Grammar\Syntax
     */
    protected function _syntax(): Grammar\Syntax
    {
        return $this->syntax ??= new Grammar\Syntax($this->driver, $this, $this->utils);
    }

    /**
     * @return Grammar\Database
     */
    protected function _database(): Grammar\Database
    {
        return $this->database ??= new Grammar\Database($this->driver, $this, $this->utils);
    }

    /**
     * @return Grammar\Table
     */
    protected function _table(): Grammar\Table
    {
        return $this->table ??= new Grammar\Table($this->driver, $this, $this->utils);
    }

    /**
     * @return Grammar\Query
     */
    protected function _query(): Grammar\Query
    {
        return $this->query ??= new Grammar\Query($this->driver, $this, $this->utils);
    }
}
