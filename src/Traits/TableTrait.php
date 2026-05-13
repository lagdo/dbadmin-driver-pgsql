<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Traits;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;

use function preg_match;

trait TableTrait
{
    /**
     * @var string
     */
    protected string $nsOid = "(SELECT oid FROM pg_namespace WHERE nspname = current_schema())";

    /**
     * @param string $table
     *
     * @return string
     */
    protected function tableOid(string $table): string
    {
        return "(SELECT oid FROM pg_class WHERE relnamespace = {$this->nsOid} AND relname = " .
            $this->_engine()->quote($table) . " AND relkind IN ('r', 'm', 'v', 'f', 'p'))";
    }

    /**
     * @param ColumnDto $column
     *
     * @return string
     */
    protected function getSequenceName(ColumnDto $column): string
    {
        return $column->autoIncrement &&
            preg_match("~nextval\('(.+)'\)$~", $column->default, $match) ? $match[1] : '';
    }
}
