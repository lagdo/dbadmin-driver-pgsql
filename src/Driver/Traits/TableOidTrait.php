<?php

namespace Lagdo\DbAdmin\Support\PgSql\Driver\Traits;

trait TableOidTrait
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
            $this->_driver()->quote($table) . " AND relkind IN ('r', 'm', 'v', 'f', 'p'))";
    }
}
