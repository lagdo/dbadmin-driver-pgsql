<?php

namespace Lagdo\DbAdmin\Support\PgSql\Grammar;

use Lagdo\DbAdmin\Support\Db\Engine\Grammar\AbstractQuery;

use function preg_match;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    protected function limitToOne(string $table, string $query, string $where): string
    {
        return preg_match('~^INTO~', $query) ?
            $this->getLimitClause($query, $where, 1, 0) :
            " $query" . ($this->driver->isView($this->driver->tableStatusOrName($table)) ?
                $where : " WHERE ctid = (SELECT ctid FROM " .
                    $this->grammar->escapeTableName($table) . $where . ' LIMIT 1)');
    }
}
