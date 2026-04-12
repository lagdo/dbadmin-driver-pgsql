<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractQuery;

use function preg_match;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    public function limitToOne(string $table, string $query, string $where): string
    {
        return preg_match('~^INTO~', $query) ?
            $this->getLimitClause($query, $where, 1, 0) :
            " $query" . ($this->_engine()->isView($this->_engine()->tableStatusOrName($table)) ?
                $where : " WHERE ctid = (SELECT ctid FROM " .
                    $this->_statement()->escapeTableName($table) . $where . ' LIMIT 1)');
    }
}
