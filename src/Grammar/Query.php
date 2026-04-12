<?php

namespace Lagdo\DbAdmin\Support\PgSql\Grammar;

use Lagdo\DbAdmin\Support\Db\Engine\Grammar\AbstractQuery;

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
            " $query" . ($this->_driver()->isView($this->_driver()->tableStatusOrName($table)) ?
                $where : " WHERE ctid = (SELECT ctid FROM " .
                    $this->_grammar()->escapeTableName($table) . $where . ' LIMIT 1)');
    }
}
