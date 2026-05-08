<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractQuery;

use function implode;
use function preg_match;
use function strtoupper;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    public function lastAutoIncrementId(): string
    {
        return '0'; // there can be several sequences
    }

    /**
     * @inheritDoc
     */
    public function convertSearch(string $idf, array $value, ColumnDto $column): string
    {
        return preg_match('~char|text' .
            (!preg_match('~LIKE~', $value["op"]) ?
                '|date|time(stamp)?|boolean|uuid|' . $this->_engine()->numberRegex() : '') .
            '~', $column->type) ? $idf : "CAST($idf AS text)";
    }

    /**
     * @inheritDoc
     */
    public function countRows(TableDto $tableStatus, array $where): int|null
    {
        $query = "EXPLAIN SELECT * FROM " . $this->_statement()->escapeId($tableStatus->name) .
            ($where ? " WHERE " . implode(" AND ", $where) : "");
        if (preg_match("~ rows=([0-9]+)~", $this->_engine()->columnValue($query), $regs))
        {
            return $regs[1];
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function view(string $name): array
    {
        $status = $this->_engine()->tableStatus($name);
        $type = strtoupper($status->engine);
        return [
            'name' => $name,
            'type' => $type,
            'materialized' => ($type != 'VIEW'),
            'select' => trim($this->_engine()->columnValue("SELECT pg_get_viewdef(" .
                $this->_engine()->columnValue("SELECT oid FROM pg_class WHERE relnamespace = " .
                "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND relname = " .
                $this->_engine()->quote($name)) . ")") ?? ''),
        ];
    }

    /**
     * @inheritDoc
     */
    public function slowQuery(string $query, int $timeout): string|null
    {
        // $this->connection->timeout = 1000 * $timeout;
        $this->_engine()->execute("SET statement_timeout = " . (1000 * $timeout));
        return $query;
    }
}
