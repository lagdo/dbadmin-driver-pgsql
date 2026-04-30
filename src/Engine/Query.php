<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractQuery;

use function array_keys;
use function implode;
use function preg_match;
use function strtoupper;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    // public function insertOrUpdate(string $table, array $rows, array $primary): bool
    // {
    //     $tableName = $this->_statement()->escapeTableName($table);
    //     foreach ($rows as $set) {
    //         $update = [];
    //         $where = [];
    //         foreach ($set as $key => $val) {
    //             $update[] = "$key = $val";
    //             if (isset($primary[$this->_statement()->unescapeId($key)])) {
    //                 $where[] = "$key = $val";
    //             }
    //         }
    //         $updateColumns = implode(", ", $update);
    //         $updateFilters = implode(" AND ", $where);
    //         $insertColumns = implode(", ", array_keys($set));
    //         $insertValues = implode(", ", $set);
    //         if (!(
    //             ($where && $this->_engine()->execute("UPDATE $tableName SET $updateColumns WHERE $updateFilters") &&
    //             $this->_engine()->affectedRows()) ||
    //             $this->_engine()->execute("INSERT INTO $tableName ($insertColumns) VALUES ($insertValues)")
    //         )) {
    //             return false;
    //         }
    //     }
    //     return true;
    // }

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
        if (preg_match("~ rows=([0-9]+)~", $this->_engine()->result($query), $regs))
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
            'select' => trim($this->_engine()->result("SELECT pg_get_viewdef(" .
                $this->_engine()->result("SELECT oid FROM pg_class WHERE relnamespace = " .
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
