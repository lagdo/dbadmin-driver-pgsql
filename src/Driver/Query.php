<?php

namespace Lagdo\DbAdmin\Support\PgSql\Driver;

use Lagdo\DbAdmin\Support\Db\Engine\Driver\AbstractQuery;
use Lagdo\DbAdmin\Support\Dto\TableFieldDto;
use Lagdo\DbAdmin\Support\Dto\TableDto;

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
    //     $tableName = $this->_grammar()->escapeTableName($table);
    //     foreach ($rows as $set) {
    //         $update = [];
    //         $where = [];
    //         foreach ($set as $key => $val) {
    //             $update[] = "$key = $val";
    //             if (isset($primary[$this->_grammar()->unescapeId($key)])) {
    //                 $where[] = "$key = $val";
    //             }
    //         }
    //         $updateFields = implode(", ", $update);
    //         $updateFilters = implode(" AND ", $where);
    //         $insertFields = implode(", ", array_keys($set));
    //         $insertValues = implode(", ", $set);
    //         if (!(
    //             ($where && $this->_driver()->execute("UPDATE $tableName SET $updateFields WHERE $updateFilters") &&
    //             $this->_driver()->affectedRows()) ||
    //             $this->_driver()->execute("INSERT INTO $tableName ($insertFields) VALUES ($insertValues)")
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
    public function convertSearch(string $idf, array $value, TableFieldDto $field): string
    {
        return preg_match('~char|text' .
            (!preg_match('~LIKE~', $value["op"]) ?
                '|date|time(stamp)?|boolean|uuid|' . $this->_driver()->numberRegex() : '') .
            '~', $field->type) ? $idf : "CAST($idf AS text)";
    }

    /**
     * @inheritDoc
     */
    public function countRows(TableDto $tableStatus, array $where): int|null
    {
        $query = "EXPLAIN SELECT * FROM " . $this->_grammar()->escapeId($tableStatus->name) .
            ($where ? " WHERE " . implode(" AND ", $where) : "");
        if (preg_match("~ rows=([0-9]+)~", $this->_driver()->result($query), $regs))
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
        $status = $this->_driver()->tableStatus($name);
        $type = strtoupper($status->engine);
        return [
            'name' => $name,
            'type' => $type,
            'materialized' => ($type != 'VIEW'),
            'select' => trim($this->_driver()->result("SELECT pg_get_viewdef(" .
                $this->_driver()->result("SELECT oid FROM pg_class WHERE relnamespace = " .
                "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND relname = " .
                $this->_driver()->quote($name)) . ")") ?? ''),
        ];
    }

    /**
     * @inheritDoc
     */
    public function slowQuery(string $query, int $timeout): string|null
    {
        // $this->connection->timeout = 1000 * $timeout;
        $this->_driver()->execute("SET statement_timeout = " . (1000 * $timeout));
        return $query;
    }
}
