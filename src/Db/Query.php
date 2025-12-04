<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;
use Lagdo\DbAdmin\Driver\Db\Query as AbstractQuery;

use function strtoupper;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    protected function limitToOne(string $table, string $query, string $where): string
    {
        return (preg_match('~^INTO~', $query) ? $this->driver->getLimitClause($query, $where, 1, 0) :
            " $query" . ($this->driver->isView($this->driver->tableStatusOrName($table)) ? $where :
            " WHERE ctid = (SELECT ctid FROM " . $this->driver->escapeTableName($table) . $where . ' LIMIT 1)'));
    }

    /**
     * @inheritDoc
     */
    public function insertOrUpdate(string $table, array $rows, array $primary): bool
    {
        foreach ($rows as $set) {
            $update = [];
            $where = [];
            foreach ($set as $key => $val) {
                $update[] = "$key = $val";
                if (isset($primary[$this->driver->unescapeId($key)])) {
                    $where[] = "$key = $val";
                }
            }
            if (!(
                ($where && $this->driver->execute("UPDATE " . $this->driver->escapeTableName($table) .
                " SET " . implode(", ", $update) . " WHERE " . implode(" AND ", $where)) &&
                $this->driver->affectedRows()) ||
                $this->driver->execute("INSERT INTO " . $this->driver->escapeTableName($table) .
                " (" . implode(", ", array_keys($set)) . ") VALUES (" . implode(", ", $set) . ")")
            )) {
                return false;
            }
        }
        return true;
    }

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
    public function convertSearch(string $idf, array $value, TableFieldEntity $field): string
    {
        return (preg_match('~char|text' . (!preg_match('~LIKE~', $value["op"]) ?
            '|date|time(stamp)?|boolean|uuid|' . $this->driver->numberRegex() : '') .
            '~', $field->type) ? $idf : "CAST($idf AS text)"
        );
    }

    /**
     * @inheritDoc
     */
    public function countRows(TableEntity $tableStatus, array $where): int|null
    {
        $query = "EXPLAIN SELECT * FROM " . $this->driver->escapeId($tableStatus->name) .
            ($where ? " WHERE " . implode(" AND ", $where) : "");
        if (preg_match("~ rows=([0-9]+)~", $this->driver->result($query), $regs))
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
        $status = $this->driver->tableStatus($name);
        $type = strtoupper($status->engine);
        return [
            'name' => $name,
            'type' => $type,
            'materialized' => ($type != 'VIEW'),
            'select' => trim($this->driver->result("SELECT pg_get_viewdef(" .
                $this->driver->result("SELECT oid FROM pg_class WHERE relnamespace = " .
                "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND relname = " .
                $this->driver->quote($name)) . ")") ?? ''),
        ];
    }

    /**
     * @inheritDoc
     */
    public function slowQuery(string $query, int $timeout): string|null
    {
        // $this->connection->timeout = 1000 * $timeout;
        $this->driver->execute("SET statement_timeout = " . (1000 * $timeout));
        return $query;
    }
}
