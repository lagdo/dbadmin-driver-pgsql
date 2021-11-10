<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\Database as AbstractDatabase;

trait DatabaseTrait
{
    /**
     * Get queries to create or alter table.
     *
     * @param TableEntity $tableAttrs
     *
     * @return array
     */
    private function getQueries(TableEntity $tableAttrs)
    {
        $queries = [];

        foreach ($tableAttrs->edited as $field) {
            $column = $this->driver->escapeId($field[0]);
            $val = $field[1];
            $val5 = $val[5] ?? '';
            if ($val[0] !== '' && $column != $val[0]) {
                $queries[] = 'ALTER TABLE ' . $this->driver->table($tableAttrs->name) . " RENAME $column TO $val[0]";
            }
            if ($column !== '' || $val5 !== '') {
                $queries[] = 'COMMENT ON COLUMN ' . $this->driver->table($tableAttrs->name) .
                    ".$val[0] IS " . ($val5 != '' ? substr($val5, 9) : "''");
            }
        }
        foreach ($tableAttrs->fields as $field) {
            $column = $this->driver->escapeId($field[0]);
            $val = $field[1];
            $val5 = $val[5] ?? '';
            if ($column !== '' || $val5 !== '') {
                $queries[] = 'COMMENT ON COLUMN ' . $this->driver->table($tableAttrs->name) .
                    ".$val[0] IS " . ($val5 != '' ? substr($val5, 9) : "''");
            }
        }
        if ($tableAttrs->comment != '') {
            $queries[] = 'COMMENT ON TABLE ' . $this->driver->table($tableAttrs->name) .
                ' IS ' . $this->driver->quote($tableAttrs->comment);
        }

        return $queries;
    }

    /**
     * Get queries to create or alter table.
     *
     * @param TableEntity $tableAttrs
     *
     * @return array
     */
    private function getNewColumns(TableEntity $tableAttrs)
    {
        $columns = [];

        foreach ($tableAttrs->fields as $field) {
            $val = $field[1];
            if (isset($val[6])) { // auto increment
                $val[1] = ($val[1] == ' bigint' ? ' big' : ($val[1] == ' smallint' ? ' small' : ' ')) . 'serial';
            }
            $columns[] = implode($val);
            if (isset($val[6])) {
                $columns[] = " PRIMARY KEY ($val[0])";
            }
        }

        return $columns;
    }

    /**
     * Get queries to create or alter table.
     *
     * @param TableEntity $tableAttrs
     *
     * @return array
     */
    private function getColumnChanges(TableEntity $tableAttrs)
    {
        $columns = [];

        foreach ($tableAttrs->fields as $field) {
            $val = $field[1];
            if (isset($val[6])) { // auto increment
                $val[1] = ($val[1] == ' bigint' ? ' big' : ($val[1] == ' smallint' ? ' small' : ' ')) . 'serial';
            }
            $columns[] = 'ADD ' . implode($val);
            if (isset($val[6])) {
                $columns[] = "ADD PRIMARY KEY ($val[0])";
            }
        }
        foreach ($tableAttrs->edited as $field) {
            $column = $this->driver->escapeId($field[0]);
            $val = $field[1];
            $columns[] = "ALTER $column TYPE$val[1]";
            if (!$val[6]) {
                $columns[] = "ALTER $column " . ($val[3] ? "SET$val[3]" : 'DROP DEFAULT');
                $columns[] = "ALTER $column " . ($val[2] == ' NULL' ? 'DROP NOT' : 'SET') . $val[2];
            }
        }
        foreach ($tableAttrs->dropped as $column) {
            $columns[] = 'DROP ' . $this->driver->escapeId($column);
        }

        return $columns;
    }

    /**
     * @inheritDoc
     */
    public function moveTables(array $tables, array $views, string $target)
    {
        foreach (array_merge($tables, $views) as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute('ALTER ' . strtoupper($status->engine) . ' ' .
                $this->driver->table($table) . ' SET SCHEMA ' . $this->driver->escapeId($target))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function truncateTables(array $tables)
    {
        $this->driver->execute('TRUNCATE ' . implode(', ', array_map(function ($table) {
            return $this->driver->table($table);
        }, $tables)));
        return true;
    }

    /**
     * @inheritDoc
     */
    public function userTypes()
    {
        $query = 'SELECT typname FROM pg_type WHERE typnamespace = ' .
            '(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) ' .
            "AND typtype IN ('b','d','e') AND typelem = 0";
        return $this->driver->values($query);
    }
}
