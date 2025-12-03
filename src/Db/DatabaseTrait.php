<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;

use function substr;
use function implode;
use function array_merge;
use function strtoupper;
use function array_map;

trait DatabaseTrait
{
    /**
     * @param TableEntity $tableAttrs
     * @param array $queries
     *
     * @return void
     */
    private function _getRenameColumnQueries(TableEntity $tableAttrs, array &$queries)
    {
        foreach ($tableAttrs->edited as $field) {
            $column = $this->driver->escapeId($field[0]);
            $val = $field[1];
            $val5 = $val[5] ?? '';
            if ($val[0] !== '' && $column !== $val[0]) {
                $queries[] = 'ALTER TABLE ' . $this->driver->escapeTableName($tableAttrs->name) . " RENAME $column TO $val[0]";
            }
            if ($column !== '' || $val5 !== '') {
                $queries[] = 'COMMENT ON COLUMN ' . $this->driver->escapeTableName($tableAttrs->name) .
                    ".$val[0] IS " . ($val5 !== '' ? substr($val5, 9) : "''");
            }
        }
    }

    /**
     * @param TableEntity $tableAttrs
     * @param array $queries
     *
     * @return void
     */
    private function _getColumnCommentQueries(TableEntity $tableAttrs, array &$queries)
    {
        foreach ($tableAttrs->fields as $field) {
            $column = $this->driver->escapeId($field[0]);
            $val = $field[1];
            $val5 = $val[5] ?? '';
            if ($column !== '' || $val5 !== '') {
                $queries[] = 'COMMENT ON COLUMN ' . $this->driver->escapeTableName($tableAttrs->name) .
                    ".$val[0] IS " . ($val5 !== '' ? substr($val5, 9) : "''");
            }
        }
    }

    /**
     * Get queries to create or alter table.
     *
     * @param TableEntity $tableAttrs
     *
     * @return array
     */
    private function getQueries(TableEntity $tableAttrs): array
    {
        $queries = [];

        $this->_getRenameColumnQueries($tableAttrs, $queries);
        $this->_getColumnCommentQueries($tableAttrs, $queries);
        if ($tableAttrs->comment !== '') {
            $queries[] = 'COMMENT ON TABLE ' . $this->driver->escapeTableName($tableAttrs->name) .
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
    private function getNewColumns(TableEntity $tableAttrs): array
    {
        $columns = [];

        foreach ($tableAttrs->fields as $field) {
            $val = $field[1];
            if (isset($val[6])) { // auto increment
                // Todo: use match
                $val[1] = ($val[1] === ' bigint' ? ' big' : ($val[1] === ' smallint' ? ' small' : ' ')) . 'serial';
            }
            $columns[] = implode($val);
            if (isset($val[6])) {
                $columns[] = " PRIMARY KEY ($val[0])";
            }
        }

        return $columns;
    }

    /**
     * @param TableEntity $tableAttrs
     * @param array $columns
     *
     * @return void
     */
    private function _getCreateColumnQueries(TableEntity $tableAttrs, array &$columns)
    {
        foreach ($tableAttrs->fields as $field) {
            $val = $field[1];
            if (isset($val[6])) { // auto increment
                // Todo: use match
                $val[1] = ($val[1] === ' bigint' ? ' big' : ($val[1] === ' smallint' ? ' small' : ' ')) . 'serial';
            }
            $columns[] = 'ADD ' . implode($val);
            if (isset($val[6])) {
                $columns[] = "ADD PRIMARY KEY ($val[0])";
            }
        }
    }

    /**
     * @param TableEntity $tableAttrs
     * @param array $columns
     *
     * @return void
     */
    private function _getAlterColumnQueries(TableEntity $tableAttrs, array &$columns)
    {
        foreach ($tableAttrs->edited as $field) {
            $column = $this->driver->escapeId($field[0]);
            $val = $field[1];
            $columns[] = "ALTER $column TYPE$val[1]";
            if (!$val[6]) {
                $columns[] = "ALTER $column " . ($val[3] ? "SET$val[3]" : 'DROP DEFAULT');
                $columns[] = "ALTER $column " . ($val[2] === ' NULL' ? 'DROP NOT' : 'SET') . $val[2];
            }
        }
    }

    /**
     * Get queries to create or alter table.
     *
     * @param TableEntity $tableAttrs
     *
     * @return array
     */
    private function getColumnChanges(TableEntity $tableAttrs): array
    {
        $columns = [];

        $this->_getCreateColumnQueries($tableAttrs, $columns);
        $this->_getAlterColumnQueries($tableAttrs, $columns);
        foreach ($tableAttrs->dropped as $column) {
            $columns[] = 'DROP ' . $this->driver->escapeId($column);
        }

        return $columns;
    }

    /**
     * @inheritDoc
     */
    public function moveTables(array $tables, array $views, string $target): bool
    {
        foreach (array_merge($tables, $views) as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute('ALTER ' . strtoupper($status->engine) . ' ' .
                $this->driver->escapeTableName($table) . ' SET SCHEMA ' . $this->driver->escapeId($target))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function truncateTables(array $tables): bool
    {
        $this->driver->execute('TRUNCATE ' . implode(', ', array_map(function ($table) {
            return $this->driver->escapeTableName($table);
        }, $tables)));
        return true;
    }
}
