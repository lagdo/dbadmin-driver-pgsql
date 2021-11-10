<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\Database as AbstractDatabase;

class Database extends AbstractDatabase
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
    public function createTable(TableEntity $tableAttrs)
    {
        $queries = $this->getQueries($tableAttrs);
        $columns = $this->getNewColumns($tableAttrs);
        $columns = array_merge($columns, $tableAttrs->foreign);
        array_unshift($queries, 'CREATE TABLE ' . $this->driver->table($tableAttrs->name) .
            '(' . implode(', ', $columns) . ')');
        foreach ($queries as $query) {
            $this->driver->execute($query);
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function alterTable(string $table, TableEntity $tableAttrs)
    {
        $queries = $this->getQueries($tableAttrs);
        $columns = $this->getColumnChanges($tableAttrs);
        if ($tableAttrs->name !== '' && $table !== $tableAttrs->name) {
            array_unshift($queries, 'ALTER TABLE ' . $this->driver->table($table) .
                ' RENAME TO ' . $this->driver->table($tableAttrs->name));
        }
        $columns = array_merge($columns, $tableAttrs->foreign);
        if (!empty($columns)) {
            array_unshift($queries, 'ALTER TABLE ' . $this->driver->table($table) . ' ' . implode(', ', $columns));
        }
        // if ($tableAttrs->autoIncrement != '') {
        //     //! $queries[] = 'SELECT setval(pg_get_serial_sequence(' . $this->driver->quote($tableAttrs->name) . ', ), $tableAttrs->autoIncrement)';
        // }
        foreach ($queries as $query) {
            $this->driver->execute($query);
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function alterIndexes(string $table, array $alter, array $drop)
    {
        $queries = [];
        $columns = [];
        foreach (array_reverse($drop) as $index) {
            if ($index->type === 'INDEX') {
                $queries[] = 'DROP INDEX ' . $this->driver->escapeId($index);
            } else {
                $columns[] = 'DROP CONSTRAINT ' . $this->driver->escapeId($index->name);
            }
        }
        foreach ($alter as $index) {
            if ($index->type === 'INDEX') {
                $queries[] = 'CREATE INDEX ' .
                    $this->driver->escapeId($index->name != '' ? $index->name : uniqid($table . '_')) .
                    ' ON ' . $this->driver->table($table) . ' (' . implode(', ', $index->columns) . ')';
            } else {
                //! descending UNIQUE indexes results in syntax error
                $constraint = ($index->name != '' ? ' CONSTRAINT ' . $this->driver->escapeId($index->name) : '');
                $columns[] = "ADD$constraint " . ($index->type == 'PRIMARY' ? 'PRIMARY KEY' : $index->type) .
                    ' (' . implode(', ', $index->columns) . ')';
            }
        }
        if (!empty($columns)) {
            array_unshift($queries, 'ALTER TABLE ' . $this->driver->table($table) . implode(', ', $columns));
        }
        foreach ($queries as $query) {
            $this->driver->execute($query);
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function tables()
    {
        $query = 'SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()';
        if ($this->driver->support('materializedview')) {
            $query .= " UNION ALL SELECT matviewname, 'MATERIALIZED VIEW' FROM pg_matviews WHERE schemaname = current_schema()";
        }
        $query .= ' ORDER BY 1';
        return $this->driver->keyValues($query);
    }

    /**
     * @inheritDoc
     */
    public function sequences()
    {
        // From db.inc.php
        $query = 'SELECT sequence_name FROM information_schema.sequences ' .
            'WHERE sequence_schema = selectedSchema() ORDER BY sequence_name';
        return $this->driver->values($query);
    }

    /**
     * @inheritDoc
     */
    public function countTables(array $databases)
    {
        $connection = $this->driver->createConnection(); // New connection
        $counts = [];
        $systemSchemas = ['information_schema', 'pg_catalog', 'pg_temp_1', 'pg_toast', 'pg_toast_temp_1'];
        $query = "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('" .
            implode("','", $systemSchemas) . "')";
        foreach ($databases as $database) {
            $counts[$database] = 0;
            if (!$connection->open($database)) {
                continue;
            }
            $statement = $connection->query($query);
            if (is_object($statement) && ($row = $statement->fetchRow())) {
                $counts[$database] = intval($row[0]);
            }
        }
        return $counts;
    }

    /**
     * @inheritDoc
     */
    public function dropViews(array $views)
    {
        return $this->dropTables($views);
    }

    /**
     * @inheritDoc
     */
    public function dropTables(array $tables)
    {
        foreach ($tables as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute('DROP ' . strtoupper($status->engine) . ' ' . $this->driver->table($table))) {
                return false;
            }
        }
        return true;
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

    /**
     * @inheritDoc
     */
    public function schemas()
    {
        return $this->driver->values('SELECT nspname FROM pg_namespace ORDER BY nspname');
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type)
    {
        $query = 'SELECT routine_definition AS definition, LOWER(external_language) AS language, * ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ' .
            'AND specific_name = ' . $this->driver->quote($name);
        $rows = $this->driver->rows($query);
        $routines = $rows[0];
        $routines['returns'] = ['type' => $routines['type_udt_name']];
        $query = 'SELECT parameter_name AS field, data_type AS type, character_maximum_length AS length, ' .
            'parameter_mode AS inout FROM information_schema.parameters WHERE specific_schema = current_schema() ' .
            'AND specific_name = ' . $this->driver->quote($name) . ' ORDER BY ordinal_position';
        $routines['fields'] = $this->driver->rows($query);
        return $routines;
    }

    /**
     * @inheritDoc
     */
    public function routines()
    {
        $query = 'SELECT specific_name AS "SPECIFIC_NAME", routine_type AS "ROUTINE_TYPE", ' .
            'routine_name AS "ROUTINE_NAME", type_udt_name AS "DTD_IDENTIFIER" ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ORDER BY SPECIFIC_NAME';
        $rows = $this->driver->rows($query);
        return array_map(function($row) {
            return new RoutineEntity($row['ROUTINE_NAME'], $row['SPECIFIC_NAME'], $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER']);
        }, $rows);
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row)
    {
        $routine = [];
        foreach ($row['fields'] as $field) {
            $routine[] = $field->type;
        }
        return $this->driver->escapeId($name) . '(' . implode(', ', $routine) . ')';
    }
}
