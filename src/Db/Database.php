<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\Database as AbstractDatabase;

class Database extends AbstractDatabase
{
    use DatabaseTrait;

    /**
     * PostgreSQL system schemas
     *
     * @var array
     */
    protected $systemSchemas = ['information_schema', 'pg_catalog', 'pg_temp_1', 'pg_toast', 'pg_toast_temp_1'];

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
        $counts = [];
        $query = "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('" .
            implode("','", $this->systemSchemas) . "')";
        foreach ($databases as $database) {
            $counts[$database] = 0;
            $connection = $this->driver->connect($database); // New connection
            if (!$connection) {
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
    public function schemas()
    {
        $query = "SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('" .
            implode("','", $this->systemSchemas) . "') ORDER BY nspname";
        return $this->driver->values($query);
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
