<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Db\Grammar as AbstractGrammar;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;

class Grammar extends AbstractGrammar
{
    /**
     * @inheritDoc
     */
    public function escapeId($idf)
    {
        return '"' . str_replace('"', '""', $idf) . '"';
    }

    private function constraints(string $table)
    {
        $constraints = [];
        $query = "SELECT conname, consrc FROM pg_catalog.pg_constraint " .
            "INNER JOIN pg_catalog.pg_namespace ON pg_constraint.connamespace = pg_namespace.oid " .
            "INNER JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid " .
            "AND pg_constraint.connamespace = pg_class.relnamespace WHERE pg_constraint.contype = 'c' " .
            // "-- handle only CONSTRAINTs here, not TYPES " .
            "AND conrelid != 0  AND nspname = current_schema() AND relname = " .
            $this->driver->quote($table) . "ORDER BY connamespace, conname";
        foreach ($this->driver->rows($query) as $row)
        {
            $constraints[$row['conname']] = $row['consrc'];
        }
        return $constraints;
    }

    /**
     * @inheritDoc
     */
    public function getForeignKeysQuery(string $table)
    {
        $query = "";

        $status = $this->driver->tableStatus($table);
        $fkeys = $this->driver->foreignKeys($table);
        ksort($fkeys);

        foreach ($fkeys as $fkey_name => $fkey) {
            $query .= "ALTER TABLE ONLY " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " ADD CONSTRAINT " . $this->escapeId($fkey_name) .
                " {$fkey->definition} " . ($fkey->deferrable ? 'DEFERRABLE' : 'NOT DEFERRABLE') . ";\n";
        }

        return ($query ? "$query\n" : $query);
    }

    /**
     * @param array $fields
     * @param boolean $autoIncrement
     * @param string $style
     *
     * @return array
     */
    private function _sequences(array $fields, bool $autoIncrement, string $style)
    {
        $sequences = [];
        // Fields definitions
        foreach ($fields as $field_name => $field) {
            // sequences for fields
            if (preg_match('~nextval\(\'([^\']+)\'\)~', $field->default, $matches)) {
                $sequence_name = $matches[1];
                $rows = $this->driver->rows($this->driver->minVersion(10) ?
                    ("SELECT *, cache_size AS cache_value FROM pg_sequences " .
                    "WHERE schemaname = current_schema() AND sequencename = " .
                    $this->driver->quote($sequence_name)) : "SELECT * FROM $sequence_name");
                $sq = reset($rows);
                $sequences[] = ($style == "DROP+CREATE" ? "DROP SEQUENCE IF EXISTS $sequence_name;\n" : "") .
                    "CREATE SEQUENCE $sequence_name INCREMENT $sq[increment_by] MINVALUE $sq[min_value] MAXVALUE $sq[max_value]" .
                    ($autoIncrement && $sq['last_value'] ? " START $sq[last_value]" : "") . " CACHE $sq[cache_value];";
            }
        }
        return $sequences;
    }

    /**
     * @param string $table
     * @param array $fields
     * @param array $indexes
     *
     * @return array
     */
    private function _clauses(string $table, array $fields, array $indexes)
    {
        $clauses = [];
        $escape = function($column) { return $this->escapeId($column); };
        // Fields definitions
        foreach ($fields as $field_name => $field) {
            $clauses[] = $this->escapeId($field->name) . ' ' . $field->fullType .
                $this->driver->getDefaultValueClause($field) . ($field->null ? "" : " NOT NULL");
        }
        // Primary + unique keys
        foreach ($indexes as $index_name => $index) {
            switch ($index->type) {
                case 'UNIQUE':
                    $clauses[] = "CONSTRAINT " . $this->escapeId($index_name) .
                        " UNIQUE (" . implode(', ', array_map($escape, $index->columns)) . ")";
                    break;
                case 'PRIMARY':
                    $clauses[] = "CONSTRAINT " . $this->escapeId($index_name) .
                        " PRIMARY KEY (" . implode(', ', array_map($escape, $index->columns)) . ")";
                    break;
            }
        }
        // Constraints
        $constraints = $this->constraints($table);
        foreach ($constraints as $conname => $consrc) {
            $clauses[] = "CONSTRAINT " . $this->escapeId($conname) . " CHECK $consrc";
        }

        return $clauses;
    }

    /**
     * @param array $indexes
     * @param TableEntity $status
     *
     * @return string
     */
    private function _indexQueries(array $indexes, TableEntity $status)
    {
        $query = '';
        // Indexes after table definition
        foreach ($indexes as $index_name => $index) {
            if ($index->type == 'INDEX') {
                $columns = [];
                foreach ($index->columns as $key => $val) {
                    $columns[] = $this->escapeId($val) . ($index->descs[$key] ? " DESC" : "");
                }
                $query .= "\n\nCREATE INDEX " . $this->escapeId($index_name) . " ON " .
                    $this->escapeId($status->schema) . "." . $this->escapeId($status->name) .
                    " USING btree (" . implode(', ', $columns) . ");";
            }
        }
        return $query;
    }

    /**
     * @param array $fields
     * @param TableEntity $status
     *
     * @return string
     */
    private function _commentQueries(array $fields, TableEntity $status)
    {
        $query = '';
        $table = $this->escapeId($status->schema) . '.' . $this->escapeId($status->name);
        // Comments for table & fields
        if ($status->comment) {
            $query .= "\n\nCOMMENT ON TABLE $table IS " . $this->driver->quote($status->comment) . ";";
        }
        foreach ($fields as $name => $field) {
            if ($field->comment) {
                $query .= "\n\nCOMMENT ON COLUMN $table." . $this->escapeId($name) .
                    " IS " . $this->driver->quote($field->comment) . ";";
            }
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQuery(string $table, bool $autoIncrement, string $style)
    {
        $status = $this->driver->tableStatus($table);
        if ($status !== null && $this->driver->isView($status)) {
            $view = $this->driver->view($table);
            return rtrim("CREATE VIEW " . $this->escapeId($table) . " AS $view[select]", ";");
        }

        $fields = $this->driver->fields($table);
        if (empty($status) || empty($fields)) {
            return '';
        }

        $sequences = $this->_sequences($fields, $autoIncrement, $style);
        $indexes = $this->driver->indexes($table);
        ksort($indexes);
        $clauses = $this->_clauses($table, $fields, $indexes);
        // Adding sequences before table definition
        $query = '';
        if (!empty($sequences)) {
            $query = implode("\n\n", $sequences) . "\n\n";
        }
        $query .= 'CREATE TABLE ' . $this->escapeId($status->schema) . '.' . $this->escapeId($status->name) . " (\n    ";
        $query .= implode(",\n    ", $clauses) . "\n) WITH (oids = " . ($status->oid ? 'true' : 'false') . ");";
        $query .= $this->_indexQueries($indexes, $status);
        $query .= $this->_commentQueries($fields, $status);

        return rtrim($query, ';');
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTableQuery(string $table)
    {
        return "TRUNCATE " . $this->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table)
    {
        $status = $this->driver->tableStatus($table);
        $query = "";
        foreach ($this->driver->triggers($table) as $trg_id => $trg) {
            $trigger = $this->driver->trigger($trg_id, $status->name);
            $query .= "\nCREATE TRIGGER " . $this->escapeId($trigger['Trigger']) .
                " $trigger[Timing] $trigger[Events] ON " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " $trigger[Type] $trigger[Statement];;\n";
        }
        return $query;
    }


    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database)
    {
        return "\connect " . $this->escapeId($database);
    }

    /**
     * @inheritDoc
     */
    protected function queryRegex()
    {
        return '\\s*|[\'"]|/\*|-- |$|\$[^$]*\$';
    }
}
