<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;
use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Db\Grammar as AbstractGrammar;

class Grammar extends AbstractGrammar
{
    /**
     * @inheritDoc
     */
    public function escapeId($idf)
    {
        return '"' . str_replace('"', '""', $idf) . '"';
    }

    /**
     * @inheritDoc
     */
    public function limit(string $query, string $where, int $limit, int $offset = 0, string $separator = " ")
    {
        return " $query$where" . ($limit !== null ? $separator . "LIMIT $limit" .
            ($offset ? " OFFSET $offset" : "") : "");
    }

    /**
     * @inheritDoc
     */
    public function limitToOne(string $table, string $query, string $where, string $separator = "\n")
    {
        return (preg_match('~^INTO~', $query) ? $this->limit($query, $where, 1, 0, $separator) :
            " $query" . ($this->driver->isView($this->driver->tableStatusOrName($table)) ? $where :
            " WHERE ctid = (SELECT ctid FROM " . $this->table($table) . $where . $separator . "LIMIT 1)")
        );
    }

    private function constraints(string $table)
    {
        $return = [];
        $query = "SELECT conname, consrc FROM pg_catalog.pg_constraint " .
            "INNER JOIN pg_catalog.pg_namespace ON pg_constraint.connamespace = pg_namespace.oid " .
            "INNER JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid " .
            "AND pg_constraint.connamespace = pg_class.relnamespace WHERE pg_constraint.contype = 'c' " .
            // "-- handle only CONSTRAINTs here, not TYPES " .
            "AND conrelid != 0  AND nspname = current_schema() AND relname = " .
            $this->quote($table) . "ORDER BY connamespace, conname";
        foreach ($this->driver->rows($query) as $row)
        {
            $return[$row['conname']] = $row['consrc'];
        }
        return $return;
    }

    /**
     * @inheritDoc
     */
    public function foreignKeysSql(string $table)
    {
        $return = "";

        $status = $this->driver->tableStatus($table);
        $fkeys = $this->driver->foreignKeys($table);
        ksort($fkeys);

        foreach ($fkeys as $fkey_name => $fkey) {
            $return .= "ALTER TABLE ONLY " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " ADD CONSTRAINT " . $this->escapeId($fkey_name) .
                " {$fkey->definition} " . ($fkey->deferrable ? 'DEFERRABLE' : 'NOT DEFERRABLE') . ";\n";
        }

        return ($return ? "$return\n" : $return);
    }

    /**
     * @inheritDoc
     */
    public function createTableSql(string $table, bool $autoIncrement, string $style)
    {
        $return = '';
        $return_parts = [];
        $sequences = [];

        $status = $this->driver->tableStatus($table);
        if ($this->driver->isView($status)) {
            $view = $this->driver->view($table);
            return rtrim("CREATE VIEW " . $this->escapeId($table) . " AS $view[select]", ";");
        }
        $fields = $this->driver->fields($table);
        $indexes = $this->driver->indexes($table);
        ksort($indexes);
        $constraints = $this->constraints($table);

        if (!$status || empty($fields)) {
            return false;
        }

        $return = "CREATE TABLE " . $this->escapeId($status->schema) . "." .
            $this->escapeId($status->name) . " (\n    ";

        // fields' definitions
        foreach ($fields as $field_name => $field) {
            $part = $this->escapeId($field->name) . ' ' . $field->fullType .
                $this->driver->defaultValue($field) . ($field->attnotnull ? " NOT NULL" : "");
            $return_parts[] = $part;

            // sequences for fields
            if (preg_match('~nextval\(\'([^\']+)\'\)~', $field->default, $matches)) {
                $sequence_name = $matches[1];
                $sq = reset($this->driver->rows($this->driver->minVersion(10) ?
                    "SELECT *, cache_size AS cache_value FROM pg_sequences " .
                    "WHERE schemaname = current_schema() AND sequencename = " .
                    $this->quote($sequence_name) : "SELECT * FROM $sequence_name"));
                $sequences[] = ($style == "DROP+CREATE" ? "DROP SEQUENCE IF EXISTS $sequence_name;\n" : "") .
                    "CREATE SEQUENCE $sequence_name INCREMENT $sq[increment_by] MINVALUE $sq[min_value] MAXVALUE $sq[max_value]" .
                    ($autoIncrement && $sq['last_value'] ? " START $sq[last_value]" : "") . " CACHE $sq[cache_value];";
            }
        }

        // adding sequences before table definition
        if (!empty($sequences)) {
            $return = implode("\n\n", $sequences) . "\n\n$return";
        }

        // primary + unique keys
        foreach ($indexes as $index_name => $index) {
            switch ($index->type) {
                case 'UNIQUE':
                    $return_parts[] = "CONSTRAINT " . $this->escapeId($index_name) .
                        " UNIQUE (" . implode(', ', array_map(function ($column) {
                            return $this->escapeId($column);
                        }, $index->columns)) . ")";
                    break;
                case 'PRIMARY':
                    $return_parts[] = "CONSTRAINT " . $this->escapeId($index_name) .
                        " PRIMARY KEY (" . implode(', ', array_map(function ($column) {
                            return $this->escapeId($column);
                        }, $index->columns)) . ")";
                    break;
            }
        }

        foreach ($constraints as $conname => $consrc) {
            $return_parts[] = "CONSTRAINT " . $this->escapeId($conname) . " CHECK $consrc";
        }

        $return .= implode(",\n    ", $return_parts) . "\n) WITH (oids = " . ($status->oid ? 'true' : 'false') . ");";

        // "basic" indexes after table definition
        foreach ($indexes as $index_name => $index) {
            if ($index->type == 'INDEX') {
                $columns = [];
                foreach ($index->columns as $key => $val) {
                    $columns[] = $this->escapeId($val) . ($index->descs[$key] ? " DESC" : "");
                }
                $return .= "\n\nCREATE INDEX " . $this->escapeId($index_name) . " ON " .
                    $this->escapeId($status->schema) . "." . $this->escapeId($status->name) .
                    " USING btree (" . implode(', ', $columns) . ");";
            }
        }

        // coments for table & fields
        if ($status->comment) {
            $return .= "\n\nCOMMENT ON TABLE " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " IS " . $this->quote($status->comment) . ";";
        }

        foreach ($fields as $field_name => $field) {
            if ($field->comment) {
                $return .= "\n\nCOMMENT ON COLUMN " . $this->escapeId($status->schema) . "." .
                    $this->escapeId($status->name) . "." . $this->escapeId($field_name) .
                    " IS " . $this->quote($field->comment) . ";";
            }
        }

        return rtrim($return, ';');
    }

    /**
     * @inheritDoc
     */
    public function truncateTableSql(string $table)
    {
        return "TRUNCATE " . $this->table($table);
    }

    /**
     * @inheritDoc
     */
    public function createTriggerSql(string $table)
    {
        $status = $this->driver->tableStatus($table);
        $return = "";
        foreach ($this->driver->triggers($table) as $trg_id => $trg) {
            $trigger = $this->driver->trigger($trg_id, $status->name);
            $return .= "\nCREATE TRIGGER " . $this->escapeId($trigger['Trigger']) .
                " $trigger[Timing] $trigger[Events] ON " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " $trigger[Type] $trigger[Statement];;\n";
        }
        return $return;
    }


    /**
     * @inheritDoc
     */
    public function useDatabaseSql(string $database)
    {
        return "\connect " . $this->escapeId($database);
    }
}
