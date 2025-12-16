<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Db\AbstractGrammar;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;

use function array_map;
use function implode;
use function is_string;
use function ksort;
use function preg_match;
use function rtrim;
use function str_replace;

class Grammar extends AbstractGrammar
{
    /**
     * @var array
     */
    private $_tableQueries;

    /**
     * @var string
     */
    private $_primaryIndexName;

    /**
     * @inheritDoc
     */
    public function escapeId(string $idf): string
    {
        return '"' . str_replace('"', '""', $idf) . '"';
    }

    /**
     * @inheritDoc
     */
    public function getForeignKeysQueries(TableEntity $table): array
    {
        $queries = [];

        $foreignKeys = $this->driver->foreignKeys($table->name);
        ksort($foreignKeys);

        foreach ($foreignKeys as $name => $foreignKey) {
            $queries[] = "ALTER TABLE ONLY " . $this->escapeId($table->schema) .
                "." . $this->escapeId($table->name) . " ADD CONSTRAINT " .
                $this->escapeId($name) . " {$foreignKey->definition} " .
                ($foreignKey->deferrable ? 'DEFERRABLE' : 'NOT DEFERRABLE') . ';';
        }

        return $queries;
    }

    /**
     * @param array $fields
     * @param boolean $autoIncrement
     * @param string $style
     *
     * @return void
     */
    private function addSequenceQueries(array $fields, bool $autoIncrement, string $style): void
    {
        // Fields definitions
        foreach ($fields as $field) {
            $default = $field->hasDefault() && is_string($field->default) ? $field->default : '';
            // sequences for fields
            if (preg_match('~nextval\(\'([^\']+)\'\)~', $default, $matches)) {
                $sequenceName = $matches[1];
                $quotedName = $this->driver->quote($sequenceName);
                $rows = $this->driver->rows($this->driver->minVersion(10) ?
                    "SELECT *, cache_size AS cache_value FROM pg_sequences " .
                        "WHERE schemaname = current_schema() AND sequencename = $quotedName" :
                    "SELECT * FROM $sequenceName");
                $sequence = reset($rows);
                if ($style == "DROP+CREATE") {
                    $this->_tableQueries[] = "DROP SEQUENCE IF EXISTS $sequenceName;";
                }

                $incrementBy = $sequence['increment_by'];
                $minValue = $sequence['min_value'];
                $maxValue = $sequence['max_value'];
                $lastValue = !($autoIncrement && $sequence['last_value']) ? '' :
                    ' START ' . ((int)$sequence['last_value'] + 1);
                $cacheValue = $sequence['cache_value'];
                $this->_tableQueries[] = "CREATE SEQUENCE $sequenceName INCREMENT $incrementBy " .
                    "MINVALUE $minValue MAXVALUE $maxValue$lastValue CACHE $cacheValue;";
                $this->_tableQueries[] = ''; // Insert an empty line after each sequence.
            }
        }
    }

    /**
     * @param TableEntity $status
     *
     * @return void
     */
    private function addIndexQueries(TableEntity $status): void
    {
        // From pgsql.inc.php
        $tableName = $this->driver->quote($status->name);
        // Primary keys are not added here.
        $primaryClause = !$this->_primaryIndexName ? '' :
            " AND indexname != " . $this->driver->quote($this->_primaryIndexName);
        $query = "SELECT indexdef FROM pg_catalog.pg_indexes
WHERE schemaname = current_schema() AND tablename = $tableName $primaryClause";
        // Indexes after table definition
		foreach ($this->driver->rows($query) as $row) {
            $this->_tableQueries[] = ''; // Insert an empty line
			$this->_tableQueries[] = $row['indexdef'] . ';';
		}
    }

    /**
     * @param array $fields
     * @param TableEntity $status
     *
     * @return void
     */
    private function addCommentQueries(array $fields, TableEntity $status): void
    {
        $table = $this->escapeId($status->schema) . '.' . $this->escapeId($status->name);
        // Comments for table & fields
        if ($status->comment) {
            $this->_tableQueries[] = "\nCOMMENT ON TABLE $table IS " . $this->driver->quote($status->comment) . ";";
        }
        foreach ($fields as $name => $field) {
            if ($field->comment) {
                $this->_tableQueries[] = "\nCOMMENT ON COLUMN $table." . $this->escapeId($name) .
                    " IS " . $this->driver->quote($field->comment) . ";";
            }
        }
    }

    /**
     * @param string $table
     * @param array $fields
     * @param TableEntity $status
     *
     * @return void
     */
    private function addCreateTableQuery(array $fields, TableEntity $status): void
    {
        $table = $status->name;
        // From pgsql.inc.php
        $clauses = [];
        // Fields definitions
        foreach ($fields as $field) {
            $clauses[] = $this->escapeId($field->name) . ' ' . $field->fullType .
                $this->driver->getDefaultValueClause($field) . ($field->null ? "" : " NOT NULL");
        }

        $indexes = $this->driver->indexes($table);
        ksort($indexes);
        // Primary + unique keys
        $escape = fn($column) => $this->escapeId($column);
        foreach ($indexes as $indexName => $index) {
            // Only primary indexes are added here (with the CONSTRAINT keyword).
            if ($index->type === 'PRIMARY') {
                $this->_primaryIndexName = $indexName;
                $indexName = $this->escapeId($indexName);
                $indexFields = implode(', ', array_map($escape, $index->columns));
                $clauses[] = "CONSTRAINT $indexName PRIMARY KEY ($indexFields)";
            }
        }

        // Constraints
        $constraints = $this->driver->checkConstraints($status);
        foreach ($constraints as $conname => $consrc) {
            $clauses[] = "CONSTRAINT " . $this->escapeId($conname) . " CHECK $consrc";
        }

        // Partitions
        $partition = $this->driver->partitionsInfo($table);
        $partitionClause = !$partition ? '' :
            "\nPARTITION BY {$partition->strategy}({$partition->fields})";

        $tableName = $this->escapeId($status->schema) . '.' . $this->escapeId($table);
        $this->_tableQueries[] = "CREATE TABLE $tableName (\n    " .
            implode(",\n    ", $clauses) .
            "\n)$partitionClause\nWITH (oids = " . ($status->oid ? 'true' : 'false') . ");";
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQuery(string $table, bool $autoIncrement, string $style): string
    {
        $status = $this->driver->tableStatus($table);
        if ($status === null) {
            return '';
        }

        if ($this->driver->isView($status)) {
            $view = $this->driver->view($table);
            return rtrim("CREATE VIEW " . $this->escapeId($table) . " AS $view[select]", ";");
        }

        $fields = $this->driver->fields($table);
        if (empty($fields)) {
            return '';
        }

        $this->_tableQueries = [];
        $this->_primaryIndexName = '';
        // Adding sequences before table definition
        $this->addSequenceQueries($fields, $autoIncrement, $style);
        $this->addCreateTableQuery($fields, $status);
        $this->addIndexQueries($status);
        $this->addCommentQueries($fields, $status);

        return rtrim(implode("\n", $this->_tableQueries), ';');
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTableQuery(string $table): string
    {
        return "TRUNCATE " . $this->driver->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table): string
    {
        $status = $this->driver->tableStatus($table);
        $query = "";
        foreach ($this->driver->triggers($table) as $trg_id => $_) {
            $trigger = $this->driver->trigger($trg_id, $status->name);
            $triggerName = $this->escapeId($trigger->name);
            $statusName = $this->escapeId($status->name);
            $schema = $this->escapeId($status->schema);
            $query .= "\nCREATE TRIGGER $triggerName {$trigger->timing} {$trigger->events} " .
                "ON $schema.$statusName {$trigger->type} {$trigger->statement};;\n";
        }
        return $query;
    }

    /**
     * @param string $database
     * @param string $style
     *
     * @return string
     */
    private function getCreateDatabaseQuery(string $database, string $style = ''): string
    {
        if (!preg_match('~CREATE~', $style)) {
            return '';
        }

        $drop = $style !== 'DROP+CREATE' ? '' : "DROP DATABASE IF EXISTS $database;\n";
        $create = "CREATE DATABASE $database;\n";
        return "{$drop}{$create}";
    }

    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database, string $style = ''): string
    {
        $name = $this->escapeId($database);
        return $this->getCreateDatabaseQuery($name, $style) . "\\connect $name;";
    }
}
