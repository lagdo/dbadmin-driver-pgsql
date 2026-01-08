<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Db\AbstractGrammar;
use Lagdo\DbAdmin\Driver\Entity\ColumnEntity;
use Lagdo\DbAdmin\Driver\Entity\TableAlterEntity;
use Lagdo\DbAdmin\Driver\Entity\TableCreateEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;

use function array_map;
use function count;
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
    public function getAutoIncrementModifier(): string
    {
        return '';
    }

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
    protected function limitToOne(string $table, string $query, string $where): string
    {
        return preg_match('~^INTO~', $query) ?
            $this->getLimitClause($query, $where, 1, 0) :
            " $query" . ($this->driver->isView($this->driver->tableStatusOrName($table)) ?
                $where : " WHERE ctid = (SELECT ctid FROM " .
                    $this->escapeTableName($table) . $where . ' LIMIT 1)');
    }

    /**
     * @param string $tableName
     * @param array<ColumnEntity> $columns
     *
     * @return array<string>
     */
    private function getColumnRenameQueries(string $tableName, array $columns): array
    {
        $queries = [];
        foreach ($columns as $fieldName => $column) {
            if ($fieldName !== $column->field->name) {
                $fieldName = $this->escapeId($fieldName);
                $queries[] = "ALTER TABLE $tableName RENAME $fieldName TO {$column->name}";
            }
        }
        return $queries;
    }

    /**
     * @param string $tableName
     * @param string $tableComment
     * @param array<ColumnEntity> $columns
     *
     * @return array<string>
     */
    private function getTableCommentQueries(string $tableName, string $tableComment, array $columns): array
    {
        $queries = [];
        foreach ($columns as $column) {
            if ($column->comment !== '') {
                $comment = substr($column->comment, 9);
                $queries[] = "COMMENT ON COLUMN $tableName.{$column->name} IS '$comment'";
            }
        }
        if ($tableComment !== '') {
            $queries[] = "COMMENT ON TABLE {$tableName} IS " . $this->driver->quote($tableComment);
        }
        return $queries;
    }

    /**
     * @param string $tableName
     * @param ColumnEntity $column
     *
     * @return string
     */
    private function getChangedColumnValue(string $tableName, ColumnEntity $column): string
    {
        if ($column->defaultValue) {
            $pattern = '~GENERATED ALWAYS(.*) STORED~';
            return "SET" . preg_replace($pattern, 'EXPRESSION\1', $column->defaultValue);
        }

        $sequenceName = "{$tableName}_{$column->field->name}_seq";
        return $column->autoIncrement ?
            "SET DEFAULT nextval(" . $this->driver->quote($sequenceName) . ")" :
            "DROP DEFAULT"; //! change to DROP EXPRESSION with generated columns
    }

    /**
     * @param TableAlterEntity $table
     *
     * @return array<string>
     */
    private function getTableSequenceQuery(TableAlterEntity $table): array
    {
        foreach ($table->changedColumns as $column) {
            if ($column->autoIncrement) {
                $sequenceName = "{$table->name}_{$column->field->name}_seq";
                $tableName = $this->driver->escapeTableName($table->name);
                return [
                    "CREATE SEQUENCE IF NOT EXISTS $sequenceName OWNED BY $tableName.{$column->name}",
                ];
            }
        }
        return [];
    }

    /**
     * @param TableAlterEntity $table
     *
     * @return array<string>
     */
    private function getChangedColumnClauses(TableAlterEntity $table): array
    {
        $clauses = [];
        foreach ($table->changedColumns as $fieldName => $column) {
            $fieldName =  $this->escapeId($fieldName);
            $clauses[] = "ALTER $fieldName TYPE{$column->type}";
            $clauses[] = "ALTER $fieldName " .
                $this->getChangedColumnValue($table->name, $column);
            $clauses[] = "ALTER $fieldName " .
                ($column->field->nullable ? 'DROP NOT NULL' : 'SET NOT NULL');
        }
        return $clauses;
    }

    /**
     * @param array<ColumnEntity> $columns
     * @param string $prefix
     *
     * @return array<string>
     */
    private function getAddedColumnClauses(array $columns, string $prefix = ''): array
    {
        $clauses = [];
        foreach ($columns as $column) {
            if ($column->autoIncrement !== null) { // auto increment
                $column->type = match($column->type) {
                    ' bigint' => ' bigserial',
                    ' smallint' => ' smallserial',
                    default => ' serial',
                };
            }
            $clauses[] = $prefix . $column->clause();
            if ($column->autoIncrement !== null) {
                $clauses[] = "{$prefix}PRIMARY KEY ({$column->name})";
            }
        }
        return $clauses;
    }

    /**
     * @inheritDoc
     */
    public function getTableCreationQueries(TableCreateEntity $table): array
    {
        $tableName = $this->driver->escapeTableName($table->name);
        // Tables columns
        $columns = [
            ...$this->getAddedColumnClauses($table->columns),
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];

        return [
            "CREATE TABLE $tableName" . '(' . implode(', ', $columns) . ')',
            ...$this->getTableCommentQueries($tableName, $table->comment, $table->columns),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getTableAlterationQueries(TableAlterEntity $table): array
    {
        $tableName = $this->driver->escapeTableName($table->name);
        $renameTableQuery = [];
        if ($table->name !== $table->current->name) {
            $currTableName = $this->driver->escapeTableName($table->current->name);
            $renameTableQuery[] = "ALTER TABLE $currTableName RENAME TO $tableName";
        }

        $droppedColumnClauses = array_map(fn($fieldName) =>
            'DROP ' .  $this->escapeId($fieldName), $table->droppedColumns);
        $clauses =  [
            ...$this->getAddedColumnClauses($table->addedColumns, 'ADD '),
            ...$this->getChangedColumnClauses($table),
            ...$droppedColumnClauses,
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];
        $alterTableQuery = count($clauses) === 0 ? [] :
            ["ALTER TABLE $tableName " . implode(', ', $clauses)];

        return [
            ...$this->getTableSequenceQuery($table),
            ...$renameTableQuery,
            ...$alterTableQuery,
            ...$this->getColumnRenameQueries($tableName, $table->changedColumns),
            ...$this->getTableCommentQueries($tableName, $table->comment, [
                ...$table->addedColumns,
                ...$table->changedColumns,
            ]),
        ];
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
     * @param array<TableFieldEntity> $fields
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
                $this->getDefaultValueClause($field) . ($field->nullable ? "" : " NOT NULL");
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
    public function getTableDefinitionQueries(string $table, bool $autoIncrement, string $style): string
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
    public function getTableTruncationQuery(string $table): string
    {
        return "TRUNCATE " . $this->driver->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getTriggerCreationQuery(string $table): string
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
