<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableFieldDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function array_map;
use function array_reverse;
use function array_unshift;
use function count;
use function implode;
use function is_string;
use function ksort;
use function preg_match;
use function rtrim;
use function uniqid;

class Table extends AbstractTable
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
     * @param string $tableName
     * @param array<ColumnDto> $columns
     *
     * @return array<string>
     */
    private function getColumnRenameQueries(string $tableName, array $columns): array
    {
        $queries = [];
        foreach ($columns as $fieldName => $column) {
            if ($fieldName !== $column->field->name) {
                $fieldName = $this->_statement()->escapeId($fieldName);
                $queries[] = "ALTER TABLE $tableName RENAME $fieldName TO {$column->name}";
            }
        }
        return $queries;
    }

    /**
     * @param string $tableName
     * @param string|null $tableComment
     * @param array<ColumnDto> $columns
     *
     * @return array<string>
     */
    private function getTableCommentQueries(string $tableName, string|null $tableComment, array $columns): array
    {
        $queries = [];
        foreach ($columns as $column) {
            if ($column->comment !== null) {
                $comment = substr($column->comment, 9);
                $queries[] = "COMMENT ON COLUMN $tableName.{$column->name} IS '$comment'";
            }
        }
        if ($tableComment !== null) {
            $queries[] = "COMMENT ON TABLE {$tableName} IS " . $this->_engine()->quote($tableComment);
        }
        return $queries;
    }

    /**
     * @param string $tableName
     * @param ColumnDto $column
     *
     * @return string
     */
    private function getChangedColumnValue(string $tableName, ColumnDto $column): string
    {
        if ($column->defaultValue) {
            $pattern = '~GENERATED ALWAYS(.*) STORED~';
            return "SET" . preg_replace($pattern, 'EXPRESSION\1', $column->defaultValue);
        }

        $sequenceName = "{$tableName}_{$column->field->name}_seq";
        return $column->autoIncrement ?
            "SET DEFAULT nextval(" . $this->_engine()->quote($sequenceName) . ")" :
            "DROP DEFAULT"; //! change to DROP EXPRESSION with generated columns
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getTableSequenceQuery(TableAlterDto $table): array
    {
        foreach ($table->changedColumns as $column) {
            if ($column->autoIncrement) {
                $sequenceName = "{$table->name}_{$column->field->name}_seq";
                $tableName = $this->_statement()->escapeTableName($table->name);
                return [
                    "CREATE SEQUENCE IF NOT EXISTS $sequenceName OWNED BY $tableName.{$column->name}",
                ];
            }
        }
        return [];
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getChangedColumnClauses(TableAlterDto $table): array
    {
        $clauses = [];
        foreach ($table->changedColumns as $fieldName => $column) {
            $fieldName =  $this->_statement()->escapeId($fieldName);
            $clauses[] = "ALTER $fieldName TYPE{$column->type}";
            $clauses[] = "ALTER $fieldName " .
                $this->getChangedColumnValue($table->name, $column);
            $clauses[] = "ALTER $fieldName " .
                ($column->field->nullable ? 'DROP NOT NULL' : 'SET NOT NULL');
        }
        return $clauses;
    }

    /**
     * @param array<ColumnDto> $columns
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
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);
        // Tables columns
        $columns = implode(', ', [
            ...$this->getAddedColumnClauses($table->columns),
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ]);

        return [
            "CREATE TABLE $tableName($columns)",
            ...$this->getTableCommentQueries($tableName, $table->comment, $table->columns),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);
        $renameTableQuery = [];
        if ($table->name !== $table->current->name) {
            $currTableName = $this->_statement()->escapeTableName($table->current->name);
            $renameTableQuery[] = "ALTER TABLE $currTableName RENAME TO $tableName";
        }

        $droppedColumnClauses = array_map(fn($fieldName) =>
            'DROP ' .  $this->_statement()->escapeId($fieldName), $table->droppedColumns);
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
    public function getForeignKeyQueries(TableDto $table): array
    {
        $queries = [];

        $foreignKeys = $this->_engine()->foreignKeys($table->name);
        ksort($foreignKeys);

        foreach ($foreignKeys as $name => $foreignKey) {
            $queries[] = "ALTER TABLE ONLY " . $this->_statement()->escapeId($table->schema) .
                "." . $this->_statement()->escapeId($table->name) . " ADD CONSTRAINT " .
                $this->_statement()->escapeId($name) . " {$foreignKey->definition} " .
                ($foreignKey->deferrable ? 'DEFERRABLE' : 'NOT DEFERRABLE') . ';';
        }

        return $queries;
    }

    /**
     * @param array<TableFieldDto> $fields
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
                $quotedName = $this->_engine()->quote($sequenceName);
                $rows = $this->_engine()->rows($this->_engine()->minVersion(10) ?
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
     * @param TableDto $status
     *
     * @return void
     */
    private function addIndexQueries(TableDto $status): void
    {
        // From pgsql.inc.php
        $tableName = $this->_engine()->quote($status->name);
        // Primary keys are not added here.
        $primaryClause = !$this->_primaryIndexName ? '' :
            " AND indexname != " . $this->_engine()->quote($this->_primaryIndexName);
        $query = "SELECT indexdef FROM pg_catalog.pg_indexes
WHERE schemaname = current_schema() AND tablename = $tableName $primaryClause";
        // Indexes after table definition
        foreach ($this->_engine()->rows($query) as $row) {
            $this->_tableQueries[] = ''; // Insert an empty line
            $this->_tableQueries[] = $row['indexdef'] . ';';
        }
    }

    /**
     * @param array<TableFieldDto> $fields
     * @param TableDto $status
     *
     * @return void
     */
    private function addCommentQueries(array $fields, TableDto $status): void
    {
        $table = $this->_statement()->escapeId($status->schema) .
            '.' . $this->_statement()->escapeId($status->name);
        // Comments for table & fields
        if ($status->comment !== null) {
            $comment = $this->_engine()->quote($status->comment);
            $this->_tableQueries[] = "\nCOMMENT ON TABLE $table IS $comment;";
        }
        foreach ($fields as $name => $field) {
            if ($field->comment !== null) {
                $name = $this->_statement()->escapeId($name);
                $comment = $this->_engine()->quote($field->comment);
                $this->_tableQueries[] = "\nCOMMENT ON COLUMN $table.$name IS $comment;";
            }
        }
    }

    /**
     * @param array<TableFieldDto> $fields
     * @param TableDto $status
     *
     * @return void
     */
    private function addCreateTableQuery(array $fields, TableDto $status): void
    {
        $table = $status->name;
        // From pgsql.inc.php
        $clauses = [];
        // Fields definitions
        foreach ($fields as $field) {
            $clauses[] = $this->_statement()->escapeId($field->name) . ' ' . $field->fullType .
                $this->_statement()->getDefaultValueClause($field) . ($field->nullable ? "" : " NOT NULL");
        }

        $indexes = $this->_engine()->indexes($table);
        ksort($indexes);
        // Primary + unique keys
        $escape = $this->_statement()->escapeId(...);
        foreach ($indexes as $indexName => $index) {
            // Only primary indexes are added here (with the CONSTRAINT keyword).
            if ($index->type === 'PRIMARY') {
                $this->_primaryIndexName = $indexName;
                $indexName = $this->_statement()->escapeId($indexName);
                $indexFields = implode(', ', array_map($escape, $index->columns));
                $clauses[] = "CONSTRAINT $indexName PRIMARY KEY ($indexFields)";
            }
        }

        // Constraints
        $constraints = $this->_engine()->checkConstraints($status);
        foreach ($constraints as $conname => $consrc) {
            $clauses[] = "CONSTRAINT " . $this->_statement()->escapeId($conname) . " CHECK $consrc";
        }

        // Partitions
        $partition = $this->_engine()->partitionsInfo($table);
        $partitionClause = !$partition ? '' :
            "\nPARTITION BY {$partition->strategy}({$partition->fields})";

        $tableName = $this->_statement()->escapeId($status->schema) . '.' . $this->_statement()->escapeId($table);
        $this->_tableQueries[] = "CREATE TABLE $tableName (\n    " .
            implode(",\n    ", $clauses) .
            "\n)$partitionClause\nWITH (oids = " . ($status->oid ? 'true' : 'false') . ");";
    }

    /**
     * @inheritDoc
     */
    public function getExportTableQueries(string $table, bool $autoIncrement, string $style): string
    {
        $status = $this->_engine()->tableStatus($table);
        if ($status === null) {
            return '';
        }

        if ($this->_engine()->isView($status)) {
            $view = $this->_engine()->view($table);
            return rtrim("CREATE VIEW " . $this->_statement()->escapeId($table) . " AS $view[select]", ";");
        }

        $fields = $this->_engine()->fields($table);
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
        return "TRUNCATE " . $this->_statement()->escapeTableName($table);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTriggerQuery(string $table): string
    {
        $status = $this->_engine()->tableStatus($table);
        $query = "";
        foreach ($this->_engine()->triggers($table) as $trg_id => $_) {
            $trigger = $this->_engine()->trigger($trg_id, $status->name);
            $triggerName = $this->_statement()->escapeId($trigger->name);
            $statusName = $this->_statement()->escapeId($status->name);
            $schema = $this->_statement()->escapeId($status->schema);
            $query .= "\nCREATE TRIGGER $triggerName {$trigger->timing} {$trigger->events} " .
                "ON $schema.$statusName {$trigger->type} {$trigger->statement};;\n";
        }
        return $query;
    }

    /**
     * @inheritDoc
     */
    public function getAlterIndexQueries(string $table, array $alter, array $drop): array
    {
        $queries = [];
        $columns = [];
        foreach (array_reverse($drop) as $index) {
            if ($index->type === 'INDEX') {
                $queries[] = 'DROP INDEX ' . $this->_statement()->escapeId($index);
            } else {
                $columns[] = 'DROP CONSTRAINT ' . $this->_statement()->escapeId($index->name);
            }
        }
        foreach ($alter as $index) {
            if ($index->type === 'INDEX') {
                $queries[] = 'CREATE INDEX ' .
                    $this->_statement()->escapeId($index->name != '' ? $index->name : uniqid($table . '_')) .
                    ' ON ' . $this->_statement()->escapeTableName($table) .
                    ' (' . implode(', ', $index->columns) . ')';
            } else {
                //! descending UNIQUE indexes results in syntax error
                $constraint = ($index->name != '' ? ' CONSTRAINT ' . $this->_statement()->escapeId($index->name) : '');
                $columns[] = "ADD$constraint " . ($index->type == 'PRIMARY' ? 'PRIMARY KEY' : $index->type) .
                    ' (' . implode(', ', $index->columns) . ')';
            }
        }
        if (!empty($columns)) {
            array_unshift($queries, 'ALTER TABLE ' .
                $this->_statement()->escapeTableName($table) . implode(', ', $columns));
        }
        return $queries;
    }
}
