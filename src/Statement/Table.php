<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnInputDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ForeignKeyDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDdlDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function array_filter;
use function array_map;
use function array_reduce;
use function array_reverse;
use function array_values;
use function count;
use function implode;
use function ksort;
use function preg_match;
use function preg_replace;
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
     * @return string
     */
    protected function getEditColumnClause(ColumnInputDto $input): string
    {
        $name = $this->_statement()->escapeId($input->name);
        $type = $this->_statement()->getColumnType($input->typeColumn ?? $input);

        $nullValue = $input->nullable ? ' NULL' : ' NOT NULL'; // NULL for timestamp
        $defaultValue = $this->getDefaultValueClause($input);
        $autoIncrement = $input->autoIncrement ?
            $this->_statement()->getAutoIncrementModifier() : '';

        // MariaDB exports CURRENT_TIMESTAMP as a function.
        $onUpdate = $input->onUpdate === '' || !preg_match('~timestamp|datetime~', $type) ?
            '' : ' ON UPDATE ' . $this->fixOnUpdateTimestamp($input->onUpdate);
        $comment = $this->_engine()->support('comment') && $input->comment !== null ?
            ' COMMENT ' . $this->_engine()->quote($input->comment) : '';

        return "$name$type$nullValue$defaultValue$onUpdate$comment$autoIncrement";
    }

    /**
     * @param TableDdlDto $table
     * @param array<ColumnInputDto> $inputs
     *
     * @return array<string>
     */
    private function getTableCommentQueries(TableDdlDto $table, array $inputs): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);
        $filter = fn(ColumnInputDto $input) => $input->commentChanged();
        $columnQueries = array_map(function(ColumnInputDto $input) use($tableName) {
            $columnName = $this->_statement()->escapeTableName($input->name);
            $comment = $this->_engine()->quote($input->comment);
            return "COMMENT ON COLUMN $tableName.$columnName IS $comment";
        }, array_filter($inputs, $filter));

        $tableQueries = $table->commentChanged() ? [
            "COMMENT ON TABLE {$tableName} IS " . $this->_engine()->quote($table->comment),
        ] : [];

        return [...$columnQueries, ...$tableQueries];
    }

    /**
     * @param TableAlterDto $table
     *
     * @return string
     */
    private function getTableSequenceQuery(TableAlterDto $table): string
    {
        $inputsWithAutoIncrement = array_values(array_filter($table->editedColumns(),
            fn(ColumnInputDto $input) => $input->autoIncrement));
        $autoIncrementInput = $inputsWithAutoIncrement[0] ?? null;
        if ($autoIncrementInput === null) {
            return '';
        }

        $sequenceName = "{$table->name}_{$autoIncrementInput->name}_seq";
        $tableName = $this->_statement()->escapeTableName($table->name);
        $columnName = $this->_statement()->escapeId($autoIncrementInput->name);
        return "CREATE SEQUENCE IF NOT EXISTS $sequenceName OWNED BY $tableName.$columnName";
    }

    /**
     * @param ColumnInputDto $input
     * @param string $tableName
     *
     * @return string
     */
    private function getEditedColumnValue(ColumnInputDto $input, string $tableName): string
    {
        if ($input->default !== null) {
            $pattern = '~GENERATED ALWAYS(.*) STORED~';
            return "SET " . preg_replace($pattern, 'EXPRESSION\1', $input->default);
        }

        $sequenceName = $this->_engine()->quote("{$tableName}_{$input->name}_seq");
        return $input->autoIncrement ? "SET DEFAULT nextval($sequenceName)" :
            "DROP DEFAULT"; //! change to DROP EXPRESSION with generated columns
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getEditColumnClauses(TableAlterDto $table): array
    {
        $columnCb = function(array $clauses, ColumnInputDto $input) use($table) {
            // These queries are execued before the columns rename.
            // They must then use the current column names.
            $columnName =  $this->_statement()->escapeId($input->column->name);

            if ($input->typeChanged()) {
                $type = $this->_statement()->getColumnType($input->typeColumn ?? $input);
                $clauses[] = "ALTER $columnName TYPE $type";
            }
            if ($input->valueChanged()) {
                $columnValue = $this->getEditedColumnValue($input, $table->name);
                $clauses[] = "ALTER $columnName $columnValue";
            }
            if ($input->nullableChanged()) {
                $nullable = $input->nullable ? 'DROP NOT NULL' : 'SET NOT NULL';
                $clauses[] = "ALTER $columnName $nullable";
            }

            return $clauses;
        };

        return array_reduce($table->editedColumns(), $columnCb, []);
    }

    /**
     * @param array<ColumnInputDto> $inputs
     * @param string $prefix
     *
     * @return array<string>
     */
    private function getAddColumnClauses(array $inputs, string $prefix = ''): array
    {
        $columnCb = function(array $clauses, ColumnInputDto $input) use($prefix) {
            if ($input->autoIncrement) { // auto increment
                $input->type = match($input->type) {
                    ' bigint' => ' bigserial',
                    ' smallint' => ' smallserial',
                    default => ' serial',
                };
            }
            $columnClauses = [$prefix . $this->getAddColumnClause($input)];
            if ($input->primary) {
                $columnName = $this->_statement()->escapeId($input->name);
                $columnClauses[] = "{$prefix}PRIMARY KEY ($columnName)";
            }

            return [
                ...$clauses,
                ...$columnClauses,
            ];
        };
        return array_reduce($inputs, $columnCb, []);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);
        $columns = $table->addedColumns();
        // Tables columns
        $clauses = implode("\n  ", [
            ...$this->getAddColumnClauses($columns),
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ]);

        return [
            "CREATE TABLE $tableName(\n  $clauses\n)",
            ...$this->getTableCommentQueries($table, $columns),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);

        $sequenceQuery = $this->getTableSequenceQuery($table);
        $tableQueries = $sequenceQuery !== '' ? [$sequenceQuery] : [];

        if ($table->nameChanged()) {
            $currTableName = $this->_statement()->escapeTableName($table->current->name);
            $tableQueries[] = "ALTER TABLE $currTableName RENAME TO $tableName";
        }

        $tableClauses =  [
            ...$this->getAddColumnClauses($table->addedColumns(), 'ADD '),
            ...$this->getEditColumnClauses($table),
            ...$this->getDropColumnClauses($table),
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];
        if (count($tableClauses) > 0) {
            $tableQueries[] = "ALTER TABLE $tableName\n  " . implode(",\n  ", $tableClauses);
        }

        $renameColumnFilter = fn(ColumnInputDto $input) => $input->nameChanged();
        $renameColumnsQueries = array_map(function(ColumnInputDto $input) use($tableName) {
            $currName = $this->_statement()->escapeId($input->column->name);
            $newName = $this->_statement()->escapeId($input->name);

            return "ALTER TABLE $tableName RENAME $currName TO $newName";
        }, array_filter($table->editedColumns(), $renameColumnFilter));
        // Using array_values() is important for the final merge.
        $renameColumnsQueries = array_values($renameColumnsQueries);

        return [
            ...$tableQueries,
            ...$renameColumnsQueries,
            ...$this->getTableCommentQueries($table, [
                ...$table->addedColumns(),
                ...$table->editedColumns(),
            ]),
        ];
    }

    /**
     * @inheritDoc
     */
    public function getForeignKeyQueries(TableDto $table): array
    {
        $foreignKeys = $this->_engine()->foreignKeys($table->name);
        ksort($foreignKeys);

        return array_map(function(ForeignKeyDto $foreignKey, string $name) use($table) {
            $tableSchema = $this->_statement()->escapeId($table->schema);
            $tableName = $this->_statement()->escapeId($table->name);
            $constraint = $this->_statement()->escapeId($name);
            $deferrable = $foreignKey->deferrable ? 'DEFERRABLE' : 'NOT DEFERRABLE';
            return "ALTER TABLE ONLY $tableSchema.$tableName " .
                "ADD CONSTRAINT $constraint {$foreignKey->definition} $deferrable;";
        }, $foreignKeys, array_values($foreignKeys));
    }

    /**
     * @param array<ColumnDto> $columns
     * @param boolean $autoIncrement
     * @param string $style
     *
     * @return void
     */
    private function addSequenceQueries(array $columns, bool $autoIncrement, string $style): void
    {
        // Columns definitions
        foreach ($columns as $column) {
            $default = $column->hasDefault(true) ? $column->default : '';
            // sequences for columns
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
     * @param array<ColumnDto> $columns
     * @param TableDto $status
     *
     * @return void
     */
    private function addCommentQueries(array $columns, TableDto $status): void
    {
        $table = $this->_statement()->escapeId($status->schema) .
            '.' . $this->_statement()->escapeId($status->name);
        // Comments for table & columns
        if ($status->comment !== null) {
            $comment = $this->_engine()->quote($status->comment);
            $this->_tableQueries[] = "\nCOMMENT ON TABLE $table IS $comment;";
        }
        foreach ($columns as $name => $column) {
            if ($column->comment !== null) {
                $name = $this->_statement()->escapeId($name);
                $comment = $this->_engine()->quote($column->comment);
                $this->_tableQueries[] = "\nCOMMENT ON COLUMN $table.$name IS $comment;";
            }
        }
    }

    /**
     * @param array<ColumnDto> $columns
     * @param TableDto $status
     *
     * @return void
     */
    private function addCreateTableQuery(array $columns, TableDto $status): void
    {
        $table = $status->name;
        // From pgsql.inc.php
        $clauses = [];
        // Columns definitions
        foreach ($columns as $column) {
            $clauses[] = $this->_statement()->escapeId($column->name) . ' ' .
                $column->fullType . $this->getDefaultValueClause($column) .
                ($column->nullable ? "" : " NOT NULL");
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
                $indexColumns = implode(', ', array_map($escape, $index->columns));
                $clauses[] = "CONSTRAINT $indexName PRIMARY KEY ($indexColumns)";
            }
        }

        // Constraints
        $constraints = $this->_engine()->checkConstraints($status);
        foreach ($constraints as $conname => $consrc) {
            $clauses[] = "CONSTRAINT " . $this->_statement()->escapeId($conname) . " CHECK $consrc";
        }

        // Partitions
        $partition = $this->_engine()->partitionsInfo($table);
        $tableName = $this->_statement()->escapeId($status->schema) .
            '.' . $this->_statement()->escapeId($table);
        $this->_tableQueries[] = "CREATE TABLE $tableName (
    " . implode(",
    ", $clauses) . "
)" . (!$partition ? '' :"
PARTITION BY {$partition->strategy}({$partition->columns})") . "
WITH (oids = " . ($status->oid ? 'true' : 'false') . ");";
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
            $viewName = $this->_statement()->escapeId($table);
            return rtrim("CREATE VIEW $viewName AS {$view['select']}", ";");
        }

        $columns = $this->_engine()->columns($table);
        if (empty($columns)) {
            return '';
        }

        $this->_tableQueries = [];
        $this->_primaryIndexName = '';
        // Adding sequences before table definition
        $this->addSequenceQueries($columns, $autoIncrement, $style);
        $this->addCreateTableQuery($columns, $status);
        $this->addIndexQueries($status);
        $this->addCommentQueries($columns, $status);

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
        $triggers = array_values($this->_engine()->triggers($table));
        $triggers = array_map(function(string $triggerId) use($status) {
            $trigger = $this->_engine()->trigger($triggerId, $status->name);
            $triggerName = $this->_statement()->escapeId($trigger->name);
            $statusName = $this->_statement()->escapeId($status->name);
            $schema = $this->_statement()->escapeId($status->schema);
            return "CREATE TRIGGER $triggerName {$trigger->timing} {$trigger->events} " .
                "ON $schema.$statusName {$trigger->type} {$trigger->statement}";
        }, $triggers);

        return "\n" . implode(";;\n\n", $triggers);
    }

    /**
     * @inheritDoc
     */
    public function getAlterIndexQueries(string $table, array $alter, array $drop): array
    {
        $tableName = $this->_statement()->escapeTableName($table);
        $drop = array_reverse($drop);
        $indexFilter = fn(IndexDto $index) => $index->type === 'INDEX';
        $clauseFilter = fn(IndexDto $index) => $index->type !== 'INDEX';

        $dropQueries = array_map(fn(IndexDto $index) => 'DROP INDEX ' .
            $this->_statement()->escapeId($index->name), array_filter($drop, $indexFilter));
        $alterQueries = array_map(function(IndexDto $index) use($table, $tableName) {
            $indexColumns = implode(', ', $index->columns);
            $indexName = $index->name != '' ? $index->name : uniqid("{$table}_");
            $indexName = $this->_statement()->escapeId($indexName);

            return "CREATE INDEX $indexName ON $tableName ($indexColumns)";
        }, array_filter($alter, $indexFilter));

        $dropClauses = array_map(fn(IndexDto $index) => 'DROP CONSTRAINT ' .
            $this->_statement()->escapeId($index->name), array_filter($drop, $clauseFilter));

        $alterClauses = array_map(function(IndexDto $index) use($table) {
            $indexColumns = implode(', ', $index->columns);
            $indexName = $index->name != '' ? $index->name : uniqid("{$table}_");
            $indexName = $this->_statement()->escapeId($indexName);
            $indexType = $index->type === 'PRIMARY' ? 'PRIMARY KEY' : $index->type;
            //! descending UNIQUE indexes results in syntax error
            $clause = $index->name === '' ? 'ADD' : "ADD CONSTRAINT $indexName";

            return "$clause $indexType ($indexColumns)";
        }, array_filter($alter, $clauseFilter));

        $clauses = [...$dropClauses, ...$alterClauses];
        return count($clauses) === 0 ? [...$dropQueries, ...$alterQueries,] : [
            "ALTER TABLE $tableName " . implode(', ', $clauses),
            ...$dropQueries,
            ...$alterQueries,
        ];
    }
}
