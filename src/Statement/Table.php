<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\PgSql\Traits\TableTrait;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDdDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ForeignKeyDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDdDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function array_filter;
use function array_keys;
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
    use TableTrait;

    /**
     * @inheritDoc
     */
    protected function getColumnModifier(ColumnDdDto $input, TableDdDto $table): string
    {
        return '';
    }

    /**
     * @param TableDdDto $table
     *
     * @return array<string>
     */
    private function getRemovedAutoIncrementQueries(TableDdDto $table): array
    {
        if (!$table->autoIncrementChanged() || !$table->autoIncrementRemoved()) {
            return [];
        }

        $input = $table->removedAutoIncrementInput;
        // Use the previous table and column names.
        $sequenceName = "{$table->statusName()}_{$input->column->name}_seq";
        $sequenceName = $this->_statement()->escapeId($sequenceName);
        $tableName = $this->_statement()->escapeTableName($table->statusName());
        $columnName = $this->_statement()->escapeId($input->column->name);

        return [
            "ALTER TABLE $tableName ALTER $columnName DROP DEFAULT",
            "DROP SEQUENCE IF EXISTS $sequenceName",
        ];
    }

    /**
     * @param TableDdDto $table
     *
     * @return array<string>
     */
    private function getAddedAutoIncrementQueries(TableDdDto $table): array
    {
        if (!$table->autoIncrementChanged() || !$table->autoIncrementAdded()) {
            return [];
        }

        $input = $table->addedAutoIncrementInput;
        $sequenceName = "{$table->name}_{$input->name}_seq";
        $quotedSequenceName = $this->_engine()->quote($sequenceName);
        $sequenceName = $this->_statement()->escapeId($sequenceName);
        // Use the current table and column names.
        $tableName = $this->_statement()->escapeTableName($table->name);
        $columnName = $this->_statement()->escapeId($input->name);

        // Empty for a create table query or a new column in an alter table query.
        $queries = $input->column->name === '' ? [] : [
            "CREATE SEQUENCE IF NOT EXISTS $sequenceName OWNED BY $tableName.$columnName",
            "ALTER TABLE $tableName ALTER $columnName SET DEFAULT nextval($quotedSequenceName)",
        ];
        if ($table->hasAutoIncrement()) {
            $queries[] = "SELECT setval($quotedSequenceName, {$table->autoIncrement})";
        }

        return $queries;
    }

    /**
     * @param TableDdDto $table
     *
     * @return array<string>
     */
    private function getAutoIncrementValueQueries(TableDdDto $table): array
    {
        if (!$table->autoIncrementChanged() || !$table->autoIncrementValueChanged()) {
            return [];
        }

        $sequenceName = $this->getSequenceName($table->autoIncrementColumn);
        if ($sequenceName === '') {
            return [];
        }

        $quotedSequenceName = $this->_engine()->quote($sequenceName);
        return [
            "SELECT setval($quotedSequenceName, {$table->autoIncrement})",
        ];
    }

    /**
     * @param TableDdDto $table
     *
     * @return array
     */
    private function getAutoIncrementQueries(TableDdDto $table): array
    {
        // This function MUST be called before processing the AI columns.
        if (!$table->autoIncrementChanged()) {
            return [];
        }

        return [
            // Create a new sequence.
            ...$this->getAddedAutoIncrementQueries($table),
            // Change the current auto increment value.
            ...$this->getAutoIncrementValueQueries($table),
        ];
    }

    /**
     * @param TableDdDto $table
     * @param array<ColumnDdDto> $inputs
     *
     * @return array<string>
     */
    private function getTableCommentQueries(TableDdDto $table, array $inputs): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);
        $filter = fn(ColumnDdDto $input) => $input->hasComment();
        $columnQueries = array_map(function(ColumnDdDto $input) use($tableName) {
            $columnName = $this->_statement()->escapeTableName($input->name);
            $comment = $this->_engine()->quote($input->comment);
            return "COMMENT ON COLUMN $tableName.$columnName IS $comment";
        }, array_filter($inputs, $filter));

        $tableQueries = $table->hasComment() ? [
            "COMMENT ON TABLE {$tableName} IS " . $this->_engine()->quote($table->comment),
        ] : [];

        return [...$columnQueries, ...$tableQueries];
    }

    /**
     * @param array<ColumnDdDto> $inputs
     * @param TableDdDto $table
     * @param string $prefix
     *
     * @return array<string>
     */
    private function getAddColumnClauses(array $inputs, TableDdDto $table, string $prefix = ''): array
    {
        $clauses = array_reduce($inputs, fn(array $clauses, ColumnDdDto $input) => [
            ...$clauses,
            $prefix . $this->getAddColumnClause($input, $table),
        ], []);

        if ($table->primaryKeyColumnCount() > 0) {
            $clauses[] = $prefix . $table->primaryKeyClause($this->_statement()->escapeId(...));
        }

        return $clauses;
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        $table->setupAutoIncrement();

        $tableName = $this->_statement()->escapeTableName($table->name);
        $columns = $table->addedColumns();
        // Tables columns
        $clauses = implode(",\n  ", [
            ...$this->getAddColumnClauses($columns, $table),
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ]);

        return [
            "CREATE TABLE $tableName(\n  $clauses\n)",
            ...$this->getAutoIncrementQueries($table),
            ...$this->getTableCommentQueries($table, $columns),
        ];
    }

    /**
     * @param ColumnDdDto $input
     *
     * @return string
     */
    private function getColumnDefaultClause(ColumnDdDto $input): string
    {
        if ($input->default === null) {
            return "DROP DEFAULT"; //! change to DROP EXPRESSION with generated columns
        }

        $regex = '~GENERATED ALWAYS(.*) STORED~';
        return 'SET ' . preg_replace($regex, 'EXPRESSION\1', $input->default);
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getEditColumnClauses(TableAlterDto $table): array
    {
        $columnCb = function(array $clauses, ColumnDdDto $input) use($table) {
            // These queries are execued before the columns rename.
            // They must then use the current column names.
            $columnName =  $this->_statement()->escapeId($input->column->name);

            if ($input->typeChanged()) {
                $type = $this->_statement()->getColumnType($input->typeColumn ?? $input);
                $clauses[] = "ALTER $columnName TYPE $type";
            }
            if ($input->defaultChanged() && !$input->autoIncrement) {
                $defaultValue = $this->getColumnDefaultClause($input);
                $clauses[] = "ALTER $columnName $defaultValue";
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
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        $table->setupAutoIncrement();

        $tableName = $this->_statement()->escapeTableName($table->name);

        $tableQueries = [];
        if ($table->nameChanged()) {
            $currTableName = $this->_statement()->escapeTableName($table->status->name);
            $tableQueries[] = "ALTER TABLE $currTableName RENAME TO $tableName";
        }

        $tableClauses =  [
            ...$this->getAddColumnClauses($table->addedColumns(), $table, 'ADD '),
            ...$this->getEditColumnClauses($table),
            ...$this->getDropColumnClauses($table),
            ...$this->getForeignKeyClauses($table, 'ADD '),
        ];
        if (count($tableClauses) > 0) {
            $tableQueries[] = "ALTER TABLE $tableName\n  " . implode(",\n  ", $tableClauses);
        }

        $renameColumnFilter = fn(ColumnDdDto $input) => $input->nameChanged();
        $renameColumnsQueries = array_map(function(ColumnDdDto $input) use($tableName) {
            $currName = $this->_statement()->escapeId($input->column->name);
            $newName = $this->_statement()->escapeId($input->name);

            return "ALTER TABLE $tableName RENAME $currName TO $newName";
        }, array_filter($table->editedColumns(), $renameColumnFilter));
        // Using array_values() is important for the final merge.
        $renameColumnsQueries = array_values($renameColumnsQueries);

        return [
            // Drop the current sequence.
            ...$this->getRemovedAutoIncrementQueries($table),
            ...$tableQueries,
            ...$renameColumnsQueries,
            ...$this->getAutoIncrementQueries($table),
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

            return "ALTER TABLE ONLY $tableSchema.$tableName
ADD CONSTRAINT $constraint {$foreignKey->definition} $deferrable;";
        }, $foreignKeys, array_values($foreignKeys));
    }

    /**
     * @param array<ColumnDto> $columns
     * @param boolean $autoIncrement
     * @param string $style
     *
     * @return array
     */
    private function getSequenceQueries(array $columns, bool $autoIncrement, string $style): array
    {
        $queries = [];

        // Columns definitions
        foreach ($columns as $column) {
            $default = $column->hasDefault(true) ? $column->default : '';
            // sequences for columns
            if (preg_match('~nextval\(\'([^\']+)\'\)~', $default, $matches)) {
                $sequenceName = $matches[1];
                $quotedName = $this->_engine()->quote($sequenceName);
                $rows = $this->_engine()->rows($this->_engine()->minVersion(10) ?
                    "SELECT *, cache_size AS cache_value FROM pg_sequences
WHERE schemaname = current_schema() AND sequencename = $quotedName" :
                    "SELECT * FROM $sequenceName");
                $sequence = reset($rows);
                if ($style == "DROP+CREATE") {
                    $queries[] = "DROP SEQUENCE IF EXISTS $sequenceName;";
                }

                $incrementBy = $sequence['increment_by'];
                $minValue = $sequence['min_value'];
                $maxValue = $sequence['max_value'];
                $lastValue = !($autoIncrement && $sequence['last_value']) ? '' :
                    ' START ' . ((int)$sequence['last_value'] + 1);
                $cacheValue = $sequence['cache_value'];
                $queries[] = "CREATE SEQUENCE $sequenceName INCREMENT $incrementBy
MINVALUE $minValue MAXVALUE $maxValue$lastValue CACHE $cacheValue;";
                $queries[] = ''; // Insert an empty line after each sequence.
            }
        }

        return $queries;
    }

    /**
     * @param array<ColumnDto> $columns
     * @param TableDto $status
     *
     * @return array
     */
    private function getCommentQueries(array $columns, TableDto $status): array
    {
        $table = $this->_statement()->escapeId($status->schema) .
            '.' . $this->_statement()->escapeId($status->name);

        // Comments for table & columns
        $queries = [];
        if ($status->comment !== null) {
            $comment = $this->_engine()->quote($status->comment);
            $queries[] = "\nCOMMENT ON TABLE $table IS $comment;";
        }

        $commentColumns = array_filter($columns,
            fn(ColumnDto $column) => $column->comment !== null);
        $commentQueries = array_map(function(ColumnDto $column, string $name) use($table) {
            $name = $this->_statement()->escapeId($name);
            $comment = $this->_engine()->quote($column->comment);

            return "\nCOMMENT ON COLUMN $table.$name IS $comment;";
        }, $commentColumns, array_keys($commentColumns));

        return [...$queries, ...$commentQueries];
    }

    /**
     * @param array<ColumnDto> $columns
     * @param TableDto $status
     *
     * @return array
     */
    private function getTableQueries(array $columns, TableDto $status): array
    {
        $table = $status->name;
        $escape = $this->_statement()->escapeId(...);

        // From pgsql.inc.php
        // Columns definitions
        $columnClauses = array_map(fn(ColumnDto $column) =>
            $escape($column->name) . ' ' . $column->fullType .
                $this->getDefaultValueClause($column) .
                ($column->nullable ? "" : " NOT NULL"), $columns);

        $indexes = $this->_engine()->indexes($table);
        ksort($indexes);
        // Primary + unique keys
        $primaryIndexName = '';
        $primaryIndexes = array_filter($indexes,
            fn(IndexDto $index) => $index->type === 'PRIMARY');
        $indexClauses = array_map(function(IndexDto $index, string $name)
            use($escape, &$primaryIndexName) {
            $primaryIndexName = $name;
            $indexName = $escape($name);
            $indexColumns = implode(', ', array_map($escape, $index->columns));

            return "CONSTRAINT $indexName PRIMARY KEY ($indexColumns)";
        }, $primaryIndexes, array_keys($primaryIndexes));

        $indexQueries = [];
        // From pgsql.inc.php
        $tableName = $this->_engine()->quote($status->name);
        // Primary keys are not added here.
        $primaryClause = $primaryIndexName === '' ? '' :
            " AND indexname != " . $this->_engine()->quote($primaryIndexName);
        $query = "SELECT indexdef FROM pg_catalog.pg_indexes
WHERE schemaname = current_schema() AND tablename = $tableName $primaryClause";
        // Indexes after table definition
        foreach ($this->_engine()->rows($query) as $row) {
            $indexQueries[] = ''; // Insert an empty line
            $indexQueries[] = $row['indexdef'] . ';';
        }

        // Constraints
        $constraints = $this->_engine()->checkConstraints($status);
        $constraintClauses = array_map(fn(string $source, string $name) =>
            "CONSTRAINT " . $this->_statement()->escapeId($name) .
            " CHECK $source", $constraints, array_keys($constraints));

        // Partitions
        $partition = $this->_engine()->partitionsInfo($table);
        $tableName = $this->_statement()->escapeId($status->schema) .
            '.' . $this->_statement()->escapeId($table);
        $tableQuery = "CREATE TABLE $tableName (
    " . implode(",\n    ", [...$columnClauses, ...$indexClauses, ...$constraintClauses]) . "
)" . (!$partition ? '' :"
PARTITION BY {$partition->strategy}({$partition->columns})") . "
WITH (oids = " . ($status->oid ? 'true' : 'false') . ");";

        return [$tableQuery, ...$indexQueries];
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

        $columns = $status->columns();
        if (empty($columns)) {
            return '';
        }

        // Add sequences before table definition
        $queries = [
            ...$this->getSequenceQueries($columns, $autoIncrement, $style),
            ...$this->getTableQueries($columns, $status),
            ...$this->getCommentQueries($columns, $status),
        ];

        return rtrim(implode("\n", $queries), ';');
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
