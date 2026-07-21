<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Exception\DriverException;
use Lagdo\DbAdmin\Driver\PgSql\Traits\TableTrait;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDdDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ForeignKeyDdDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableCreateDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDdDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractTable;

use function array_filter;
use function array_map;
use function array_reduce;
use function array_values;
use function count;
use function implode;
use function preg_replace;

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
     * @param string $prefix
     *
     * @return array<string>
     */
    private function getAddColumnClauses(TableDdDto $table, string $prefix = ''): array
    {
        return array_map(fn(ColumnDdDto $input) => $prefix .
            $this->getAddColumnClause($input, $table), $table->addedColumns());
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
            $columnName =  $this->_statement()->escapeId($input->statusName());

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
     * @param TableDdDto $table
     * @param array<ColumnDdDto> $inputs
     *
     * @return array<string>
     */
    private function getTableCommentQueries(TableDdDto $table, array $inputs): array
    {
        $tableName = $this->_statement()->escapeTableName($table->name);
        // Columns queries.
        $filter = fn(ColumnDdDto $input) => $input->hasComment();
        $queries = array_map(function(ColumnDdDto $input) use($tableName) {
            $columnName = $this->_statement()->escapeTableName($input->name);
            $comment = $this->_engine()->quote($input->comment);
            return "COMMENT ON COLUMN $tableName.$columnName IS $comment";
        }, array_filter($inputs, $filter));

        // Table query.
        if ($table->hasComment()) {
            $comment = $this->_engine()->quote($table->comment);
            $queries[] = "COMMENT ON TABLE {$tableName} IS $comment";
        }

        return $queries;
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableQueries(TableCreateDto $table): array
    {
        if ($table->name === '') {
            throw new DriverException($this->_utils()->lang('The table name must be defined.'));
        }

        $tableName = $this->_statement()->escapeTableName($table->name);
        $clauses = implode(",\n  ", [
            ...$this->getAddColumnClauses($table),
            ...$this->getCreatePrimaryKeyClause($table),
            ...$this->getCreateForeignKeyClauses($table),
        ]);
        return [
            "CREATE TABLE $tableName (\n  $clauses\n)",
            ...$this->getTableCommentQueries($table, $table->addedColumns()),
        ];
    }

    /**
     * @inheritDoc
     */
    protected function getDropPrimaryKeyClause(TableAlterDto $table): array
    {
        if (!$this->primaryKeyChanged($table)) {
            return [];
        }

        // Use the previous table name.
        $tableName = $table->statusName();
        // Find the primary key index.
        $indexes = array_filter($this->_engine()->indexes($tableName),
            fn(IndexDto $index) => $index->type === 'PRIMARY');
        $primaryIndex = array_values($indexes)[0] ?? null;
        if ($primaryIndex !== null) {
            $constraintName = $this->_statement()->escapeId($primaryIndex->name);
            return ["DROP CONSTRAINT $constraintName"];
        }

        $constraintName = "{$tableName}_pkey";
        $constraintName = $this->_statement()->escapeId($constraintName);
        return ["DROP CONSTRAINT $constraintName"];
    }

    /**
    * @inheritDoc
     */
    protected function getDeleteForeignKeyClauses(TableAlterDto $table): array
    {
        $filter = fn(ForeignKeyDdDto $foreignKey) =>
            $foreignKey->edited() || $foreignKey->dropped();
        $foreignKeys = array_filter($table->foreignKeys, $filter);
        $formatter = fn(ForeignKeyDdDto $foreignKey) =>
            'DROP CONSTRAINT ' . $this->_statement()->escapeId($foreignKey->name);
        return array_map($formatter, $foreignKeys);
    }

    /**
     * @param TableDdDto $table
     *
     * @return array<string>
     */
    private function getRemovedAutoIncrementQueries(TableDdDto $table): array
    {
        if (!$table->autoIncrementRemoved()) {
            return [];
        }

        $input = $table->removedAutoIncrementInput;
        // Use the previous table and column names.
        $sequenceName = "{$table->statusName()}_{$input->statusName()}_seq";
        $sequenceName = $this->_statement()->escapeId($sequenceName);
        $tableName = $this->_statement()->escapeTableName($table->statusName());
        $columnName = $this->_statement()->escapeId($input->statusName());

        return [
            "ALTER TABLE $tableName ALTER $columnName DROP DEFAULT",
            "DROP SEQUENCE IF EXISTS $sequenceName",
        ];
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getRenameQueries(TableAlterDto $table): array
    {
        $tableName = $this->_statement()->escapeTableName($table->statusName());

        // Rename the columns.
        $renameColumnFilter = fn(ColumnDdDto $input) => $input->nameChanged();
        // Using array_values() is important for the final merge.
        $queries = array_values(array_map(function(ColumnDdDto $input) use($tableName) {
            $currName = $this->_statement()->escapeId($input->statusName());
            $newName = $this->_statement()->escapeId($input->name);

            return "ALTER TABLE $tableName RENAME $currName TO $newName";
        }, array_filter($table->editedColumns(), $renameColumnFilter)));

        // Rename the table.
        if ($table->nameChanged()) {
            $newName = $this->_statement()->escapeTableName($table->name);
            $queries[] = "ALTER TABLE $tableName RENAME TO $newName";
        }

        return $queries;
    }

    /**
     * @param TableDdDto $table
     *
     * @return array<string>
     */
    private function getCreateAutoIncrementQueries(TableDdDto $table): array
    {
        if (!$table->autoIncrementAdded()) {
            return [];
        }

        $input = $table->addedAutoIncrementInput;
        // Empty for a new column in an alter table query.
        if ($input->added()) {
            return [];
        }

        $sequenceName = "{$table->name}_{$input->name}_seq";
        $quotedSequenceName = $this->_engine()->quote($sequenceName);
        $sequenceName = $this->_statement()->escapeId($sequenceName);
        // Use the current table and column names.
        $tableName = $this->_statement()->escapeTableName($table->name);
        $columnName = $this->_statement()->escapeId($input->name);

        return [
            "CREATE SEQUENCE IF NOT EXISTS $sequenceName OWNED BY $tableName.$columnName",
            "ALTER TABLE $tableName ALTER $columnName SET DEFAULT nextval($quotedSequenceName)",
        ];
    }

    /**
     * @param TableDdDto $table
     *
     * @return array<string>
     */
    private function getAutoIncrementValueQueries(TableDdDto $table): array
    {
        $sequenceName = match(true) {
            !$table->hasAutoIncrement() => '',
            $table->addedAutoIncrementInput !== null =>
                "{$table->name}_{$table->addedAutoIncrementInput->name}_seq",
            ($input = $table->autoIncrementInput()) !== null =>
                $this->getSequenceName($input->column),
            default => '',
        };
        if ($sequenceName === '') {
            return [];
        }

        $sequenceName = $this->_engine()->quote($sequenceName);
        return ["SELECT setval($sequenceName, {$table->autoIncrement})"];
    }

    /**
     * @param TableAlterDto $table
     *
     * @return array
     */
    private function getAlterTableQuery(TableAlterDto $table): array
    {
        $clauses = [
            ...$this->getAddColumnClauses($table, 'ADD '),
            ...$this->getEditColumnClauses($table),
            ...$this->getDropColumnClauses($table),
            ...$this->getCreatePrimaryKeyClause($table, 'ADD '),
            ...$this->getCreateForeignKeyClauses($table, 'ADD '),
        ];
        if (count($clauses) === 0) {
            return [];
        }

        $tableName = $this->_statement()->escapeTableName($table->name);
        return ["ALTER TABLE $tableName\n  " . implode(",\n  ", $clauses)];
    }

    /**
     * @inheritDoc
     */
    public function getAlterTableQueries(TableAlterDto $table): array
    {
        if ($table->name === '') {
            throw new DriverException($this->_utils()->lang('The table name must be defined.'));
        }

        return [
            ...$this->getDropConstraintsQuery($table),
            ...$this->getRemovedAutoIncrementQueries($table),
            ...$this->getRenameQueries($table),
            ...$this->getAlterTableQuery($table),
            ...$this->getCreateAutoIncrementQueries($table),
            ...$this->getAutoIncrementValueQueries($table),
            ...$this->getTableCommentQueries($table, [
                ...$table->addedColumns(),
                ...$table->editedColumns(),
            ]),
        ];
    }
}
