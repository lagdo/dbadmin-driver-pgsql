<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\PgSql\Traits\TableTrait;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnInputDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableAlterDto;

use function array_reduce;
use function preg_replace;

trait AlterTableTrait
{
    use TableTrait;

    /**
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getDisabledAutoIncrementQueries(TableAlterDto $table): array
    {
        $input = $table->disabledAutoIncrementInput;
        // Use the previous table and column names.
        $sequenceName = "{$table->status->name}_{$input->column->name}_seq";
        $sequenceName = $this->_statement()->escapeId($sequenceName);
        $tableName = $this->_statement()->escapeTableName($table->status->name);
        $columnName = $this->_statement()->escapeId($input->column->name);

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
    private function getEnabledAutoIncrementQueries(TableAlterDto $table): array
    {
        $input = $table->enabledAutoIncrementInput;
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
     * @param TableAlterDto $table
     *
     * @return array<string>
     */
    private function getAutoIncrementValueQueries(TableAlterDto $table): array
    {
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
     * @param TableAlterDto $table
     *
     * @return array
     */
    private function getAutoIncrementQueries(TableAlterDto $table): array
    {
        if (!$table->setupAutoIncrement()) {
            // Nothing to do for auto increment.
            return [];
        }

        $queries = [];

        // Drop the current sequence.
        if ($table->autoIncrementDisabled()) {
            $queries = [
                ...$queries,
                ...$this->getDisabledAutoIncrementQueries($table),
            ];
        }
        // Create a new sequence.
        if ($table->autoIncrementEnabled()) {
            $queries = [
                ...$queries,
                ...$this->getEnabledAutoIncrementQueries($table),
            ];
        }
        // Just change the current auto increment value.
        if ($table->autoIncrementValueChanged()) {
            $queries = [
                ...$queries,
                ...$this->getAutoIncrementValueQueries($table),
            ];
        }

        return $queries;
    }

    /**
     * @param ColumnInputDto $input
     *
     * @return string
     */
    private function getColumnDefaultClause(ColumnInputDto $input): string
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
        $columnCb = function(array $clauses, ColumnInputDto $input) use($table) {
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
}
