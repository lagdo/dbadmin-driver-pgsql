<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Dto\UpsertDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractQuery;
use stdClass;

use function array_keys;
use function array_map;
use function array_slice;
use function implode;
use function preg_match;

class Query extends AbstractQuery
{
    /**
     * @inheritDoc
     */
    public function limitToOne(string $table, string $query, string $where): string
    {
        return preg_match('~^INTO~', $query) ?
            $this->getLimitClause($query, $where, 1, 0) :
            " $query" . ($this->_engine()->isView($this->_engine()->tableStatusOrName($table)) ?
                $where : " WHERE ctid = (SELECT ctid FROM " .
                    $this->_statement()->escapeTableName($table) . $where . ' LIMIT 1)');
    }

    /**
     * @inheritDoc
     */
    public function getTableUpsertQueries(UpsertDto $input): array
    {
        $tableName = $this->_statement()->escapeTableName($input->table);
        $columns = new stdClass();
        $columns->wheres = array_keys($input->keys);
        $columns->values = array_keys($input->values);
        $columns->insert = implode(', ', [...$columns->wheres, ...$columns->values]);

        return array_map(function(...$row) use($tableName, $input, $columns) {
            $updateWheres = array_slice($row, 0, $input->keyCount());
            $updateWheres = $this->getUpdateClause(' AND ', $updateWheres, $columns->wheres);
            $updateValues = array_slice($row, $input->keyCount());
            $updateValues = $this->getUpdateClause(', ', $updateValues, $columns->values);
            $update = "UPDATE $tableName SET $updateValues WHERE $updateWheres";

            $insertValues = implode(', ', $row);
            $insert = "INSERT INTO $tableName({$columns->insert}) VALUES ($insertValues)";

            return ['upsert', [$update, $insert]];
        }, ...$input->rows());
    }
}
