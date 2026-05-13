<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Connection\PgSql;

use Lagdo\DbAdmin\Driver\Sql\Connection\QueryResultInterface;
use Lagdo\DbAdmin\Driver\Sql\Dto\ResultColumnDto;
use PgSql\Result as PgResult;

use function is_bool;
use function pg_fetch_assoc;
use function pg_fetch_row;
use function pg_field_name;
use function pg_field_table;
use function pg_field_type;
use function pg_free_result;
use function pg_num_rows;

class QueryResult implements QueryResultInterface
{
    /**
     * @var PgResult|null
     */
    private PgResult|null $result;

    /**
     * @var bool
     */
    private bool $hasError;

    /**
     * @var int
     */
    private int $columnOffset = 0;

    /**
     * The constructor
     *
     * @param PgResult|bool $result
     */
    public function __construct(PgResult|bool $result)
    {
        $isBool = is_bool($result);
        $this->hasError = $isBool ? !$result : false;
        $this->result = $isBool ? null : $result;
    }

    /**
     * @inheritDoc
     */
    public function hasError(): bool
    {
        return $this->hasError;
    }

    /**
     * @inheritDoc
     */
    public function hasRowset(): bool
    {
        return $this->result !== null;
    }

    /**
     * @inheritDoc
     */
    public function rowCount(): int
    {
        return $this->result === null ? 0 : pg_num_rows($this->result);
    }

    /**
     * @inheritDoc
     */
    public function fetchAssoc(): array|null
    {
        return $this->result === null ? null : (pg_fetch_assoc($this->result) ?: null);
    }

    /**
     * @inheritDoc
     */
    public function fetchRow(): array|null
    {
        return $this->result === null ? null : (pg_fetch_row($this->result) ?: null);
    }

    /**
     * @inheritDoc
     */
    public function fetchColumn(): ResultColumnDto|null
    {
        if ($this->result === null) {
            return null;
        }

        $column = $this->columnOffset++;
        $table = pg_field_table($this->result, $column) ?: '';
        $name = pg_field_name($this->result, $column);
        $type = pg_field_type($this->result, $column);

        return new ResultColumnDto($type, $type === "bytea", $name, $name, $table, $table);
    }

    /**
     * The destructor
     */
    public function __destruct()
    {
        if ($this->result !== null) {
            pg_free_result($this->result);
            $this->result = null;
        }
    }
}
