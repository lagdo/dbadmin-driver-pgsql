<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db\PgSql;

use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\Entity\StatementFieldEntity;

class Statement implements StatementInterface
{
    /**
     * @var resource
     */
    protected $result;

    /**
     * @var int
     */
    protected $offset = 0;

    /**
     * The constructor
     *
     * @param resource $result
     */
    public function __construct($result)
    {
        $this->result = $result;
    }

    /**
     * @inheritDoc
     */
    public function rowCount(): int
    {
        return pg_num_rows($this->result);
    }

    /**
     * @inheritDoc
     */
    public function fetchAssoc(): array|null
    {
        return pg_fetch_assoc($this->result) ?: null;
    }

    /**
     * @inheritDoc
     */
    public function fetchRow(): array|null
    {
        return pg_fetch_row($this->result) ?: null;
    }

    /**
     * @inheritDoc
     */
    public function fetchField(): StatementFieldEntity|null
    {
        $column = $this->offset++;
        // $table = function_exists('pg_field_table') ? pg_field_table($this->result, $column) : '';
        $table = pg_field_table($this->result, $column);
        $name = pg_field_name($this->result, $column);
        $type = pg_field_type($this->result, $column);
        return new StatementFieldEntity($type, $type === "bytea", $name, $name, $table, $table);
    }

    /**
     * The destructor
     */
    public function __destruct()
    {
        pg_free_result($this->result);
    }
}
