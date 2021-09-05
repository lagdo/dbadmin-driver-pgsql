<?php

namespace Lagdo\DbAdmin\Driver\PgSql\PgSql;

use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\Db\StatementField;

use stdClass;

class Statement implements StatementInterface
{
    /**
     * Undocumented variable
     *
     * @var object
     */
    public $result;

    /**
     * Undocumented variable
     *
     * @var int
     */
    public $offset = 0;

    /**
     * Undocumented variable
     *
     * @var int
     */
    public $numRows;

    /**
     * The constructor
     *
     * @param resource $result
     */
    public function __construct($result)
    {
        $this->result = $result;
        $this->numRows = pg_num_rows($result);
    }

    /**
     * @inheritDoc
     */
    public function fetchAssoc()
    {
        return pg_fetch_assoc($this->result);
    }

    /**
     * @inheritDoc
     */
    public function fetchRow()
    {
        return pg_fetch_row($this->result);
    }

    /**
     * @inheritDoc
     */
    public function fetchField()
    {
        $column = $this->offset++;
        // $table = function_exists('pg_field_table') ? pg_field_table($this->result, $column) : '';
        $table = pg_field_table($this->result, $column);
        $name = pg_field_name($this->result, $column);
        $type = pg_field_type($this->result, $column);
        return new StatementField($type, $type === "bytea", $name, $name, $table, $table);
    }

    /**
     * The destructor
     */
    public function __destruct()
    {
        pg_free_result($this->result);
    }
}
