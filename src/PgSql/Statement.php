<?php

namespace Lagdo\DbAdmin\Driver\PgSql\PgSql;

use stdClass;

class Statement
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

    public function __construct($result)
    {
        $this->result = $result;
        $this->numRows = pg_num_rows($result);
    }

    public function fetch_assoc()
    {
        return pg_fetch_assoc($this->result);
    }

    public function fetch_row()
    {
        return pg_fetch_row($this->result);
    }

    public function fetch_field()
    {
        $column = $this->offset++;
        $return = new stdClass;
        if (function_exists('pg_field_table')) {
            $return->orgtable = pg_field_table($this->result, $column);
        }
        $return->name = pg_field_name($this->result, $column);
        $return->orgname = $return->name;
        $return->type = pg_field_type($this->result, $column);
        $return->charsetnr = ($return->type == "bytea" ? 63 : 0); // 63 - binary
        return $return;
    }

    public function __destruct()
    {
        pg_free_result($this->result);
    }
}
