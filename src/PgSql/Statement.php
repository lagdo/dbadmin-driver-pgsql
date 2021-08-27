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
    public $_result;

    /**
     * Undocumented variable
     *
     * @var int
     */
    public $_offset = 0;

    /**
     * Undocumented variable
     *
     * @var int
     */
    public $num_rows;

    public function __construct($result)
    {
        $this->_result = $result;
        $this->num_rows = pg_num_rows($result);
    }

    public function fetch_assoc()
    {
        return pg_fetch_assoc($this->_result);
    }

    public function fetch_row()
    {
        return pg_fetch_row($this->_result);
    }

    public function fetch_field()
    {
        $column = $this->_offset++;
        $return = new stdClass;
        if (function_exists('pg_field_table')) {
            $return->orgtable = pg_field_table($this->_result, $column);
        }
        $return->name = pg_field_name($this->_result, $column);
        $return->orgname = $return->name;
        $return->type = pg_field_type($this->_result, $column);
        $return->charsetnr = ($return->type == "bytea" ? 63 : 0); // 63 - binary
        return $return;
    }

    public function __destruct()
    {
        pg_free_result($this->_result);
    }
}
