<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Pdo;

use Lagdo\DbAdmin\Driver\Db\Pdo\Connection as PdoConnection;

/**
 * PostgreSQL driver to be used with the pdo_pgsql PHP extension.
 */
class Connection extends PdoConnection
{
    /**
     * Undocumented variable
     *
     * @var int
     */
    public $timeout;

    /**
    * @inheritDoc
    */
    public function open($server, array $options)
    {
        $username = $options['username'];
        $password = $options['password'];

        $db = $this->server->currentDatabase();
        //! client_encoding is supported since 9.1 but we can't yet use min_version here
        $this->dsn("pgsql:host='" . str_replace(":", "' port='", addcslashes($server, "'\\")) .
            "' client_encoding=utf8 dbname='" .
            ($db != "" ? addcslashes($db, "'\\") : "postgres") . "'", $username, $password);
        //! connect without DB in case of an error
        return true;
    }

    public function selectDatabase($database)
    {
        return ($this->server->currentDatabase() == $database);
    }

    public function quoteBinary($string)
    {
        return $this->quote($string);
    }

    public function query($query, $unbuffered = false)
    {
        $return = parent::query($query, $unbuffered);
        if ($this->timeout) {
            $this->timeout = 0;
            parent::query("RESET statement_timeout");
        }
        return $return;
    }

    public function warnings()
    {
        return ''; // not implemented in PDO_PgSQL as of PHP 7.2.1
    }
}
