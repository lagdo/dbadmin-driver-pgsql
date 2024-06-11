<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db\Pdo;

use Lagdo\DbAdmin\Driver\Db\Pdo\Connection as PdoConnection;

/**
 * PostgreSQL driver to be used with the pdo_pgsql PHP extension.
 */
class Connection extends PdoConnection
{
    /**
     * @var int
     */
    public $timeout;

    /**
    * @inheritDoc
    */
    public function open(string $database, string $schema = '')
    {
        $server = str_replace(":", "' port='", addcslashes($this->driver->options('server'), "'\\"));
        $options = $this->driver->options();
        $username = $options['username'];
        $password = $options['password'];
        $database = ($database) ? addcslashes($database, "'\\") : "postgres";
        if (!$password) {
            $password = '';
        }

        //! client_encoding is supported since 9.1 but we can't yet use min_version here
        $this->dsn("pgsql:host='$server' client_encoding=utf8 dbname='$database'", $username, $password);
        if ($this->driver->minVersion(9, 0)) {
            $this->query("SET application_name = 'Jaxon DbAdmin'");
        }
        if (($schema)) {
            $this->query("SET search_path TO " . $this->driver->escapeId($schema));
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function quoteBinary(string $string)
    {
        return $this->quote($string);
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        $result = parent::query($query, $unbuffered);
        if ($this->timeout) {
            $this->timeout = 0;
            parent::query("RESET statement_timeout");
        }
        return $result;
    }

    /**
     * @inheritDoc
     */
    public function warnings()
    {
        return ''; // not implemented in PDO_PgSQL as of PHP 7.2.1
    }
}
