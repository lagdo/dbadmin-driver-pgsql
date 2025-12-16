<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db\Pdo;

use Lagdo\DbAdmin\Driver\Db\Pdo\AbstractConnection;
use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\PgSql\Db\Traits\ConnectionTrait;

/**
 * PostgreSQL driver to be used with the pdo_pgsql PHP extension.
 */
class Connection extends AbstractConnection
{
    use ConnectionTrait;

    /**
     * @var int
     */
    public $timeout;

    /**
    * @inheritDoc
    */
    public function open(string $database, string $schema = ''): bool
    {
        $server = str_replace(":", "' port='", addcslashes($this->options('server'), "'\\"));
        $username = $this->options['username'];
        $password = $this->options['password'];
        $database = !$database ? 'postgres' : addcslashes($database, "'\\");
        if (!$password) {
            $password = '';
        }

        //! client_encoding is supported since 9.1 but we can't yet use min_version here
        $dsn = "pgsql:host='$server' client_encoding=utf8 dbname='$database'";
        if (!$this->dsn($dsn, $username, $password)) {
            return false;
        }

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
    public function quoteBinary(string $string): string
    {
        return $this->quote($string);
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false): StatementInterface|bool
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
    public function nextResult(): mixed
    {
        // PgSQL extension doesn't support multiple results
        return false;
    }

    /**
     * @inheritDoc
     */
    protected function warnings(): string
    {
        return ''; // not implemented in PDO_PgSQL as of PHP 7.2.1
    }
}
