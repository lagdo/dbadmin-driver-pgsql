<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Connection\Pdo;

use Lagdo\DbAdmin\Driver\PgSql\Connection\Traits\ConnectionTrait;
use Lagdo\DbAdmin\Driver\Sql\Connection\Pdo\AbstractConnection;
use Lagdo\DbAdmin\Driver\Sql\Connection\QueryResultInterface;

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
        $server = $this->_server($this->options('server'));
        $username = $this->options['username'];
        $password = $this->options['password'];
        $database = $this->_database($database);
        if (!$password) {
            $password = '';
        }

        //! client_encoding is supported since 9.1 but we can't yet use min_version here
        $dsn = "pgsql:host='$server' client_encoding=utf8 dbname='$database'";
        if (!$this->dsn($dsn, $username, $password)) {
            return false;
        }

        if ($this->_engine()->minVersion(9, 0)) {
            $this->executeQuery("SET application_name = 'Jaxon DbAdmin'");
        }
        if (($schema)) {
            $this->executeQuery("SET search_path TO " . $this->_statement()->escapeId($schema));
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
    public function executeQuery(string $query, bool $unbuffered = false): QueryResultInterface
    {
        $result = parent::executeQuery($query, $unbuffered);
        if ($this->timeout) {
            $this->timeout = 0;
            parent::executeQuery("RESET statement_timeout");
        }
        return $result;
    }

    /**
     * @inheritDoc
     */
    public function nextRowset(QueryResultInterface $result): bool
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
