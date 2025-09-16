<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db\PgSql;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;
use Lagdo\DbAdmin\Driver\PgSql\Db\ConnectionTrait;

/**
 * PostgreSQL driver to be used with the pgsql PHP extension.
 */
class Connection extends AbstractConnection
{
    use ConnectionTrait;

    /**
     * @var mixed
     */
    public $result;

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
        $username = addcslashes($this->options['username'], "'\\");
        $password = addcslashes($this->options['password'], "'\\");
        $database = ($database) ? addcslashes($database, "'\\") : "postgres";

        $connString = "host='$server' user='$username' password='$password' dbname='$database'";
        $this->client = pg_connect($connString, PGSQL_CONNECT_FORCE_NEW);
        // if (!$this->client && $database != "") {
        //     // try to connect directly with database for performance
        //     $this->_database = false;
        //     $this->client = pg_connect("{$this->_string} dbname='postgres'", PGSQL_CONNECT_FORCE_NEW);
        // }

        if (!$this->client) {
            $this->setError($this->utils->trans->lang('Unable to connect to database server.'));
            return false;
        }

        if ($this->driver->minVersion(9, 0)) {
            if (@pg_query($this->client, "SET application_name = 'Jaxon DbAdmin'") === false) {
                $this->setError(pg_last_error($this->client));
            }
        }
        if (($schema)) {
            if (@pg_query($this->client, "SET search_path TO " . $this->driver->escapeId($schema)) === false) {
                $this->setError(pg_last_error($this->client));
            }
        }
        pg_set_client_encoding($this->client, "UTF8");
        return true;
    }

    /**
     * @inheritDoc
     */
    public function serverInfo()
    {
        $version = pg_version($this->client);
        return $version["server"];
    }

    /**
     * @inheritDoc
     */
    public function quote(string $string)
    {
        return "'" . pg_escape_string($this->client, $string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function value($value, TableFieldEntity $field)
    {
        $type = $field->type;
        return $type == "bytea" && $value !== null ? pg_unescape_bytea($value) : $value;
    }

    /**
     * @inheritDoc
     */
    public function quoteBinary(string $string)
    {
        return "'" . pg_escape_bytea($this->client, $string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function close(): void
    {
        // $this->client = pg_connect("{$this->_string} dbname='postgres'");
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        $result = @pg_query($this->client, $query);
        $this->setError();
        if ($result === false) {
            $this->setError(pg_last_error($this->client));
            $statement = false;
        } elseif (!pg_num_fields($result)) {
            $this->setAffectedRows(pg_affected_rows($result));
            $statement = true;
        } else {
            $statement = new Statement($result);
        }
        if ($this->timeout) {
            $this->timeout = 0;
            $this->query("RESET statement_timeout");
        }
        return $statement;
    }

    /**
     * @inheritDoc
     */
    public function multiQuery(string $query)
    {
        $this->statement = $this->query($query);
        return $this->statement !== false;
    }

    /**
     * @inheritDoc
     */
    public function storedResult()
    {
        return $this->statement;
    }

    /**
     * @inheritDoc
     */
    public function nextResult()
    {
        // PgSQL extension doesn't support multiple results
        return false;
    }

    /**
     * @inheritDoc
     */
    protected function warnings(): string
    {
        // second parameter is available since PHP 7.1.0
        return $this->utils->str->html(pg_last_notice($this->client));
    }
}
