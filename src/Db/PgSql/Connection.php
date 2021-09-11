<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db\PgSql;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;

/**
 * PostgreSQL driver to be used with the pgsql PHP extension.
 */
class Connection extends AbstractConnection
{
    /**
     * Undocumented variable
     *
     * @var string
     */
    public $_string;

    /**
     * Undocumented variable
     *
     * @var string
     */
    public $_database = true;

    /**
     * Undocumented variable
     *
     * @var int
     */
    public $timeout;

    public function _error($errno, $error)
    {
        if ($this->util->iniBool("html_errors")) {
            $error = html_entity_decode(strip_tags($error));
        }
        $error = preg_replace('~^[^:]*: ~', '', $error);
        $this->driver->setError($error);
    }

    /**
     * @inheritDoc
     */
    public function open(string $server, array $options)
    {
        $username = $options['username'];
        $password = $options['password'];

        $database = $this->driver->selectedDatabase();
        set_error_handler(array($this, '_error'));
        $this->_string = "host='" . str_replace(":", "' port='", addcslashes($server, "'\\")) .
            "' user='" . addcslashes($username, "'\\") . "' password='" . addcslashes($password, "'\\") . "'";
        $this->client = @pg_connect("{$this->_string} dbname='" .
            ($database != "" ? addcslashes($database, "'\\") : "postgres") . "'", PGSQL_CONNECT_FORCE_NEW);
        if (!$this->client && $database != "") {
            // try to connect directly with database for performance
            $this->_database = false;
            $this->client = @pg_connect("{$this->_string} dbname='postgres'", PGSQL_CONNECT_FORCE_NEW);
        }
        restore_error_handler();

        if (!$this->client) {
            return false;
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
    public function value(?string $val, TableFieldEntity $field)
    {
        $type = $field->type;
        return ($type == "bytea" && $val !== null ? pg_unescape_bytea($val) : $val);
    }

    /**
     * @inheritDoc
     */
    public function quoteBinary($string)
    {
        return "'" . pg_escape_bytea($this->client, $string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function selectDatabase($database)
    {
        if ($database == $this->driver->selectedDatabase()) {
            return $this->_database;
        }
        $client = @pg_connect("{$this->_string} dbname='" . addcslashes($database, "'\\") . "'", PGSQL_CONNECT_FORCE_NEW);
        if ($client) {
            $this->client = $client;
        }
        return $client;
    }

    /**
     * @inheritDoc
     */
    public function close()
    {
        $this->client = @pg_connect("{$this->_string} dbname='postgres'");
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        $result = @pg_query($this->client, $query);
        $this->driver->setError();
        if (!$result) {
            $this->driver->setError(pg_last_error($this->client));
            $return = false;
        } elseif (!pg_num_fields($result)) {
            $this->driver->setAffectedRows(pg_affected_rows($result));
            $return = true;
        } else {
            $return = new Statement($result);
        }
        if ($this->timeout) {
            $this->timeout = 0;
            $this->query("RESET statement_timeout");
        }
        return $return;
    }

    /**
     * @inheritDoc
     */
    public function multiQuery($query)
    {
        return $this->result = $this->query($query);
    }

    /**
     * @inheritDoc
     */
    public function storedResult()
    {
        return $this->result;
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
    public function result(string $query, int $field = 0)
    {
        $result = $this->query($query);
        if (!$result || !$result->numRows) {
            return false;
        }
        return pg_fetch_result($result->result, 0, $field);
    }

    /**
     * @inheritDoc
     */
    public function warnings()
    {
        // second parameter is available since PHP 7.1.0
        return $this->util->html(pg_last_notice($this->client));
    }
}
