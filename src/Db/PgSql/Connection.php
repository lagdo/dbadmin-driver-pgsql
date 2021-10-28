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
     * @var mixed
     */
    public $result;

    /**
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
    public function open(string $database, string $schema = '')
    {
        $server = str_replace(":", "' port='", addcslashes($this->driver->options('server'), "'\\"));
        $options = $this->driver->options();
        $username = addcslashes($options['username'], "'\\");
        $password = addcslashes($options['password'], "'\\");
        $database = ($database) ? addcslashes($database, "'\\") : "postgres";

        set_error_handler(array($this, '_error'));
        $connString = "host='$server' user='$username' password='$password' dbname='$database'";
        $this->client = pg_connect($connString, PGSQL_CONNECT_FORCE_NEW);
        // if (!$this->client && $database != "") {
        //     // try to connect directly with database for performance
        //     $this->_database = false;
        //     $this->client = pg_connect("{$this->_string} dbname='postgres'", PGSQL_CONNECT_FORCE_NEW);
        // }
        restore_error_handler();

        if (!$this->client) {
            return false;
        }

        if ($this->driver->minVersion(9, 0)) {
            if (pg_query($this->client, "SET application_name = 'Adminer'") === false) {
                $this->driver->setError(pg_last_error($this->client));
            }
        }
        if (($schema)) {
            if (pg_query($this->client, "SET search_path TO " . $this->driver->escapeId($schema)) === false) {
                $this->driver->setError(pg_last_error($this->client));
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
        return ($type == "bytea" && $value !== null ? pg_unescape_bytea($value) : $value);
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
    public function close()
    {
        // $this->client = pg_connect("{$this->_string} dbname='postgres'");
    }

    /**
     * @inheritDoc
     */
    public function query(string $query, bool $unbuffered = false)
    {
        $result = pg_query($this->client, $query);
        $this->driver->setError();
        if ($result === false) {
            $this->driver->setError(pg_last_error($this->client));
            $statement = false;
        } elseif (!pg_num_fields($result)) {
            $this->driver->setAffectedRows(pg_affected_rows($result));
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
    public function multiQuery($query)
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
    public function result(string $query, int $field = -1)
    {
        if ($field < 0) {
            $field = $this->defaultField();
        }
        $result = $this->query($query);
        if ($result === null || !$result->rowCount()) {
            return null;
        }
        // return pg_fetch_result($result->result, 0, $field);
        $row = $result->fetchRow();
        return is_array($row) && count($row) > $field ? $row[$field] : null;
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
