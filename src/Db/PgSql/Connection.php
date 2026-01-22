<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db\PgSql;

use Lagdo\DbAdmin\Driver\Db\AbstractConnection;
use Lagdo\DbAdmin\Driver\Db\PreparedStatement;
use Lagdo\DbAdmin\Driver\Db\StatementInterface;
use Lagdo\DbAdmin\Driver\Dto\TableFieldDto;
use Lagdo\DbAdmin\Driver\PgSql\Db\Traits\ConnectionTrait;

use function addcslashes;
use function pg_affected_rows;
use function pg_connect;
use function pg_escape_bytea;
use function pg_escape_string;
use function pg_last_error;
use function pg_num_fields;
use function pg_query;
use function pg_set_client_encoding;
use function pg_unescape_bytea;
use function pg_version;
use function uniqid;
use function pg_prepare;
use function pg_execute;
use function pg_last_notice;

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
        $server = $this->_server($this->options('server'));
        $username = addcslashes($this->options['username'], "'\\");
        $password = addcslashes($this->options['password'], "'\\");
        $database = $this->_database($database);

        $connString = "host='$server' user='$username' password='$password' " .
            "dbname='$database' connect_timeout=2";
        $this->client = @pg_connect($connString, PGSQL_CONNECT_FORCE_NEW);
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
    public function serverInfo(): string
    {
        if (!$this->client) {
            return '';
        }
        $version = pg_version($this->client);
        return $version["server"];
    }

    /**
     * @inheritDoc
     */
    public function quote(string $string): string
    {
        return "'" . pg_escape_string($this->client, $string) . "'";
    }

    /**
     * @inheritDoc
     */
    public function value($value, TableFieldDto $field): mixed
    {
        $type = $field->type;
        return $type == "bytea" && $value !== null ? pg_unescape_bytea($value) : $value;
    }

    /**
     * @inheritDoc
     */
    public function quoteBinary(string $string): string
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
    public function query(string $query, bool $unbuffered = false): StatementInterface|bool
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
    public function multiQuery(string $query): bool
    {
        $this->statement = $this->query($query);
        return $this->statement !== false;
    }

    /**
     * @inheritDoc
     */
    public function storedResult(): StatementInterface|bool
    {
        return $this->statement;
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
    public function prepareStatement(string $query): PreparedStatement
    {
        // PgSQL extension uses '$n' as placeholders for query params.
        $replace = fn($name, $pos) => '$' . $pos;
        [$params, $query] = $this->getPreparedParams($query, $replace);
        // The prepared statement needs a unique name.
        $name = uniqid('st');
        $statement = pg_prepare($this->client, $name, $query);
        return new PreparedStatement($query, $statement, $params, $name);
    }

    /**
     * @inheritDoc
     */
    public function executeStatement(PreparedStatement $statement,
        array $values): ?StatementInterface
    {
        if (!$statement->prepared()) {
            return null;
        }

        $values = $statement->paramValues($values, false);
        $result = pg_execute($this->client, $statement->name(), $values);
        return !$result ? null : new Statement($result);
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
