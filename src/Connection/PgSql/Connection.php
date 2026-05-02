<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Connection\PgSql;

use Lagdo\DbAdmin\Driver\PgSql\Connection\Traits\ConnectionTrait;
use Lagdo\DbAdmin\Driver\Sql\Connection\AbstractConnection;
use Lagdo\DbAdmin\Driver\Sql\Connection\PreparedStatement;
use Lagdo\DbAdmin\Driver\Sql\Connection\QueryResultInterface;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use PgSql\Connection as PgConnection;
use PgSql\Result as PgResult;

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
use function pg_prepare;
use function pg_execute;
use function sprintf;
use function uniqid;

/**
 * PostgreSQL driver to be used with the pgsql PHP extension.
 */
class Connection extends AbstractConnection
{
    use ConnectionTrait;

    /**
     * The client object used to query the database driver
     *
     * @var PgConnection|bool
     */
    protected PgConnection|bool $client;

    /**
     * @var int
     */
    public $timeout = 0;

    /**
     * @inheritDoc
     */
    public function open(string $database, string $schema = ''): bool
    {
        $server = $this->_server($this->options('server'));
        $username = addcslashes($this->options['username'], "'\\");
        $password = addcslashes($this->options['password'], "'\\");
        $database = $this->_database($database);

        $connString = sprintf("host='%s' user='%s' password='%s' dbname='%s' connect_timeout=2",
            $server, $username, $password, $database);
        $this->client = @pg_connect($connString, PGSQL_CONNECT_FORCE_NEW);
        // if (!$this->client && $database != "") {
        //     // try to connect directly with database for performance
        //     $this->_database = false;
        //     $this->client = pg_connect("{$this->_string} dbname='postgres'", PGSQL_CONNECT_FORCE_NEW);
        // }

        if (!$this->client) {
            $this->setError($this->_utils()->lang('Unable to connect to database server.'));
            return false;
        }

        if ($this->_engine()->minVersion(9, 0)) {
            if (@pg_query($this->client, "SET application_name = 'Jaxon DbAdmin'") === false) {
                $this->setError(pg_last_error($this->client));
            }
        }
        if (($schema)) {
            if (@pg_query($this->client, "SET search_path TO " . $this->_statement()->escapeId($schema)) === false) {
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
    public function convertValue(mixed $value, ColumnDto $column): mixed
    {
        return $column->type === 'bytea' && $value !== null ? pg_unescape_bytea($value) : $value;
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
     * @param string $query
     *
     * @return QueryResult
     */
    private function execQuery(string $query): QueryResultInterface
    {
        $result = @pg_query($this->client, $query);
        $this->setError();
        if ($result === false) {
            $this->setError(pg_last_error($this->client));
            return new QueryResult(false);
        }

        if (!pg_num_fields($result)) {
            $this->setAffectedRows(pg_affected_rows($result));
            return new QueryResult(true);
        }

        return new QueryResult($result);
    }

    /**
     * @inheritDoc
     */
    public function executeQuery(string $query, bool $unbuffered = false): QueryResultInterface
    {
        $statement = $this->execQuery($query);
        if ($this->timeout > 0) {
            $this->timeout = 0;
            $this->execQuery("RESET statement_timeout");
        }

        return $statement;
    }

    /**
     * @inheritDoc
     */
    public function prepareStatement(string $query): PreparedStatement
    {
        // PgSQL extension uses '$n' as placeholders for query params.
        $replace = fn($name, $pos) => "\${$pos}";
        [$params, $query] = $this->getPreparedParams($query, $replace);
        // The prepared statement needs a unique name.
        $name = uniqid('st');
        $statement = pg_prepare($this->client, $name, $query);
        return new PreparedStatement($statement, $query, $params, $name);
    }

    /**
     * @inheritDoc
     */
    public function executeStatement(PreparedStatement $preparedStatement,
        array $values): QueryResultInterface
    {
        /** @var PgResult|bool */
        $statement = $preparedStatement->statement();
        if (!$statement) {
            $this->setError($this->_utils()->lang($this->statementNotPrepared));
            return new QueryResult(false);
        }

        $values = $preparedStatement->paramValues($values, false);
        $result = pg_execute($this->client, $preparedStatement->name(), $values);
        if ($result === false) {
            $this->setError(pg_last_error($this->client));
        }

        return new QueryResult($result);
    }

    /**
     * @inheritDoc
     */
    public function executeMultiQuery(string $query): QueryResultInterface
    {
        return $this->executeQuery($query);
    }

    /**
     * @inheritDoc
     */
    public function readRowset(QueryResultInterface $result): QueryResultInterface
    {
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
}
