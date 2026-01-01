<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\AbstractDriver;
use Lagdo\DbAdmin\Driver\Db\AbstractConnection;
use Lagdo\DbAdmin\Driver\Exception\AuthException;

use function array_map;
use function count;
use function extension_loaded;

class Driver extends AbstractDriver
{
    /**
     * @var Db\Server|null
     */
    private Db\Server|null $server = null;

    /**
     * @var Db\Database|null
     */
    private Db\Database|null $database = null;

    /**
     * @var Db\Table|null
     */
    private Db\Table|null $table = null;

    /**
     * @var Db\Query|null
     */
    private Db\Query|null $query = null;

    /**
     * @var Db\Grammar|null
     */
    private Db\Grammar|null $grammar = null;

    /**
     * @return Db\Server
     */
    protected function _server(): Db\Server
    {
        return $this->server ?: $this->server = new Db\Server($this, $this->utils);
    }

    /**
     * @return Db\Database
     */
    protected function _database(): Db\Database
    {
        return $this->database ?: $this->database = new Db\Database($this, $this->utils);
    }

    /**
     * @return Db\Table
     */
    protected function _table(): Db\Table
    {
        return $this->table ?: $this->table = new Db\Table($this, $this->utils);
    }

    /**
     * @return Db\Grammar
     */
    protected function _grammar(): Db\Grammar
    {
        return $this->grammar ?: $this->grammar = new Db\Grammar($this, $this->utils);
    }

    /**
     * @return Db\Query
     */
    protected function _query(): Db\Query
    {
        return $this->query ?: $this->query = new Db\Query($this, $this->utils);
    }

    /**
     * @inheritDoc
     */
    public function name(): string
    {
        return "PostgreSQL";
    }

    /**
     * @inheritDoc
     */
    protected function beforeConnection(): void
    {
        $trans = $this->utils->trans;
        // Init config
        $this->config->jush = 'pgsql';
        $this->config->drivers = ["PgSQL", "PDO_PgSQL"];
        $this->config->types = [ //! arrays
            $trans->lang('Numbers') => ["smallint" => 5, "integer" => 10, "bigint" => 19, "boolean" => 1,
                "numeric" => 0, "real" => 7, "double precision" => 16, "money" => 20],
            $trans->lang('Date and time') => ["date" => 13, "time" => 17, "timestamp" => 20, "timestamptz" => 21, "interval" => 0],
            $trans->lang('Strings') => ["character" => 0, "character varying" => 0, "text" => 0,
                "tsquery" => 0, "tsvector" => 0, "uuid" => 0, "xml" => 0],
            $trans->lang('Binary') => ["bit" => 0, "bit varying" => 0, "bytea" => 0],
            $trans->lang('Network') => ["cidr" => 43, "inet" => 43, "macaddr" => 17, "txid_snapshot" => 0],
            $trans->lang('Geometry') => ["box" => 0, "circle" => 0, "line" => 0, "lseg" => 0,
                "path" => 0, "point" => 0, "polygon" => 0],
        ];
        // $this->config->unsigned = [];
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE",
            "LIKE %%", "ILIKE", "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT ILIKE",
            "NOT IN", "IS NOT NULL", "SQL"]; // no "SQL" to avoid CSRF
        $this->config->functions = ["char_length", "lower", "round", "to_hex", "to_timestamp", "upper"];
        $this->config->grouping = ["avg", "count", "count distinct", "max", "min", "sum"];
        $this->config->insertFunctions = [
            "char" => ["md5"],
            "date|time" => ["now"],
        ];
        $this->config->editFunctions = [
            $this->numberRegex() => ["+", "-"],
            "date|time" => ["+ interval", "- interval"], //! escape
            "char|text" => ["||"],
        ];
        $this->config->features = ['check', 'columns', 'comment', 'database', 'drop_col', 'dump',
            'descidx', 'indexes', 'kill', 'partial_indexes', 'routine', 'scheme', 'sequence',
            'sql', 'table', 'trigger', 'type', 'variables', 'view'];

        // Regex to parse SQL statements in a text
        $this->config->sqlStatementRegex = '\\s*|[\'"]|/\*|-- |$|\$[^$]*\$';
    }

    /**
     * @inheritDoc
     */
    protected function configConnection(): void
    {
        $trans = $this->utils->trans;
        //! get types from current_schemas('t')
        $userTypes = array_map(fn($type) => (int)$type->oid, $this->userTypes(false));
        if (count($userTypes) > 0) {
            $this->config->types[$trans->lang('User types')] = $userTypes;
        }

        if ($this->minVersion(9.2, 0)) {
            $this->config->types[$trans->lang('Strings')]["json"] = 4294967295;
            if ($this->minVersion(9.4, 0)) {
                $this->config->types[$trans->lang('Strings')]["jsonb"] = 4294967295;
            }
        }
        if ($this->minVersion(12, 0)) {
            $this->config->generated = ["STORED"];
        }
        $this->config->partitionBy = ["RANGE", "LIST"];
        // if (!connection()->flavor) {
        //     $this->config->partitionBy[] = "HASH";
        // }

        if ($this->minVersion(9.3)) {
            $this->config->features[] = 'materializedview';
        }
        if ($this->minVersion(11)) {
            $this->config->features[] = 'procedure';
        }
        /*if (connection()->flavor == 'cockroach)*/ {
            $this->config->features[] = 'processlist';
        }
    }

    /**
     * @inheritDoc
     */
    protected function connectionOpened(): void
    {
        $this->_server()->setConnection($this->connection);
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection(array $options): AbstractConnection|null
    {
        $preferPdo = $options['prefer_pdo'] ?? false;
        if (!$preferPdo && extension_loaded("pgsql")) {
            return new Db\PgSql\Connection($this, $this->utils, $options, 'PgSQL');
        }
        if (extension_loaded("pdo_pgsql")) {
            return new Db\Pdo\Connection($this, $this->utils, $options, 'PDO_PgSQL');
        }
        throw new AuthException($this->utils->trans->lang('No package installed to connect to a PostgreSQL server.'));
    }
}
