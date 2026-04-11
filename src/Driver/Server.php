<?php

namespace Lagdo\DbAdmin\Support\PgSql\Driver;

use Lagdo\DbAdmin\Support\Db\Engine\Driver\AbstractServer;
use Lagdo\DbAdmin\Support\Db\Engine\Connection\AbstractConnection;
use Lagdo\DbAdmin\Support\Exception\AuthException;
use Lagdo\DbAdmin\Support\PgSql\Connection;

use function array_map;
use function count;
use function extension_loaded;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    protected function starting(): void
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
            $this->driver->numberRegex() => ["+", "-"],
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
    protected function connected(): void
    {
        $trans = $this->utils->trans;
        //! get types from current_schemas('t')
        $userTypes = array_map(fn($type) => (int)$type->oid, $this->driver->userTypes(false));
        if (count($userTypes) > 0) {
            $this->config->types[$trans->lang('User types')] = $userTypes;
        }

        if ($this->driver->minVersion(9.2, 0)) {
            $this->config->types[$trans->lang('Strings')]["json"] = 4294967295;
            if ($this->driver->minVersion(9.4, 0)) {
                $this->config->types[$trans->lang('Strings')]["jsonb"] = 4294967295;
            }
        }
        if ($this->driver->minVersion(12, 0)) {
            $this->config->generated = ["STORED"];
        }
        $this->config->partitionBy = ["RANGE", "LIST"];
        // if (!connection()->flavor) {
        //     $this->config->partitionBy[] = "HASH";
        // }

        if ($this->driver->minVersion(9.3)) {
            $this->config->features[] = 'materializedview';
        }
        if ($this->driver->minVersion(11)) {
            $this->config->features[] = 'procedure';
        }
        /*if (connection()->flavor == 'cockroach)*/ {
            $this->config->features[] = 'processlist';
        }
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection(array $options): AbstractConnection|null
    {
        $preferPdo = $options['prefer_pdo'] ?? false;
        if (!$preferPdo && extension_loaded("pgsql")) {
            return new Connection\PgSql\Connection($this->driver,
                $this->grammar, $this->utils, $options, 'PgSQL');
        }
        if (extension_loaded("pdo_pgsql")) {
            return new Connection\Pdo\Connection($this->driver,
                $this->grammar, $this->utils, $options, 'PDO_PgSQL');
        }

        throw new AuthException($this->utils->trans
            ->lang('No package installed to connect to a PostgreSQL server.'));
    }

    /**
     * @inheritDoc
     */
    public function user(): string
    {
        return $this->driver->result("SELECT user");
    }

    /**
     * @inheritDoc
     */
    public function schema()
    {
        return $this->driver->result("SELECT current_schema()");
    }

    /**
     * @inheritDoc
     */
    public function collations(): array
    {
        //! supported in CREATE DATABASE
        return [];
    }

    /**
     * @inheritDoc
     */
    public function routineLanguages(): array
    {
        return $this->driver->values("SELECT LOWER(lanname) FROM pg_catalog.pg_language");
    }

    /**
     * @inheritDoc
     */
    public function variables(): array
    {
        return $this->driver->keyValues("SHOW ALL");
    }

    /**
     * @inheritDoc
     */
    public function processes(): array
    {
        return $this->driver->rows("SELECT * FROM pg_stat_activity ORDER BY " .
            ($this->driver->minVersion(9.2) ? "pid" : "procpid"));
    }

    /**
     * @inheritDoc
     */
    public function statusVariables(): array
    {
        return [];
    }

    /**
     * @inheritDoc
     */
    // public function killProcess($val): bool
    // {
    //     return $this->driver->execute("SELECT pg_terminate_backend(" . $this->utils->str->number($val) . ")");
    // }

    /**
     * @inheritDoc
     */
    // public function maxConnections(): int
    // {
    //     return $this->driver->result("SHOW max_connections");
    // }
}
