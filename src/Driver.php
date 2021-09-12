<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Exception\AuthException;
use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;

class Driver extends AbstractDriver
{
    /**
     * @inheritDoc
     */
    public function name()
    {
        return "PostgreSQL";
    }

    /**
     * @inheritDoc
     */
    public function createConnection()
    {
        $connection = null;
        if (extension_loaded("pgsql")) {
            $connection = new Db\PgSql\Connection($this, $this->util, 'PgSQL');
        }
        elseif (extension_loaded("pdo_pgsql")) {
            $connection = new Db\Pdo\Connection($this, $this->util, 'PDO_PgSQL');
        }
        else {
            throw new AuthException($this->util->lang('No package installed to connect to a PostgreSQL server.'));
        }

        $firstConnection = ($this->connection === null);
        if ($firstConnection) {
            $this->connection = $connection;
            $this->server = new Db\Server($this, $this->util, $connection);
            $this->table = new Db\Table($this, $this->util, $connection);
            $this->query = new Db\Query($this, $this->util, $connection);
            $this->grammar = new Db\Grammar($this, $this->util, $connection);
        }

        if (!$connection->open($this->options('server'), $this->options())) {
            throw new AuthException($this->error());
        }

        if ($this->minVersion(9, 0)) {
            $connection->query("SET application_name = 'Adminer'");
        }
        if ($firstConnection && $this->minVersion(9.2, 0)) {
            $this->config->structuredTypes[$this->util->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
            if ($this->minVersion(9.4, 0)) {
                $this->config->structuredTypes[$this->util->lang('Strings')][] = "jsonb";
                $this->config->types["jsonb"] = 4294967295;
            }
        }

        return $connection;
    }

    /**
     * @inheritDoc
     */
    public function selectSchema(string $schema, ConnectionInterface $connection = null)
    {
        if (!$connection) {
            $connection = $this->connection;
        }
        $return = $connection->query("SET search_path TO " . $this->escapeId($schema));
        foreach ($this->userTypes() as $type) { //! get types from current_schemas('t')
            if (!isset($this->config->types[$type])) {
                $this->config->types[$type] = 0;
                $this->config->structuredTypes[$this->util->lang('User types')][] = $type;
            }
        }
        return $return;
    }

    /**
     * @inheritDoc
     */
    public function support(string $feature)
    {
        return preg_match('~^(database|table|columns|sql|indexes|descidx|comment|view|' .
            ($this->minVersion(9.3) ? 'materializedview|' : '') .
            'scheme|routine|processlist|sequence|trigger|type|variables|drop_col|kill|dump|fkeys_sql)$~', $feature);
    }

    /**
     * @inheritDoc
     */
    protected function initConfig()
    {
        $this->config->jush = 'pgsql';
        $this->config->drivers = ["PgSQL", "PDO_PgSQL"];

        $groups = [ //! arrays
            $this->util->lang('Numbers') => ["smallint" => 5, "integer" => 10, "bigint" => 19, "boolean" => 1, "numeric" => 0, "real" => 7, "double precision" => 16, "money" => 20],
            $this->util->lang('Date and time') => ["date" => 13, "time" => 17, "timestamp" => 20, "timestamptz" => 21, "interval" => 0],
            $this->util->lang('Strings') => ["character" => 0, "character varying" => 0, "text" => 0, "tsquery" => 0, "tsvector" => 0, "uuid" => 0, "xml" => 0],
            $this->util->lang('Binary') => ["bit" => 0, "bit varying" => 0, "bytea" => 0],
            $this->util->lang('Network') => ["cidr" => 43, "inet" => 43, "macaddr" => 17, "txid_snapshot" => 0],
            $this->util->lang('Geometry') => ["box" => 0, "circle" => 0, "line" => 0, "lseg" => 0, "path" => 0, "point" => 0, "polygon" => 0],
        ];
        foreach ($groups as $name => $types) {
            $this->config->structuredTypes[$name] = array_keys($types);
            $this->config->types = array_merge($this->config->types, $types);
        }

        // $this->config->unsigned = [];
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE", "LIKE %%", "ILIKE", "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT IN", "IS NOT NULL"]; // no "SQL" to avoid CSRF
        $this->config->functions = ["char_length", "lower", "round", "to_hex", "to_timestamp", "upper"];
        $this->config->grouping = ["avg", "count", "count distinct", "max", "min", "sum"];
        $this->config->editFunctions = [[
            "char" => "md5",
            "date|time" => "now",
        ],[
            $this->numberRegex() => "+/-",
            "date|time" => "+ interval/- interval", //! escape
            "char|text" => "||",
        ]];
    }
}
