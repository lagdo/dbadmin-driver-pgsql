<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Exception\AuthException;
use Lagdo\DbAdmin\Driver\Utils\Utils;
use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;

class Driver extends AbstractDriver
{
    /**
     * The constructor
     *
     * @param Utils $utils
     * @param array $options
     */
    public function __construct(Utils $utils, array $options)
    {
        parent::__construct($utils, $options);

        $this->server = new Db\Server($this, $this->utils);
        $this->database = new Db\Database($this, $this->utils);
        $this->table = new Db\Table($this, $this->utils);
        $this->query = new Db\Query($this, $this->utils);
        $this->grammar = new Db\Grammar($this, $this->utils);
    }

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
    protected function beforeConnection()
    {
        // Init config
        $this->config->jush = 'pgsql';
        $this->config->drivers = ["PgSQL", "PDO_PgSQL"];
        $this->config->setTypes([ //! arrays
            'Numbers' => ["smallint" => 5, "integer" => 10, "bigint" => 19, "boolean" => 1,
                "numeric" => 0, "real" => 7, "double precision" => 16, "money" => 20],
            'Date and time' => ["date" => 13, "time" => 17, "timestamp" => 20, "timestamptz" => 21, "interval" => 0],
            'Strings' => ["character" => 0, "character varying" => 0, "text" => 0,
                "tsquery" => 0, "tsvector" => 0, "uuid" => 0, "xml" => 0],
            'Binary' => ["bit" => 0, "bit varying" => 0, "bytea" => 0],
            'Network' => ["cidr" => 43, "inet" => 43, "macaddr" => 17, "txid_snapshot" => 0],
            'Geometry' => ["box" => 0, "circle" => 0, "line" => 0, "lseg" => 0,
                "path" => 0, "point" => 0, "polygon" => 0],
        ]);
        // $this->config->unsigned = [];
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE",
            "LIKE %%", "ILIKE", "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT ILIKE",
            "NOT IN", "IS NOT NULL", "SQL"]; // no "SQL" to avoid CSRF
        $this->config->functions = ["char_length", "lower", "round", "to_hex", "to_timestamp", "upper"];
        $this->config->grouping = ["avg", "count", "count distinct", "max", "min", "sum"];
        $this->config->insertFunctions = [
            "char" => "md5",
            "date|time" => "now",
        ];
        $this->config->editFunctions = [
            $this->numberRegex() => "+/-",
            "date|time" => "+ interval/- interval", //! escape
            "char|text" => "||",
        ];
        $this->config->features = ['check', 'columns', 'comment', 'database', 'drop_col', 'dump',
            'descidx', 'indexes', 'kill', 'partial_indexes', 'routine', 'scheme', 'sequence',
            'sql', 'table', 'trigger', 'type', 'variables', 'view'];
    }

    /**
     * @inheritDoc
     */
    protected function configConnection()
    {
        foreach ($this->userTypes(false) as $type) { //! get types from current_schemas('t')
            $name = $type->name;
            if (!isset($this->config->types[$name])) {
                $this->config->types[$name] = 0;
                $this->config->structuredTypes[$this->utils->trans->lang('User types')][] = $name;
            }
        }

        if ($this->minVersion(9.2, 0)) {
            $this->config->setTypes(['Strings' => ["json" => 4294967295]]);
            if ($this->minVersion(9.4, 0)) {
                $this->config->setTypes(['Strings' => ["jsonb" => 4294967295]]);
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
    protected function openedConnection()
    {
        $this->server->setConnection($this->connection);
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection(array $options)
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
