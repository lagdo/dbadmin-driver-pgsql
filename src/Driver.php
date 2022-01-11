<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Exception\AuthException;
use Lagdo\DbAdmin\Driver\Driver as AbstractDriver;
use Lagdo\DbAdmin\Driver\Db\Connection as AbstractConnection;

class Driver extends AbstractDriver
{
    /**
     * Driver features
     *
     * @var array
     */
    private $features = ['database', 'table', 'columns', 'sql', 'indexes', 'descidx',
        'comment', 'view', 'scheme', 'routine', 'processlist', 'sequence', 'trigger',
        'type', 'variables', 'drop_col', 'kill', 'dump', 'fkeys_sql'];

    /**
     * Data types
     *
     * @var array
     */
    private $types = [ //! arrays
        'Numbers' => ["smallint" => 5, "integer" => 10, "bigint" => 19, "boolean" => 1,
            "numeric" => 0, "real" => 7, "double precision" => 16, "money" => 20],
        'Date and time' => ["date" => 13, "time" => 17, "timestamp" => 20, "timestamptz" => 21, "interval" => 0],
        'Strings' => ["character" => 0, "character varying" => 0, "text" => 0,
            "tsquery" => 0, "tsvector" => 0, "uuid" => 0, "xml" => 0],
        'Binary' => ["bit" => 0, "bit varying" => 0, "bytea" => 0],
        'Network' => ["cidr" => 43, "inet" => 43, "macaddr" => 17, "txid_snapshot" => 0],
        'Geometry' => ["box" => 0, "circle" => 0, "line" => 0, "lseg" => 0,
            "path" => 0, "point" => 0, "polygon" => 0],
    ];

    /**
     * Number variants
     *
     * @var array
     */
    // private $unsigned = [];

    /**
     * Operators used in select
     *
     * @var array
     */
    private $operators = ["=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE", "LIKE %%", "ILIKE",
        "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT IN", "IS NOT NULL"]; // no "SQL" to avoid CSRF

    /**
     * Functions used in select
     *
     * @var array
     */
    private $functions = ["char_length", "lower", "round", "to_hex", "to_timestamp", "upper"];

    /**
     * Grouping functions used in select
     *
     * @var array
     */
    private $grouping = ["avg", "count", "count distinct", "max", "min", "sum"];

    /**
     * Functions used to edit data
     *
     * @var array
     */
    private $editFunctions = [[
        "char" => "md5",
        "date|time" => "now",
    ],[
        // $this->numberRegex() => "+/-",
        "date|time" => "+ interval/- interval", //! escape
        "char|text" => "||",
    ]];

    /**
     * @inheritDoc
     */
    public function name()
    {
        return "PostgreSQL";
    }

    /**
     * Initialize a new connection
     *
     * @param AbstractConnection $connection
     *
     * @return AbstractConnection
     */
    private function initConnection(AbstractConnection $connection)
    {
        if ($this->connection === null) {
            $this->connection = $connection;
            $this->server = new Db\Server($this, $this->util, $this->trans);
            $this->database = new Db\Database($this, $this->util, $this->trans);
            $this->table = new Db\Table($this, $this->util, $this->trans);
            $this->query = new Db\Query($this, $this->util, $this->trans);
            $this->grammar = new Db\Grammar($this, $this->util, $this->trans);
        }
        return $connection;
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    public function createConnection()
    {
        if (!$this->options('prefer_pdo', false) && extension_loaded("pgsql")) {
            $connection = new Db\PgSql\Connection($this, $this->util, $this->trans, 'PgSQL');
            return $this->initConnection($connection);
        }
        if (extension_loaded("pdo_pgsql")) {
            $connection = new Db\Pdo\Connection($this, $this->util, $this->trans, 'PDO_PgSQL');
            return $this->initConnection($connection);
        }
        throw new AuthException($this->trans->lang('No package installed to connect to a PostgreSQL server.'));
    }

    /**
     * @inheritDoc
     */
    public function connect(string $database, string $schema)
    {
        parent::connect($database, $schema);

        if ($this->minVersion(9.3)) {
            $this->features[] = 'materializedview';
        }

        if ($this->minVersion(9.2)) {
            $this->config->structuredTypes[$this->trans->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
            if ($this->minVersion(9.4)) {
                $this->config->structuredTypes[$this->trans->lang('Strings')][] = "jsonb";
                $this->config->types["jsonb"] = 4294967295;
            }
        }

        foreach ($this->userTypes() as $type) { //! get types from current_schemas('t')
            if (!isset($this->config->types[$type])) {
                $this->config->types[$type] = 0;
                $this->config->structuredTypes[$this->trans->lang('User types')][] = $type;
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function support(string $feature)
    {
        return in_array($feature, $this->features);
    }

    /**
     * @inheritDoc
     */
    protected function initConfig()
    {
        $this->config->jush = 'pgsql';
        $this->config->drivers = ["PgSQL", "PDO_PgSQL"];
        $this->config->setTypes($this->types, $this->trans);
        // $this->config->unsigned = [];
        $this->config->operators = $this->operators;
        $this->config->functions = $this->functions;
        $this->config->grouping = $this->grouping;
        $this->config->editFunctions = $this->editFunctions;
        $this->config->editFunctions[1][$this->numberRegex()] = "+/-";
    }

    /**
     * @inheritDoc
     */
    public function error()
    {
        $message = parent::error();
        if (preg_match('~^(.*\n)?([^\n]*)\n( *)\^(\n.*)?$~s', $message, $match)) {
            $match = array_pad($match, 5, '');
            $message = $match[1] . preg_replace('~((?:[^&]|&[^;]*;){' .
                strlen($match[3]) . '})(.*)~', '\1<b>\2</b>', $match[2]) . $match[4];
        }
        return $message;
    }
}
