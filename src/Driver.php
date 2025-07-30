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
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE", "LIKE %%", "ILIKE",
            "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT IN", "IS NOT NULL"]; // no "SQL" to avoid CSRF
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
        $this->config->features = ['database', 'table', 'columns', 'sql', 'indexes', 'descidx',
            'comment', 'view', 'scheme', 'routine', 'processlist', 'sequence', 'trigger',
            'type', 'variables', 'drop_col', 'kill', 'dump', 'fkeys_sql'];
    }

    /**
     * @inheritDoc
     */
    protected function afterConnection()
    {
        if ($this->minVersion(9.3)) {
            $this->config->features[] = 'materializedview';
        }
        if ($this->minVersion(9.2)) {
            $this->config->structuredTypes[$this->utils->trans->lang('Strings')][] = "json";
            $this->config->types["json"] = 4294967295;
            if ($this->minVersion(9.4)) {
                $this->config->structuredTypes[$this->utils->trans->lang('Strings')][] = "jsonb";
                $this->config->types["jsonb"] = 4294967295;
            }
        }
        foreach ($this->userTypes() as $type) { //! get types from current_schemas('t')
            if (!isset($this->config->types[$type])) {
                $this->config->types[$type] = 0;
                $this->config->structuredTypes[$this->utils->trans->lang('User types')][] = $type;
            }
        }
    }

    /**
     * @inheritDoc
     * @throws AuthException
     */
    protected function createConnection()
    {
        if (!$this->options('prefer_pdo', false) && extension_loaded("pgsql")) {
            $connection = new Db\PgSql\Connection($this, $this->utils, 'PgSQL');
            return $this->connection = $connection;
        }
        if (extension_loaded("pdo_pgsql")) {
            $connection = new Db\Pdo\Connection($this, $this->utils, 'PDO_PgSQL');
            return $this->connection = $connection;
        }
        throw new AuthException($this->utils->trans->lang('No package installed to connect to a PostgreSQL server.'));
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
