<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Db\Server as AbstractServer;
use Lagdo\DbAdmin\Driver\Entity\TableField;
use Lagdo\DbAdmin\Driver\Entity\Table;
use Lagdo\DbAdmin\Driver\Entity\Index;
use Lagdo\DbAdmin\Driver\Entity\ForeignKey;
use Lagdo\DbAdmin\Driver\Entity\Trigger;
use Lagdo\DbAdmin\Driver\Entity\Routine;

class Server extends AbstractServer
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
    public function connect()
    {
        $connection = null;
        if (extension_loaded("pgsql")) {
            $connection = new PgSql\Connection($this->db, $this->util, $this, 'PgSQL');
        }
        elseif (extension_loaded("pdo_pgsql")) {
            $connection = new Pdo\Connection($this->db, $this->util, $this, 'PDO_PgSQL');
        }
        else {
            throw new AuthException($this->util->lang('No package installed to connect to a PostgreSQL server.'));
        }

        if ($this->connection === null) {
            $this->connection = $connection;
            $this->driver = new Driver($this->db, $this->util, $this, $connection);
        }

        if (!$connection->open($this->db->options('server'), $this->db->options())) {
            throw new AuthException($this->util->error());
        }

        if ($this->minVersion(9, 0)) {
            $connection->query("SET application_name = 'Adminer'");
            if ($this->minVersion(9.2, 0)) {
                $this->config->structuredTypes[$this->util->lang('Strings')][] = "json";
                $this->config->types["json"] = 4294967295;
                if ($this->minVersion(9.4, 0)) {
                    $this->config->structuredTypes[$this->util->lang('Strings')][] = "jsonb";
                    $this->config->types["jsonb"] = 4294967295;
                }
            }
        }

        return $connection;
    }

    /**
     * @inheritDoc
     */
    public function escapeId($idf)
    {
        return '"' . str_replace('"', '""', $idf) . '"';
    }

    /**
     * @inheritDoc
     */
    public function databases($flush)
    {
        return $this->db->values("SELECT datname FROM pg_database WHERE " .
            "has_database_privilege(datname, 'CONNECT') ORDER BY datname");
    }

    /**
     * @inheritDoc
     */
    public function databaseSize($database)
    {
        $statement = $this->connection->query("SELECT pg_database_size(" . $this->quote($database) . ")");
        if (is_object($statement) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function limit($query, $where, $limit, $offset = 0, $separator = " ")
    {
        return " $query$where" . ($limit !== null ? $separator . "LIMIT $limit" .
            ($offset ? " OFFSET $offset" : "") : "");
    }

    /**
     * @inheritDoc
     */
    public function limitToOne($table, $query, $where, $separator = "\n")
    {
        return (preg_match('~^INTO~', $query) ? $this->limit($query, $where, 1, 0, $separator) :
            " $query" . ($this->isView($this->tableStatusOrName($table)) ? $where :
            " WHERE ctid = (SELECT ctid FROM " . $this->table($table) . $where . $separator . "LIMIT 1)")
        );
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation($db, $collations)
    {
        return $this->connection->result("SELECT datcollate FROM pg_database WHERE datname = " . $this->quote($db));
    }

    public function loggedUser()
    {
        return $this->connection->result("SELECT user");
    }

    public function tables()
    {
        $query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()";
        if ($this->support('materializedview')) {
            $query .= " UNION ALL SELECT matviewname, 'MATERIALIZED VIEW' FROM pg_matviews WHERE schemaname = current_schema()";
        }
        $query .= " ORDER BY 1";
        return $this->db->keyValues($query);
    }

    public function countTables($databases)
    {
        return []; // would require reconnect
    }

    /**
     * @inheritDoc
     */
    public function tableStatus($name = "", $fast = false)
    {
        $tables = [];
        $query = "SELECT c.relname AS \"Name\", CASE c.relkind " .
            "WHEN 'r' THEN 'table' WHEN 'm' THEN 'materialized view' ELSE 'view' END AS \"Engine\", " .
            "pg_relation_size(c.oid) AS \"Data_length\", " .
            "pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS \"Index_length\", " .
            "obj_description(c.oid, 'pg_class') AS \"Comment\", " .
            ($this->minVersion(12) ? "''" : "CASE WHEN c.relhasoids THEN 'oid' ELSE '' END") .
            " AS \"Oid\", c.reltuples as \"Rows\", n.nspname FROM pg_class c " .
            "JOIN pg_namespace n ON(n.nspname = current_schema() AND n.oid = c.relnamespace) " .
            "WHERE relkind IN ('r', 'm', 'v', 'f', 'p') " .
            ($name != "" ? "AND relname = " . $this->quote($name) : "ORDER BY relname");
        foreach ($this->db->rows($query) as $row)
        {
            $status = new Table($row['Name']);
            $status->engine = $row['Engine'];
            $status->schema = $row['nspname'];
            $status->dataLength = $row['Data_length'];
            $status->indexLength = $row['Index_length'];
            $status->oid = $row['Oid'];
            $status->rows = $row['Rows'];
            $status->comment = $row['Comment'];

            //! Index_length, Auto_increment
            if ($name != "") {
                return $status;
            }
            $tables[$row["Name"]] = $status;
        }
        return $tables;
    }

    public function isView($tableStatus)
    {
        return in_array($tableStatus->engine, ["view", "materialized view"]);
    }

    public function supportForeignKeys($tableStatus)
    {
        return true;
    }

    public function fields($table)
    {
        $fields = [];
        $aliases = [
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamptz',
        ];

        $identity_column = $this->minVersion(10) ? 'a.attidentity' : '0';
        $query = "SELECT a.attname AS field, format_type(a.atttypid, a.atttypmod) AS full_type, " .
            "pg_get_expr(d.adbin, d.adrelid) AS default, a.attnotnull::int, " .
            "col_description(c.oid, a.attnum) AS comment, $identity_column AS identity FROM pg_class c " .
            "JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid " .
            "LEFT JOIN pg_attrdef d ON c.oid = d.adrelid AND a.attnum = d.adnum WHERE c.relname = " .
            $this->quote($table) .
            " AND n.nspname = current_schema() AND NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum";
        foreach ($this->db->rows($query) as $row)
        {
            $field = new TableField();

            $field->name = $row["field"];
            $field->fullType = $row["full_type"];
            $field->default = $row["default"];
            $field->comment = $row["comment"];
            //! No collation, no info about primary keys
            preg_match('~([^([]+)(\((.*)\))?([a-z ]+)?((\[[0-9]*])*)$~', $field->fullType, $match);
            list(, $type, $length, $field->length, $addon, $array) = $match;
            $field->length .= $array;
            $check_type = $type . $addon;
            if (isset($aliases[$check_type])) {
                $field->type = $aliases[$check_type];
                $field->fullType = $field->type . $length . $array;
            } else {
                $field->type = $type;
                $field->fullType = $field->type . $length . $addon . $array;
            }
            if (in_array($row['identity'], ['a', 'd'])) {
                $field->default = 'GENERATED ' . ($row['identity'] == 'd' ? 'BY DEFAULT' : 'ALWAYS') . ' AS IDENTITY';
            }
            $field->null = !$row["attnotnull"];
            $field->autoIncrement = $row['identity'] || preg_match('~^nextval\(~i', $row["default"]);
            $field->privileges = ["insert" => 1, "select" => 1, "update" => 1];
            if (preg_match('~(.+)::[^,)]+(.*)~', $row["default"], $match)) {
                $match1 = $match[1] ?? '';
                $match10 = $match1[0] ?? '';
                $match2 = $match[2] ?? '';
                $field->default = ($match1 == "NULL" ? null :
                    (($match10 == "'" ? $this->unescapeId($match1) : $match1) . $match2));
            }

            $fields[$field->name] = $field;
        }
        return $fields;
    }

    public function indexes($table, $connection = null)
    {
        if (!is_object($connection)) {
            $connection = $this->connection;
        }
        $indexes = [];
        $table_oid = $connection->result("SELECT oid FROM pg_class WHERE " .
            "relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) " .
            "AND relname = " . $this->quote($table));
        $columns = $this->db->keyValues("SELECT attnum, attname FROM pg_attribute WHERE " .
            "attrelid = $table_oid AND attnum > 0", $connection);
        foreach ($this->db->rows("SELECT relname, indisunique::int, indisprimary::int, indkey, " .
            "indoption, (indpred IS NOT NULL)::int as indispartial FROM pg_index i, pg_class ci " .
            "WHERE i.indrelid = $table_oid AND ci.oid = i.indexrelid", $connection) as $row)
        {
            $index = new Index();

            $relname = $row["relname"];
            $index->type = ($row["indispartial"] ? "INDEX" :
                ($row["indisprimary"] ? "PRIMARY" : ($row["indisunique"] ? "UNIQUE" : "INDEX")));
            $index->columns = [];
            foreach (explode(" ", $row["indkey"]) as $indkey) {
                $index->columns[] = $columns[$indkey];
            }
            $index->descs = [];
            foreach (explode(" ", $row["indoption"]) as $indoption) {
                $index->descs[] = ($indoption & 1 ? '1' : null); // 1 - INDOPTION_DESC
            }
            $index->lengths = [];

            $indexes[$relname] = $index;
        }
        return $indexes;
    }

    public function foreignKeys($table)
    {
        $foreignKeys = [];
        $query = "SELECT conname, condeferrable::int AS deferrable, pg_get_constraintdef(oid) " .
            "AS definition FROM pg_constraint WHERE conrelid = (SELECT pc.oid FROM pg_class AS pc " .
            "INNER JOIN pg_namespace AS pn ON (pn.oid = pc.relnamespace) WHERE pc.relname = " .
            $this->quote($table) .
            " AND pn.nspname = current_schema()) AND contype = 'f'::char ORDER BY conkey, conname";
        foreach ($this->db->rows($query) as $row) {
            if (preg_match('~FOREIGN KEY\s*\((.+)\)\s*REFERENCES (.+)\((.+)\)(.*)$~iA', $row['definition'], $match)) {
                $match1 = $match[1] ?? '';
                $match2 = $match[2] ?? '';
                $match3 = $match[3] ?? '';
                $match4 = $match[4] ?? '';
                $match11 = '';

                $foreignKey = new ForeignKey();

                $foreignKey->source = array_map('trim', explode(',', $match1));
                $foreignKey->target = array_map('trim', explode(',', $match3));
                $foreignKey->onDelete = preg_match("~ON DELETE ({$this->onActions})~", $match4, $match10) ? $match11 : 'NO ACTION';
                $foreignKey->onUpdate = preg_match("~ON UPDATE ({$this->onActions})~", $match4, $match10) ? $match11 : 'NO ACTION';

                if (preg_match('~^(("([^"]|"")+"|[^"]+)\.)?"?("([^"]|"")+"|[^"]+)$~', $match2, $match10)) {
                    $match11 = $match10[1] ?? '';
                    $match12 = $match10[2] ?? '';
                    // $match13 = $match10[3] ?? '';
                    $match14 = $match10[4] ?? '';
                    $foreignKey->schema = str_replace('""', '"', preg_replace('~^"(.+)"$~', '\1', $match12));
                    $foreignKey->table = str_replace('""', '"', preg_replace('~^"(.+)"$~', '\1', $match14));
                }

                $foreignKeys[$row['conname']] = $foreignKey;
            }
        }
        return $foreignKeys;
    }

    public function constraints($table)
    {
        $return = [];
        $query = "SELECT conname, consrc FROM pg_catalog.pg_constraint " .
            "INNER JOIN pg_catalog.pg_namespace ON pg_constraint.connamespace = pg_namespace.oid " .
            "INNER JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid " .
            "AND pg_constraint.connamespace = pg_class.relnamespace WHERE pg_constraint.contype = 'c' " .
            // "-- handle only CONSTRAINTs here, not TYPES " .
            "AND conrelid != 0  AND nspname = current_schema() AND relname = " .
            $this->quote($table) . "ORDER BY connamespace, conname";
        foreach ($this->db->rows($query) as $row)
        {
            $return[$row['conname']] = $row['consrc'];
        }
        return $return;
    }

    public function view($name)
    {
        return [
            "select" => trim($this->connection->result("SELECT pg_get_viewdef(" .
                $this->connection->result("SELECT oid FROM pg_class WHERE relnamespace = " .
                "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND relname = " .
                $this->quote($name)) . ")"))
        ];
    }

    public function collations()
    {
        //! supported in CREATE DATABASE
        return [];
    }

    public function isInformationSchema($db)
    {
        return ($db == "information_schema");
    }

    public function error()
    {
        $return = parent::error();
        if (preg_match('~^(.*\n)?([^\n]*)\n( *)\^(\n.*)?$~s', $return, $match)) {
            $match1 = $match[1] ?? '';
            $match2 = $match[2] ?? '';
            $match3 = $match[3] ?? '';
            $match4 = $match[4] ?? '';
            $return = $match1 . preg_replace('~((?:[^&]|&[^;]*;){' .
                strlen($match3) . '})(.*)~', '\1<b>\2</b>', $match2) . $match4;
        }
        return $this->util->convertEolToHtml($return);
    }

    public function createDatabase($db, $collation)
    {
        return $this->db->queries("CREATE DATABASE " . $this->escapeId($db) .
            ($collation ? " ENCODING " . $this->escapeId($collation) : ""));
    }

    public function dropDatabases($databases)
    {
        $this->connection->close();
        return $this->db->applyQueries("DROP DATABASE", $databases, function ($database) {
            return $this->escapeId($database);
        });
    }

    public function renameDatabase($name, $collation)
    {
        //! current database cannot be renamed
        return $this->db->queries("ALTER DATABASE " . $this->escapeId($this->selectedDatabase()) .
            " RENAME TO " . $this->escapeId($name));
    }

    public function alterTable($table, $name, $fields, $foreign, $comment, $engine,
        $collation, $autoIncrement, $partitioning)
    {
        $alter = [];
        $queries = [];
        if ($table != "" && $table != $name) {
            $queries[] = "ALTER TABLE " . $this->table($table) . " RENAME TO " . $this->table($name);
        }
        foreach ($fields as $field) {
            $column = $this->escapeId($field[0]);
            $val = $field[1];
            if (!$val) {
                $alter[] = "DROP $column";
            } else {
                $val5 = $val[5];
                unset($val[5]);
                if ($field[0] == "") {
                    if (isset($val[6])) { // auto increment
                        $val[1] = ($val[1] == " bigint" ? " big" : ($val[1] == " smallint" ? " small" : " ")) . "serial";
                    }
                    $alter[] = ($table != "" ? "ADD " : "  ") . implode($val);
                    if (isset($val[6])) {
                        $alter[] = ($table != "" ? "ADD" : " ") . " PRIMARY KEY ($val[0])";
                    }
                } else {
                    if ($column != $val[0]) {
                        $queries[] = "ALTER TABLE " . $this->table($name) . " RENAME $column TO $val[0]";
                    }
                    $alter[] = "ALTER $column TYPE$val[1]";
                    if (!$val[6]) {
                        $alter[] = "ALTER $column " . ($val[3] ? "SET$val[3]" : "DROP DEFAULT");
                        $alter[] = "ALTER $column " . ($val[2] == " NULL" ? "DROP NOT" : "SET") . $val[2];
                    }
                }
                if ($field[0] != "" || $val5 != "") {
                    $queries[] = "COMMENT ON COLUMN " . $this->table($name) . ".$val[0] IS " . ($val5 != "" ? substr($val5, 9) : "''");
                }
            }
        }
        $alter = array_merge($alter, $foreign);
        if ($table == "") {
            array_unshift($queries, "CREATE TABLE " . $this->table($name) . " (\n" . implode(",\n", $alter) . "\n)");
        } elseif ($alter) {
            array_unshift($queries, "ALTER TABLE " . $this->table($table) . "\n" . implode(",\n", $alter));
        }
        if ($table != "" || $comment != "") {
            $queries[] = "COMMENT ON TABLE " . $this->table($name) . " IS " . $this->quote($comment);
        }
        if ($autoIncrement != "") {
            //! $queries[] = "SELECT setval(pg_get_serial_sequence(" . $this->quote($name) . ", ), $autoIncrement)";
        }
        foreach ($queries as $query) {
            if (!$this->db->queries($query)) {
                return false;
            }
        }
        return true;
    }

    public function alterIndexes($table, $alter)
    {
        $create = [];
        $drop = [];
        $queries = [];
        foreach ($alter as $val) {
            if ($val[0] != "INDEX") {
                //! descending UNIQUE indexes results in syntax error
                $create[] = (
                    $val[2] == "DROP" ? "\nDROP CONSTRAINT " . $this->escapeId($val[1]) :
                    "\nADD" . ($val[1] != "" ? " CONSTRAINT " . $this->escapeId($val[1]) : "") .
                    " $val[0] " . ($val[0] == "PRIMARY" ? "KEY " : "") . "(" . implode(", ", $val[2]) . ")"
                );
            } elseif ($val[2] == "DROP") {
                $drop[] = $this->escapeId($val[1]);
            } else {
                $queries[] = "CREATE INDEX " . $this->escapeId($val[1] != "" ? $val[1] : uniqid($table . "_")) .
                    " ON " . $this->table($table) . " (" . implode(", ", $val[2]) . ")";
            }
        }
        if ($create) {
            array_unshift($queries, "ALTER TABLE " . $this->table($table) . implode(",", $create));
        }
        if ($drop) {
            array_unshift($queries, "DROP INDEX " . implode(", ", $drop));
        }
        foreach ($queries as $query) {
            if (!$this->db->queries($query)) {
                return false;
            }
        }
        return true;
    }

    public function truncateTables($tables)
    {
        return $this->db->queries("TRUNCATE " . implode(", ", array_map(function ($table) {
            return $this->table($table);
        }, $tables)));
        return true;
    }

    public function dropViews($views)
    {
        return $this->dropTables($views);
    }

    public function dropTables($tables)
    {
        foreach ($tables as $table) {
            $status = $this->tableStatus($table);
            if (!$this->db->queries("DROP " . strtoupper($status->engine) . " " . $this->table($table))) {
                return false;
            }
        }
        return true;
    }

    public function moveTables($tables, $views, $target)
    {
        foreach (array_merge($tables, $views) as $table) {
            $status = $this->tableStatus($table);
            if (!$this->db->queries("ALTER " . strtoupper($status->engine) . " " .
                $this->table($table) . " SET SCHEMA " . $this->escapeId($target))) {
                return false;
            }
        }
        return true;
    }

    public function trigger($name, $table = null)
    {
        if ($name == "") {
            return ["Statement" => "EXECUTE PROCEDURE ()"];
        }
        if ($table === null) {
            $table = $this->util->input()->getTable();
        }
        $query = 'SELECT t.trigger_name AS "Trigger", t.action_timing AS "Timing", ' .
            '(SELECT STRING_AGG(event_manipulation, \' OR \') FROM information_schema.triggers ' .
            'WHERE event_object_table = t.event_object_table AND trigger_name = t.trigger_name ) AS "Events", ' .
            't.event_manipulation AS "Event", \'FOR EACH \' || t.action_orientation AS "Type", ' .
            't.action_statement AS "Statement" FROM information_schema.triggers t WHERE t.event_object_table = ' .
            $this->quote($table) . ' AND t.trigger_name = ' . $this->quote($name);
        $rows = $this->db->rows($query);
        return reset($rows);
    }

    public function triggers($table)
    {
        $triggers = [];
        $query = "SELECT * FROM information_schema.triggers WHERE trigger_schema = current_schema() " .
            "AND event_object_table = " . $this->quote($table);
        foreach ($this->db->rows($query) as $row) {
            $triggers[$row["trigger_name"]] = new Trigger($row["action_timing"], $row["event_manipulation"]);
        }
        return $triggers;
    }

    public function triggerOptions()
    {
        return [
            "Timing" => ["BEFORE", "AFTER"],
            "Event" => ["INSERT", "UPDATE", "DELETE"],
            "Type" => ["FOR EACH ROW", "FOR EACH STATEMENT"],
        ];
    }

    public function routine($name, $type)
    {
        $query = 'SELECT routine_definition AS definition, LOWER(external_language) AS language, * ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ' .
            'AND specific_name = ' . $this->quote($name);
        $rows = $this->db->rows($query);
        $return = $rows[0];
        $return["returns"] = ["type" => $return["type_udt_name"]];
        $query = 'SELECT parameter_name AS field, data_type AS type, character_maximum_length AS length, ' .
            'parameter_mode AS inout FROM information_schema.parameters WHERE specific_schema = current_schema() ' .
            'AND specific_name = ' . $this->quote($name) . ' ORDER BY ordinal_position';
        $return["fields"] = $this->db->rows($query);
        return $return;
    }

    public function routines()
    {
        $query = 'SELECT specific_name AS "SPECIFIC_NAME", routine_type AS "ROUTINE_TYPE", ' .
            'routine_name AS "ROUTINE_NAME", type_udt_name AS "DTD_IDENTIFIER" ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ORDER BY SPECIFIC_NAME';
        $rows = $this->db->rows($query);
        return array_map(function($row) {
            return new Routine($row['ROUTINE_NAME'], $row['SPECIFIC_NAME'], $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER']);
        }, $rows);
    }

    public function routineLanguages()
    {
        return $this->db->values("SELECT LOWER(lanname) FROM pg_catalog.pg_language");
    }

    public function routineId($name, $row)
    {
        $return = [];
        foreach ($row["fields"] as $field) {
            $return[] = $field->type;
        }
        return $this->escapeId($name) . "(" . implode(", ", $return) . ")";
    }

    public function lastAutoIncrementId()
    {
        return 0; // there can be several sequences
    }

    public function explain($connection, $query)
    {
        return $connection->query("EXPLAIN $query");
    }

    public function countRows($tableStatus, $where)
    {
        $query = "EXPLAIN SELECT * FROM " . $this->escapeId($tableStatus->name) .
            ($where ? " WHERE " . implode(" AND ", $where) : "");
        if (preg_match("~ rows=([0-9]+)~", $this->connection->result($query), $regs ))
        {
            return $regs[1];
        }
        return false;
    }

    public function userTypes()
    {
        $query = "SELECT typname FROM pg_type WHERE typnamespace = " .
            "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) " .
            "AND typtype IN ('b','d','e') AND typelem = 0";
        return $this->db->values($query);
    }

    public function schemas()
    {
        return $this->db->values("SELECT nspname FROM pg_namespace ORDER BY nspname");
    }

    public function schema()
    {
        return $this->connection->result("SELECT current_schema()");
    }

    public function selectSchema($schema, $connection = null)
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
    public function foreignKeysSql($table)
    {
        $return = "";

        $status = $this->tableStatus($table);
        $fkeys = $this->foreignKeys($table);
        ksort($fkeys);

        foreach ($fkeys as $fkey_name => $fkey) {
            $return .= "ALTER TABLE ONLY " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " ADD CONSTRAINT " . $this->escapeId($fkey_name) .
                " {$fkey->definition} " . ($fkey->deferrable ? 'DEFERRABLE' : 'NOT DEFERRABLE') . ";\n";
        }

        return ($return ? "$return\n" : $return);
    }

    public function createTableSql($table, $autoIncrement, $style)
    {
        $return = '';
        $return_parts = [];
        $sequences = [];

        $status = $this->tableStatus($table);
        if ($this->isView($status)) {
            $view = $this->view($table);
            return rtrim("CREATE VIEW " . $this->escapeId($table) . " AS $view[select]", ";");
        }
        $fields = $this->fields($table);
        $indexes = $this->indexes($table);
        ksort($indexes);
        $constraints = $this->constraints($table);

        if (!$status || empty($fields)) {
            return false;
        }

        $return = "CREATE TABLE " . $this->escapeId($status->schema) . "." .
            $this->escapeId($status->name) . " (\n    ";

        // fields' definitions
        foreach ($fields as $field_name => $field) {
            $part = $this->escapeId($field->name) . ' ' . $field->fullType .
                $this->db->defaultValue($field) . ($field->attnotnull ? " NOT NULL" : "");
            $return_parts[] = $part;

            // sequences for fields
            if (preg_match('~nextval\(\'([^\']+)\'\)~', $field->default, $matches)) {
                $sequence_name = $matches[1];
                $sq = reset($this->db->rows($this->minVersion(10) ?
                    "SELECT *, cache_size AS cache_value FROM pg_sequences " .
                    "WHERE schemaname = current_schema() AND sequencename = " .
                    $this->quote($sequence_name) : "SELECT * FROM $sequence_name"));
                $sequences[] = ($style == "DROP+CREATE" ? "DROP SEQUENCE IF EXISTS $sequence_name;\n" : "") .
                    "CREATE SEQUENCE $sequence_name INCREMENT $sq[increment_by] MINVALUE $sq[min_value] MAXVALUE $sq[max_value]" .
                    ($autoIncrement && $sq['last_value'] ? " START $sq[last_value]" : "") . " CACHE $sq[cache_value];";
            }
        }

        // adding sequences before table definition
        if (!empty($sequences)) {
            $return = implode("\n\n", $sequences) . "\n\n$return";
        }

        // primary + unique keys
        foreach ($indexes as $index_name => $index) {
            switch ($index->type) {
                case 'UNIQUE':
                    $return_parts[] = "CONSTRAINT " . $this->escapeId($index_name) .
                        " UNIQUE (" . implode(', ', array_map(function ($column) {
                            return $this->escapeId($column);
                        }, $index->columns)) . ")";
                    break;
                case 'PRIMARY':
                    $return_parts[] = "CONSTRAINT " . $this->escapeId($index_name) .
                        " PRIMARY KEY (" . implode(', ', array_map(function ($column) {
                            return $this->escapeId($column);
                        }, $index->columns)) . ")";
                    break;
            }
        }

        foreach ($constraints as $conname => $consrc) {
            $return_parts[] = "CONSTRAINT " . $this->escapeId($conname) . " CHECK $consrc";
        }

        $return .= implode(",\n    ", $return_parts) . "\n) WITH (oids = " . ($status->oid ? 'true' : 'false') . ");";

        // "basic" indexes after table definition
        foreach ($indexes as $index_name => $index) {
            if ($index->type == 'INDEX') {
                $columns = [];
                foreach ($index->columns as $key => $val) {
                    $columns[] = $this->escapeId($val) . ($index->descs[$key] ? " DESC" : "");
                }
                $return .= "\n\nCREATE INDEX " . $this->escapeId($index_name) . " ON " .
                    $this->escapeId($status->schema) . "." . $this->escapeId($status->name) .
                    " USING btree (" . implode(', ', $columns) . ");";
            }
        }

        // coments for table & fields
        if ($status->comment) {
            $return .= "\n\nCOMMENT ON TABLE " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " IS " . $this->quote($status->comment) . ";";
        }

        foreach ($fields as $field_name => $field) {
            if ($field->comment) {
                $return .= "\n\nCOMMENT ON COLUMN " . $this->escapeId($status->schema) . "." .
                    $this->escapeId($status->name) . "." . $this->escapeId($field_name) .
                    " IS " . $this->quote($field->comment) . ";";
            }
        }

        return rtrim($return, ';');
    }

    public function truncateTableSql($table)
    {
        return "TRUNCATE " . $this->table($table);
    }

    public function createTriggerSql($table)
    {
        $status = $this->tableStatus($table);
        $return = "";
        foreach ($this->triggers($table) as $trg_id => $trg) {
            $trigger = $this->trigger($trg_id, $status->name);
            $return .= "\nCREATE TRIGGER " . $this->escapeId($trigger['Trigger']) .
                " $trigger[Timing] $trigger[Events] ON " . $this->escapeId($status->schema) . "." .
                $this->escapeId($status->name) . " $trigger[Type] $trigger[Statement];;\n";
        }
        return $return;
    }


    public function useDatabaseSql($database)
    {
        return "\connect " . $this->escapeId($database);
    }

    public function variables()
    {
        return $this->db->keyValues("SHOW ALL");
    }

    public function processes()
    {
        return $this->db->rows("SELECT * FROM pg_stat_activity ORDER BY " . ($this->minVersion(9.2) ? "pid" : "procpid"));
    }

    public function statusVariables()
    {
    }

    public function support($feature)
    {
        return preg_match('~^(database|table|columns|sql|indexes|descidx|comment|view|' .
            ($this->minVersion(9.3) ? 'materializedview|' : '') .
            'scheme|routine|processlist|sequence|trigger|type|variables|drop_col|kill|dump|fkeys_sql)$~', $feature);
    }

    public function killProcess($val)
    {
        return $this->db->queries("SELECT pg_terminate_backend(" . $this->util->number($val) . ")");
    }

    public function connectionId()
    {
        return "SELECT pg_backend_pid()";
    }

    public function maxConnections()
    {
        return $this->connection->result("SHOW max_connections");
    }

    /**
     * @inheritDoc
     */
    protected function setConfig()
    {
        $this->config->jush = 'pgsql';
        $this->config->drivers = ["PgSQL", "PDO_PgSQL"];

        $types = [ //! arrays
            $this->util->lang('Numbers') => ["smallint" => 5, "integer" => 10, "bigint" => 19, "boolean" => 1, "numeric" => 0, "real" => 7, "double precision" => 16, "money" => 20],
            $this->util->lang('Date and time') => ["date" => 13, "time" => 17, "timestamp" => 20, "timestamptz" => 21, "interval" => 0],
            $this->util->lang('Strings') => ["character" => 0, "character varying" => 0, "text" => 0, "tsquery" => 0, "tsvector" => 0, "uuid" => 0, "xml" => 0],
            $this->util->lang('Binary') => ["bit" => 0, "bit varying" => 0, "bytea" => 0],
            $this->util->lang('Network') => ["cidr" => 43, "inet" => 43, "macaddr" => 17, "txid_snapshot" => 0],
            $this->util->lang('Geometry') => ["box" => 0, "circle" => 0, "line" => 0, "lseg" => 0, "path" => 0, "point" => 0, "polygon" => 0],
        ];
        foreach ($types as $group => $_types) {
            $this->config->structuredTypes[$group] = array_keys($_types);
            $this->config->types = array_merge($this->config->types, $_types);
        }

        // $this->config->unsigned = [];
        $this->config->operators = ["=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE", "LIKE %%", "ILIKE", "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT IN", "IS NOT NULL"]; // no "SQL" to avoid CSRF
        $this->config->functions = ["char_length", "lower", "round", "to_hex", "to_timestamp", "upper"];
        $this->config->grouping = ["avg", "count", "count distinct", "max", "min", "sum"];
        $this->config->editFunctions = [[
            "char" => "md5",
            "date|time" => "now",
        ],[
            $this->db->numberRegex() => "+/-",
            "date|time" => "+ interval/- interval", //! escape
            "char|text" => "||",
        ]];
    }
}
