<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Db\Server as AbstractServer;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    public function getName()
    {
        return "PostgreSQL";
    }

    /**
     * @inheritDoc
     */
    protected function createConnection()
    {
        if (($this->connection)) {
            // Do not create if it already exists
            return;
        }

        if (extension_loaded("pgsql")) {
            $this->connection = new PgSql\Connection($this->db, $this->util, $this, 'PgSQL');
        }
        elseif (extension_loaded("pdo_pgsql")) {
            $this->connection = new Pdo\Connection($this->db, $this->util, $this, 'PDO_PgSQL');
        }

        if($this->connection !== null) {
            $this->driver = new Driver($this->db, $this->util, $this, $this->connection);
        }
    }

    /**
     * @inheritDoc
     */
    public function getConnection()
    {
        if (!$this->connection) {
            return null;
        }

        list($server, $options) = $this->db->getOptions();
        if (!$this->connection->open($server, $options)) {
            return $this->connection->error;
        }

        if ($this->min_version(9, 0)) {
            $this->connection->query("SET application_name = 'Adminer'");
            if ($this->min_version(9.2, 0)) {
                $this->structured_types[$this->util->lang('Strings')][] = "json";
                $this->types["json"] = 4294967295;
                if ($this->min_version(9.4, 0)) {
                    $this->structured_types[$this->util->lang('Strings')][] = "jsonb";
                    $this->types["jsonb"] = 4294967295;
                }
            }
        }
        return $this->connection;
    }

    /**
     * @inheritDoc
     */
    public function idf_escape($idf)
    {
        return '"' . str_replace('"', '""', $idf) . '"';
    }

    public function get_databases($flush)
    {
        return $this->db->get_vals("SELECT datname FROM pg_database WHERE has_database_privilege(datname, 'CONNECT') ORDER BY datname");
    }

    public function limit($query, $where, $limit, $offset = 0, $separator = " ")
    {
        return " $query$where" . ($limit !== null ? $separator . "LIMIT $limit" . ($offset ? " OFFSET $offset" : "") : "");
    }

    public function limit1($table, $query, $where, $separator = "\n")
    {
        return (preg_match('~^INTO~', $query) ? $this->limit($query, $where, 1, 0, $separator) :
            " $query" . ($this->is_view($this->table_status1($table)) ? $where :
            " WHERE ctid = (SELECT ctid FROM " . $this->table($table) . $where . $separator . "LIMIT 1)")
        );
    }

    public function db_collation($db, $collations)
    {
        return $this->connection->result("SELECT datcollate FROM pg_database WHERE datname = " . $this->q($db));
    }

    public function logged_user()
    {
        return $this->connection->result("SELECT user");
    }

    public function tables_list()
    {
        $query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()";
        if ($this->support('materializedview')) {
            $query .= "
UNION ALL
SELECT matviewname, 'MATERIALIZED VIEW'
FROM pg_matviews
WHERE schemaname = current_schema()";
        }
        $query .= "
ORDER BY 1";
        return $this->db->get_key_vals($query);
    }

    public function count_tables($databases)
    {
        return []; // would require reconnect
    }

    public function table_status($name = "", $fast = false)
    {
        $return = [];
        foreach ($this->db->get_rows(
            "SELECT c.relname AS \"Name\", CASE c.relkind WHEN 'r' THEN 'table' WHEN 'm' THEN 'materialized view' ELSE 'view' END AS \"Engine\", pg_relation_size(c.oid) AS \"Data_length\", pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS \"Index_length\", obj_description(c.oid, 'pg_class') AS \"Comment\", " . ($this->min_version(12) ? "''" : "CASE WHEN c.relhasoids THEN 'oid' ELSE '' END") . " AS \"Oid\", c.reltuples as \"Rows\", n.nspname
FROM pg_class c
JOIN pg_namespace n ON(n.nspname = current_schema() AND n.oid = c.relnamespace)
WHERE relkind IN ('r', 'm', 'v', 'f', 'p')
" . ($name != "" ? "AND relname = " . $this->q($name) : "ORDER BY relname")
        ) as $row) { //! Index_length, Auto_increment
            $return[$row["Name"]] = $row;
        }
        return ($name != "" ? $return[$name] : $return);
    }

    public function is_view($table_status)
    {
        return in_array($table_status["Engine"], array("view", "materialized view"));
    }

    public function fk_support($table_status)
    {
        return true;
    }

    public function fields($table)
    {
        $return = [];
        $aliases = array(
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamptz',
        );

        $identity_column = $this->min_version(10) ? 'a.attidentity' : '0';

        foreach ($this->db->get_rows(
            "SELECT a.attname AS field, format_type(a.atttypid, a.atttypmod) AS full_type, " .
            "pg_get_expr(d.adbin, d.adrelid) AS default, a.attnotnull::int, " .
            "col_description(c.oid, a.attnum) AS comment, $identity_column AS identity
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_attribute a ON c.oid = a.attrelid
LEFT JOIN pg_attrdef d ON c.oid = d.adrelid AND a.attnum = d.adnum
WHERE c.relname = " . $this->q($table) . "
AND n.nspname = current_schema()
AND NOT a.attisdropped
AND a.attnum > 0
ORDER BY a.attnum"
        ) as $row) {
            //! collation, primary
            preg_match('~([^([]+)(\((.*)\))?([a-z ]+)?((\[[0-9]*])*)$~', $row["full_type"], $match);
            list(, $type, $length, $row["length"], $addon, $array) = $match;
            $row["length"] .= $array;
            $check_type = $type . $addon;
            if (isset($aliases[$check_type])) {
                $row["type"] = $aliases[$check_type];
                $row["full_type"] = $row["type"] . $length . $array;
            } else {
                $row["type"] = $type;
                $row["full_type"] = $row["type"] . $length . $addon . $array;
            }
            if (in_array($row['identity'], array('a', 'd'))) {
                $row['default'] = 'GENERATED ' . ($row['identity'] == 'd' ? 'BY DEFAULT' : 'ALWAYS') . ' AS IDENTITY';
            }
            $row["null"] = !$row["attnotnull"];
            $row["auto_increment"] = $row['identity'] || preg_match('~^nextval\(~i', $row["default"]);
            $row["privileges"] = array("insert" => 1, "select" => 1, "update" => 1);
            if (preg_match('~(.+)::[^,)]+(.*)~', $row["default"], $match)) {
                $match1 = $match[1] ?? '';
                $match10 = $match1[0] ?? '';
                $match2 = $match[2] ?? '';
                $row["default"] = ($match1 == "NULL" ? null :
                    (($match10 == "'" ? $this->idf_unescape($match1) : $match1) . $match2));
            }
            $return[$row["field"]] = $row;
        }
        return $return;
    }

    public function indexes($table, $connection2 = null)
    {
        if (!is_object($connection2)) {
            $connection2 = $this->connection;
        }
        $return = [];
        $table_oid = $connection2->result("SELECT oid FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND relname = " . $this->q($table));
        $columns = $this->db->get_key_vals("SELECT attnum, attname FROM pg_attribute WHERE attrelid = $table_oid AND attnum > 0", $connection2);
        foreach ($this->db->get_rows("SELECT relname, indisunique::int, indisprimary::int, indkey, indoption, (indpred IS NOT NULL)::int as indispartial FROM pg_index i, pg_class ci WHERE i.indrelid = $table_oid AND ci.oid = i.indexrelid", $connection2) as $row) {
            $relname = $row["relname"];
            $return[$relname]["type"] = ($row["indispartial"] ? "INDEX" : ($row["indisprimary"] ? "PRIMARY" : ($row["indisunique"] ? "UNIQUE" : "INDEX")));
            $return[$relname]["columns"] = [];
            foreach (explode(" ", $row["indkey"]) as $indkey) {
                $return[$relname]["columns"][] = $columns[$indkey];
            }
            $return[$relname]["descs"] = [];
            foreach (explode(" ", $row["indoption"]) as $indoption) {
                $return[$relname]["descs"][] = ($indoption & 1 ? '1' : null); // 1 - INDOPTION_DESC
            }
            $return[$relname]["lengths"] = [];
        }
        return $return;
    }

    public function foreign_keys($table)
    {
        $return = [];
        foreach ($this->db->get_rows("SELECT conname, condeferrable::int AS deferrable, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = (SELECT pc.oid FROM pg_class AS pc INNER JOIN pg_namespace AS pn ON (pn.oid = pc.relnamespace) WHERE pc.relname = " . $this->q($table) . " AND pn.nspname = current_schema())
AND contype = 'f'::char
ORDER BY conkey, conname") as $row) {
            if (preg_match('~FOREIGN KEY\s*\((.+)\)\s*REFERENCES (.+)\((.+)\)(.*)$~iA', $row['definition'], $match)) {
                $match1 = $match[1] ?? '';
                $match2 = $match[2] ?? '';
                $match3 = $match[3] ?? '';
                $match4 = $match[4] ?? '';
                $match11 = '';
                $row['source'] = array_map('trim', explode(',', $match1));
                if (preg_match('~^(("([^"]|"")+"|[^"]+)\.)?"?("([^"]|"")+"|[^"]+)$~', $match2, $match10)) {
                    $match11 = $match10[1] ?? '';
                    $match12 = $match10[2] ?? '';
                    // $match13 = $match10[3] ?? '';
                    $match14 = $match10[4] ?? '';
                    $row['ns'] = str_replace('""', '"', preg_replace('~^"(.+)"$~', '\1', $match12));
                    $row['table'] = str_replace('""', '"', preg_replace('~^"(.+)"$~', '\1', $match14));
                }
                $row['target'] = array_map('trim', explode(',', $match3));
                $row['on_delete'] = (preg_match("~ON DELETE ({$this->on_actions})~", $match4, $match10) ? $match11 : 'NO ACTION');
                $row['on_update'] = (preg_match("~ON UPDATE ({$this->on_actions})~", $match4, $match10) ? $match11 : 'NO ACTION');
                $return[$row['conname']] = $row;
            }
        }
        return $return;
    }

    public function constraints($table)
    {
        $return = [];
        foreach ($this->db->get_rows("SELECT conname, consrc
FROM pg_catalog.pg_constraint
INNER JOIN pg_catalog.pg_namespace ON pg_constraint.connamespace = pg_namespace.oid
INNER JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid AND pg_constraint.connamespace = pg_class.relnamespace
WHERE pg_constraint.contype = 'c'
AND conrelid != 0 -- handle only CONSTRAINTs here, not TYPES
AND nspname = current_schema()
AND relname = " . $this->q($table) . "
ORDER BY connamespace, conname") as $row) {
            $return[$row['conname']] = $row['consrc'];
        }
        return $return;
    }

    public function view($name)
    {
        return array("select" => trim($this->connection->result("SELECT pg_get_viewdef(" .
            $this->connection->result("SELECT oid FROM pg_class WHERE relnamespace = " .
            "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND relname = " .
            $this->q($name)) . ")")));
    }

    public function collations()
    {
        //! supported in CREATE DATABASE
        return [];
    }

    public function information_schema($db)
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
        return $this->util->nl_br($return);
    }

    public function create_database($db, $collation)
    {
        return $this->db->queries("CREATE DATABASE " . $this->idf_escape($db) .
            ($collation ? " ENCODING " . $this->idf_escape($collation) : ""));
    }

    public function drop_databases($databases)
    {
        $this->connection->close();
        return $this->db->apply_queries("DROP DATABASE", $databases, function ($database) {
            return $this->idf_escape($database);
        });
    }

    public function rename_database($name, $collation)
    {
        //! current database cannot be renamed
        return $this->db->queries("ALTER DATABASE " . $this->idf_escape($this->current_db()) .
            " RENAME TO " . $this->idf_escape($name));
    }

    public function alter_table($table, $name, $fields, $foreign, $comment, $engine, $collation, $auto_increment, $partitioning)
    {
        $alter = [];
        $queries = [];
        if ($table != "" && $table != $name) {
            $queries[] = "ALTER TABLE " . $this->table($table) . " RENAME TO " . $this->table($name);
        }
        foreach ($fields as $field) {
            $column = $this->idf_escape($field[0]);
            $val = $field[1];
            if (!$val) {
                $alter[] = "DROP $column";
            } else {
                $val5 = $val[5];
                unset($val[5]);
                if ($field[0] == "") {
                    if (isset($val[6])) { // auto_increment
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
            $queries[] = "COMMENT ON TABLE " . $this->table($name) . " IS " . $this->q($comment);
        }
        if ($auto_increment != "") {
            //! $queries[] = "SELECT setval(pg_get_serial_sequence(" . $this->q($name) . ", ), $auto_increment)";
        }
        foreach ($queries as $query) {
            if (!$this->db->queries($query)) {
                return false;
            }
        }
        return true;
    }

    public function alter_indexes($table, $alter)
    {
        $create = [];
        $drop = [];
        $queries = [];
        foreach ($alter as $val) {
            if ($val[0] != "INDEX") {
                //! descending UNIQUE indexes results in syntax error
                $create[] = (
                    $val[2] == "DROP"
                    ? "\nDROP CONSTRAINT " . $this->idf_escape($val[1])
                    : "\nADD" . ($val[1] != "" ? " CONSTRAINT " . $this->idf_escape($val[1]) : "") . " $val[0] " . ($val[0] == "PRIMARY" ? "KEY " : "") . "(" . implode(", ", $val[2]) . ")"
                );
            } elseif ($val[2] == "DROP") {
                $drop[] = $this->idf_escape($val[1]);
            } else {
                $queries[] = "CREATE INDEX " . $this->idf_escape($val[1] != "" ? $val[1] : uniqid($table . "_")) . " ON " . $this->table($table) . " (" . implode(", ", $val[2]) . ")";
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

    public function truncate_tables($tables)
    {
        return $this->db->queries("TRUNCATE " . implode(", ", array_map(function ($table) {
            return $this->table($table);
        }, $tables)));
        return true;
    }

    public function drop_views($views)
    {
        return $this->drop_tables($views);
    }

    public function drop_tables($tables)
    {
        foreach ($tables as $table) {
            $status = $this->table_status($table);
            if (!$this->db->queries("DROP " . strtoupper($status["Engine"]) . " " . $this->table($table))) {
                return false;
            }
        }
        return true;
    }

    public function move_tables($tables, $views, $target)
    {
        foreach (array_merge($tables, $views) as $table) {
            $status = $this->table_status($table);
            if (!$this->db->queries("ALTER " . strtoupper($status["Engine"]) . " " .
                $this->table($table) . " SET SCHEMA " . $this->idf_escape($target))) {
                return false;
            }
        }
        return true;
    }

    public function trigger($name, $table = null)
    {
        if ($name == "") {
            return array("Statement" => "EXECUTE PROCEDURE ()");
        }
        if ($table === null) {
            $table = $this->util->input()->getTable();
        }
        $rows = $this->db->get_rows('SELECT t.trigger_name AS "Trigger", t.action_timing AS "Timing", ' .
            '(SELECT STRING_AGG(event_manipulation, \' OR \') FROM information_schema.triggers ' .
            'WHERE event_object_table = t.event_object_table AND trigger_name = t.trigger_name ) AS ' .
            '"Events", t.event_manipulation AS "Event", \'FOR EACH \' || t.action_orientation AS ' .
            '"Type", t.action_statement AS "Statement" FROM information_schema.triggers t WHERE ' .
            't.event_object_table = ' . $this->q($table) . ' AND t.trigger_name = ' . $this->q($name));
        return reset($rows);
    }

    public function triggers($table)
    {
        $return = [];
        foreach ($this->db->get_rows("SELECT * FROM information_schema.triggers " .
            "WHERE trigger_schema = current_schema() AND event_object_table = " . $this->q($table)) as $row) {
            $return[$row["trigger_name"]] = array($row["action_timing"], $row["event_manipulation"]);
        }
        return $return;
    }

    public function trigger_options()
    {
        return array(
            "Timing" => array("BEFORE", "AFTER"),
            "Event" => array("INSERT", "UPDATE", "DELETE"),
            "Type" => array("FOR EACH ROW", "FOR EACH STATEMENT"),
        );
    }

    public function routine($name, $type)
    {
        $rows = $this->db->get_rows('SELECT routine_definition AS definition, LOWER(external_language) AS language, *
FROM information_schema.routines
WHERE routine_schema = current_schema() AND specific_name = ' . $this->q($name));
        $return = $rows[0];
        $return["returns"] = array("type" => $return["type_udt_name"]);
        $return["fields"] = $this->db->get_rows('SELECT parameter_name AS field, data_type AS type, character_maximum_length AS length, parameter_mode AS inout
FROM information_schema.parameters
WHERE specific_schema = current_schema() AND specific_name = ' . $this->q($name) . '
ORDER BY ordinal_position');
        return $return;
    }

    public function routines()
    {
        return $this->db->get_rows('SELECT specific_name AS "SPECIFIC_NAME", routine_type AS "ROUTINE_TYPE", routine_name AS "ROUTINE_NAME", type_udt_name AS "DTD_IDENTIFIER"
FROM information_schema.routines
WHERE routine_schema = current_schema()
ORDER BY SPECIFIC_NAME');
    }

    public function routine_languages()
    {
        return $this->db->get_vals("SELECT LOWER(lanname) FROM pg_catalog.pg_language");
    }

    public function routine_id($name, $row)
    {
        $return = [];
        foreach ($row["fields"] as $field) {
            $return[] = $field["type"];
        }
        return $this->idf_escape($name) . "(" . implode(", ", $return) . ")";
    }

    public function last_id()
    {
        return 0; // there can be several sequences
    }

    public function explain($connection, $query)
    {
        return $connection->query("EXPLAIN $query");
    }

    public function found_rows($table_status, $where)
    {
        if (preg_match(
            "~ rows=([0-9]+)~",
            $this->connection->result("EXPLAIN SELECT * FROM " . $this->idf_escape($table_status["Name"]) . ($where ? " WHERE " . implode(" AND ", $where) : "")),
            $regs
        )) {
            return $regs[1];
        }
        return false;
    }

    public function user_types()
    {
        return $this->db->get_vals(
            "SELECT typname
FROM pg_type
WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
AND typtype IN ('b','d','e')
AND typelem = 0"
        );
    }

    public function schemas()
    {
        return $this->db->get_vals("SELECT nspname FROM pg_namespace ORDER BY nspname");
    }

    public function get_schema()
    {
        return $this->connection->result("SELECT current_schema()");
    }

    public function set_schema($schema, $connection2 = null)
    {
        if (!$connection2) {
            $connection2 = $this->connection;
        }
        $return = $connection2->query("SET search_path TO " . $this->idf_escape($schema));
        foreach ($this->user_types() as $type) { //! get types from current_schemas('t')
            if (!isset($this->types[$type])) {
                $this->types[$type] = 0;
                $this->structured_types[$this->util->lang('User types')][] = $type;
            }
        }
        return $return;
    }

    /**
     * @inheritDoc
     */
    public function foreign_keys_sql($table)
    {
        $return = "";

        $status = $this->table_status($table);
        $fkeys = $this->foreign_keys($table);
        ksort($fkeys);

        foreach ($fkeys as $fkey_name => $fkey) {
            $return .= "ALTER TABLE ONLY " . $this->idf_escape($status['nspname']) . "." .
            $this->idf_escape($status['Name']) . " ADD CONSTRAINT " . $this->idf_escape($fkey_name) .
            " $fkey[definition] " . ($fkey['deferrable'] ? 'DEFERRABLE' : 'NOT DEFERRABLE') . ";\n";
        }

        return ($return ? "$return\n" : $return);
    }

    public function create_sql($table, $auto_increment, $style)
    {
        $return = '';
        $return_parts = [];
        $sequences = [];

        $status = $this->table_status($table);
        if ($this->is_view($status)) {
            $view = $this->view($table);
            return rtrim("CREATE VIEW " . $this->idf_escape($table) . " AS $view[select]", ";");
        }
        $fields = $this->fields($table);
        $indexes = $this->indexes($table);
        ksort($indexes);
        $constraints = $this->constraints($table);

        if (!$status || empty($fields)) {
            return false;
        }

        $return = "CREATE TABLE " . $this->idf_escape($status['nspname']) . "." . $this->idf_escape($status['Name']) . " (\n    ";

        // fields' definitions
        foreach ($fields as $field_name => $field) {
            $part = $this->idf_escape($field['field']) . ' ' . $field['full_type'] .
                $this->db->default_value($field) . ($field['attnotnull'] ? " NOT NULL" : "");
            $return_parts[] = $part;

            // sequences for fields
            if (preg_match('~nextval\(\'([^\']+)\'\)~', $field['default'], $matches)) {
                $sequence_name = $matches[1];
                $sq = reset($this->db->get_rows(
                    $this->min_version(10) ?
                    "SELECT *, cache_size AS cache_value FROM pg_sequences WHERE schemaname = current_schema() AND sequencename = " .
                    $this->q($sequence_name) : "SELECT * FROM $sequence_name"
                ));
                $sequences[] = ($style == "DROP+CREATE" ? "DROP SEQUENCE IF EXISTS $sequence_name;\n" : "") .
                    "CREATE SEQUENCE $sequence_name INCREMENT $sq[increment_by] MINVALUE $sq[min_value] MAXVALUE $sq[max_value]" .
                    ($auto_increment && $sq['last_value'] ? " START $sq[last_value]" : "") . " CACHE $sq[cache_value];";
            }
        }

        // adding sequences before table definition
        if (!empty($sequences)) {
            $return = implode("\n\n", $sequences) . "\n\n$return";
        }

        // primary + unique keys
        foreach ($indexes as $index_name => $index) {
            switch ($index['type']) {
                case 'UNIQUE':
                    $return_parts[] = "CONSTRAINT " . $this->idf_escape($index_name) .
                        " UNIQUE (" . implode(', ', array_map(function ($column) {
                            return $this->idf_escape($column);
                        }, $index['columns'])) . ")";
                    break;
                case 'PRIMARY':
                    $return_parts[] = "CONSTRAINT " . $this->idf_escape($index_name) .
                        " PRIMARY KEY (" . implode(', ', array_map(function ($column) {
                            return $this->idf_escape($column);
                        }, $index['columns'])) . ")";
                    break;
            }
        }

        foreach ($constraints as $conname => $consrc) {
            $return_parts[] = "CONSTRAINT " . $this->idf_escape($conname) . " CHECK $consrc";
        }

        $return .= implode(",\n    ", $return_parts) . "\n) WITH (oids = " . ($status['Oid'] ? 'true' : 'false') . ");";

        // "basic" indexes after table definition
        foreach ($indexes as $index_name => $index) {
            if ($index['type'] == 'INDEX') {
                $columns = [];
                foreach ($index['columns'] as $key => $val) {
                    $columns[] = $this->idf_escape($val) . ($index['descs'][$key] ? " DESC" : "");
                }
                $return .= "\n\nCREATE INDEX " . $this->idf_escape($index_name) . " ON " .
                    $this->idf_escape($status['nspname']) . "." . $this->idf_escape($status['Name']) .
                    " USING btree (" . implode(', ', $columns) . ");";
            }
        }

        // coments for table & fields
        if ($status['Comment']) {
            $return .= "\n\nCOMMENT ON TABLE " . $this->idf_escape($status['nspname']) . "." .
                $this->idf_escape($status['Name']) . " IS " . $this->q($status['Comment']) . ";";
        }

        foreach ($fields as $field_name => $field) {
            if ($field['comment']) {
                $return .= "\n\nCOMMENT ON COLUMN " . $this->idf_escape($status['nspname']) . "." .
                    $this->idf_escape($status['Name']) . "." . $this->idf_escape($field_name) .
                    " IS " . $this->q($field['comment']) . ";";
            }
        }

        return rtrim($return, ';');
    }

    public function truncate_sql($table)
    {
        return "TRUNCATE " . $this->table($table);
    }

    public function trigger_sql($table)
    {
        $status = $this->table_status($table);
        $return = "";
        foreach ($this->triggers($table) as $trg_id => $trg) {
            $trigger = $this->trigger($trg_id, $status['Name']);
            $return .= "\nCREATE TRIGGER " . $this->idf_escape($trigger['Trigger']) .
                " $trigger[Timing] $trigger[Events] ON " . $this->idf_escape($status["nspname"]) . "." .
                $this->idf_escape($status['Name']) . " $trigger[Type] $trigger[Statement];;\n";
        }
        return $return;
    }


    public function use_sql($database)
    {
        return "\connect " . $this->idf_escape($database);
    }

    public function show_variables()
    {
        return $this->db->get_key_vals("SHOW ALL");
    }

    public function process_list()
    {
        return $this->db->get_rows("SELECT * FROM pg_stat_activity ORDER BY " . ($this->min_version(9.2) ? "pid" : "procpid"));
    }

    public function show_status()
    {
    }

    public function support($feature)
    {
        return preg_match('~^(database|table|columns|sql|indexes|descidx|comment|view|' .
            ($this->min_version(9.3) ? 'materializedview|' : '') .
            'scheme|routine|processlist|sequence|trigger|type|variables|drop_col|kill|dump|fkeys_sql)$~', $feature);
    }

    public function kill_process($val)
    {
        return $this->db->queries("SELECT pg_terminate_backend(" . $this->util->number($val) . ")");
    }

    public function connection_id()
    {
        return "SELECT pg_backend_pid()";
    }

    public function max_connections()
    {
        return $this->connection->result("SHOW max_connections");
    }

    public function driver_config()
    {
        $types = [];
        $structured_types = [];
        foreach (array( //! arrays
            $this->util->lang('Numbers') => array("smallint" => 5, "integer" => 10, "bigint" => 19, "boolean" => 1, "numeric" => 0, "real" => 7, "double precision" => 16, "money" => 20),
            $this->util->lang('Date and time') => array("date" => 13, "time" => 17, "timestamp" => 20, "timestamptz" => 21, "interval" => 0),
            $this->util->lang('Strings') => array("character" => 0, "character varying" => 0, "text" => 0, "tsquery" => 0, "tsvector" => 0, "uuid" => 0, "xml" => 0),
            $this->util->lang('Binary') => array("bit" => 0, "bit varying" => 0, "bytea" => 0),
            $this->util->lang('Network') => array("cidr" => 43, "inet" => 43, "macaddr" => 17, "txid_snapshot" => 0),
            $this->util->lang('Geometry') => array("box" => 0, "circle" => 0, "line" => 0, "lseg" => 0, "path" => 0, "point" => 0, "polygon" => 0),
        ) as $key => $val) { //! can be retrieved from pg_type
            $types += $val;
            $structured_types[$key] = array_keys($val);
        }
        return array(
            'possible_drivers' => array("PgSQL", "PDO_PgSQL"),
            'jush' => "pgsql",
            'types' => $types,
            'structured_types' => $structured_types,
            'unsigned' => [],
            'operators' => array("=", "<", ">", "<=", ">=", "!=", "~", "!~", "LIKE", "LIKE %%", "ILIKE", "ILIKE %%", "IN", "IS NULL", "NOT LIKE", "NOT IN", "IS NOT NULL"), // no "SQL" to avoid CSRF
            'functions' => array("char_length", "lower", "round", "to_hex", "to_timestamp", "upper"),
            'grouping' => array("avg", "count", "count distinct", "max", "min", "sum"),
            'edit_functions' => array(
                array(
                    "char" => "md5",
                    "date|time" => "now",
                ), array(
                    $this->db->number_type() => "+/-",
                    "date|time" => "+ interval/- interval", //! escape
                    "char|text" => "||",
                )
            ),
        );
    }
}
