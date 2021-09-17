<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Db\Table as AbstractTable;

class Table extends AbstractTable
{
    /**
     * @inheritDoc
     */
    public function tableStatus(string $table = "", bool $fast = false)
    {
        $tables = [];
        $query = "SELECT c.relname AS \"Name\", CASE c.relkind " .
            "WHEN 'r' THEN 'table' WHEN 'm' THEN 'materialized view' ELSE 'view' END AS \"Engine\", " .
            "pg_relation_size(c.oid) AS \"Data_length\", " .
            "pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS \"Index_length\", " .
            "obj_description(c.oid, 'pg_class') AS \"Comment\", " .
            ($this->driver->minVersion(12) ? "''" : "CASE WHEN c.relhasoids THEN 'oid' ELSE '' END") .
            " AS \"Oid\", c.reltuples as \"Rows\", n.nspname FROM pg_class c " .
            "JOIN pg_namespace n ON(n.nspname = current_schema() AND n.oid = c.relnamespace) " .
            "WHERE relkind IN ('r', 'm', 'v', 'f', 'p') " .
            ($table != "" ? "AND relname = " . $this->driver->quote($table) : "ORDER BY relname");
        foreach ($this->driver->rows($query) as $row)
        {
            $status = new TableEntity($row['Name']);
            $status->engine = $row['Engine'];
            $status->schema = $row['nspname'];
            $status->dataLength = $row['Data_length'];
            $status->indexLength = $row['Index_length'];
            $status->oid = $row['Oid'];
            $status->rows = $row['Rows'];
            $status->comment = $row['Comment'];

            //! Index_length, Auto_increment
            if ($table != "") {
                return $status;
            }
            $tables[$row["Name"]] = $status;
        }
        return $tables;
    }

    /**
     * @inheritDoc
     */
    public function isView(TableEntity $tableStatus)
    {
        return in_array($tableStatus->engine, ["view", "materialized view"]);
    }

    /**
     * @inheritDoc
     */
    public function supportForeignKeys(TableEntity $tableStatus)
    {
        return true;
    }


    /**
     * Get the primary key of a table
     * Same as indexes(), but the columns of the primary key are returned in a array
     *
     * @param string $table
     *
     * @return array
     */
    private function primaryKeyColumns(string $table)
    {
        $indexes = [];
        $table_oid = $this->connection->result("SELECT oid FROM pg_class WHERE " .
            "relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) " .
            "AND relname = " . $this->driver->quote($table));
        $columns = $this->driver->keyValues("SELECT attnum, attname FROM pg_attribute WHERE " .
            "attrelid = $table_oid AND attnum > 0");
        foreach ($this->driver->rows("SELECT relname, indisunique::int, indisprimary::int, indkey, " .
            "indoption, (indpred IS NOT NULL)::int as indispartial FROM pg_index i, pg_class ci " .
            "WHERE i.indrelid = $table_oid AND ci.oid = i.indexrelid") as $row)
        {
            $relname = $row["relname"];
            if ($row["indisprimary"]) {
                foreach (explode(" ", $row["indkey"]) as $indkey) {
                    $indexes[] = $columns[$indkey];
                }
            }
        }
        return $indexes;
    }

    /**
     * @inheritDoc
     */
    public function fields(string $table)
    {
        $fields = [];
        $aliases = [
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamptz',
        ];

        // Primary keys
        $primaryKeyColumns = $this->primaryKeyColumns($table);

        $identity_column = $this->driver->minVersion(10) ? 'a.attidentity' : '0';
        $query = "SELECT a.attname AS field, format_type(a.atttypid, a.atttypmod) AS full_type, " .
            "pg_get_expr(d.adbin, d.adrelid) AS default, a.attnotnull::int, " .
            "col_description(c.oid, a.attnum) AS comment, $identity_column AS identity FROM pg_class c " .
            "JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid " .
            "LEFT JOIN pg_attrdef d ON c.oid = d.adrelid AND a.attnum = d.adnum WHERE c.relname = " .
            $this->driver->quote($table) .
            " AND n.nspname = current_schema() AND NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum";
        foreach ($this->driver->rows($query) as $row)
        {
            $field = new TableFieldEntity();

            $field->name = $row["field"];
            $field->primary = \in_array($field->name, $primaryKeyColumns);
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
                    (($match10 == "'" ? $this->driver->unescapeId($match1) : $match1) . $match2));
            }

            $fields[$field->name] = $field;
        }
        return $fields;
    }

    /**
     * @inheritDoc
     */
    public function indexes(string $table, ConnectionInterface $connection = null)
    {
        if (!$connection) {
            $connection = $this->connection;
        }
        $indexes = [];
        $table_oid = $connection->result("SELECT oid FROM pg_class WHERE " .
            "relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) " .
            "AND relname = " . $this->driver->quote($table));
        $columns = $this->driver->keyValues("SELECT attnum, attname FROM pg_attribute WHERE " .
            "attrelid = $table_oid AND attnum > 0", $connection);
        foreach ($this->driver->rows("SELECT relname, indisunique::int, indisprimary::int, indkey, " .
            "indoption, (indpred IS NOT NULL)::int as indispartial FROM pg_index i, pg_class ci " .
            "WHERE i.indrelid = $table_oid AND ci.oid = i.indexrelid", $connection) as $row)
        {
            $index = new IndexEntity();

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

    /**
     * @inheritDoc
     */
    public function foreignKeys(string $table)
    {
        $foreignKeys = [];
        $query = "SELECT conname, condeferrable::int AS deferrable, pg_get_constraintdef(oid) " .
            "AS definition FROM pg_constraint WHERE conrelid = (SELECT pc.oid FROM pg_class AS pc " .
            "INNER JOIN pg_namespace AS pn ON (pn.oid = pc.relnamespace) WHERE pc.relname = " .
            $this->driver->quote($table) .
            " AND pn.nspname = current_schema()) AND contype = 'f'::char ORDER BY conkey, conname";
        foreach ($this->driver->rows($query) as $row) {
            if (preg_match('~FOREIGN KEY\s*\((.+)\)\s*REFERENCES (.+)\((.+)\)(.*)$~iA', $row['definition'], $match)) {
                $match1 = $match[1] ?? '';
                $match2 = $match[2] ?? '';
                $match3 = $match[3] ?? '';
                $match4 = $match[4] ?? '';
                $match11 = '';

                $foreignKey = new ForeignKeyEntity();

                $foreignKey->source = array_map('trim', explode(',', $match1));
                $foreignKey->target = array_map('trim', explode(',', $match3));
                $foreignKey->onDelete = preg_match("~ON DELETE ({$this->driver->onActions})~", $match4, $match10) ? $match11 : 'NO ACTION';
                $foreignKey->onUpdate = preg_match("~ON UPDATE ({$this->driver->onActions})~", $match4, $match10) ? $match11 : 'NO ACTION';

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

    /**
     * @inheritDoc
     */
    public function alterTable(string $table, string $name, array $fields, array $foreign,
        string $comment, string $engine, string $collation, int $autoIncrement, string $partitioning)
    {
        $alter = [];
        $queries = [];
        if ($table != "" && $table != $name) {
            $queries[] = "ALTER TABLE " . $this->driver->table($table) . " RENAME TO " . $this->driver->table($name);
        }
        foreach ($fields as $field) {
            $column = $this->driver->escapeId($field[0]);
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
                        $queries[] = "ALTER TABLE " . $this->driver->table($name) . " RENAME $column TO $val[0]";
                    }
                    $alter[] = "ALTER $column TYPE$val[1]";
                    if (!$val[6]) {
                        $alter[] = "ALTER $column " . ($val[3] ? "SET$val[3]" : "DROP DEFAULT");
                        $alter[] = "ALTER $column " . ($val[2] == " NULL" ? "DROP NOT" : "SET") . $val[2];
                    }
                }
                if ($field[0] != "" || $val5 != "") {
                    $queries[] = "COMMENT ON COLUMN " . $this->driver->table($name) . ".$val[0] IS " . ($val5 != "" ? substr($val5, 9) : "''");
                }
            }
        }
        $alter = array_merge($alter, $foreign);
        if ($table == "") {
            array_unshift($queries, "CREATE TABLE " . $this->driver->table($name) . " (\n" . implode(",\n", $alter) . "\n)");
        } elseif ($alter) {
            array_unshift($queries, "ALTER TABLE " . $this->driver->table($table) . "\n" . implode(",\n", $alter));
        }
        if ($table != "" || $comment != "") {
            $queries[] = "COMMENT ON TABLE " . $this->driver->table($name) . " IS " . $this->driver->quote($comment);
        }
        if ($autoIncrement != "") {
            //! $queries[] = "SELECT setval(pg_get_serial_sequence(" . $this->driver->quote($name) . ", ), $autoIncrement)";
        }
        foreach ($queries as $query) {
            if (!$this->driver->queries($query)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function alterIndexes(string $table, array $alte)
    {
        $create = [];
        $drop = [];
        $queries = [];
        foreach ($alter as $val) {
            if ($val[0] != "INDEX") {
                //! descending UNIQUE indexes results in syntax error
                $create[] = (
                    $val[2] == "DROP" ? "\nDROP CONSTRAINT " . $this->driver->escapeId($val[1]) :
                    "\nADD" . ($val[1] != "" ? " CONSTRAINT " . $this->driver->escapeId($val[1]) : "") .
                    " $val[0] " . ($val[0] == "PRIMARY" ? "KEY " : "") . "(" . implode(", ", $val[2]) . ")"
                );
            } elseif ($val[2] == "DROP") {
                $drop[] = $this->driver->escapeId($val[1]);
            } else {
                $queries[] = "CREATE INDEX " . $this->driver->escapeId($val[1] != "" ? $val[1] : uniqid($table . "_")) .
                    " ON " . $this->driver->table($table) . " (" . implode(", ", $val[2]) . ")";
            }
        }
        if ($create) {
            array_unshift($queries, "ALTER TABLE " . $this->driver->table($table) . implode(",", $create));
        }
        if ($drop) {
            array_unshift($queries, "DROP INDEX " . implode(", ", $drop));
        }
        foreach ($queries as $query) {
            if (!$this->driver->queries($query)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $trigger/*, $table = null*/)
    {
        if ($trigger == "") {
            return ["Statement" => "EXECUTE PROCEDURE ()"];
        }
        // if ($table === null) {
            $table = $this->util->input()->getTable();
        // }
        $query = 'SELECT t.trigger_name AS "Trigger", t.action_timing AS "Timing", ' .
            '(SELECT STRING_AGG(event_manipulation, \' OR \') FROM information_schema.triggers ' .
            'WHERE event_object_table = t.event_object_table AND trigger_name = t.trigger_name ) AS "Events", ' .
            't.event_manipulation AS "Event", \'FOR EACH \' || t.action_orientation AS "Type", ' .
            't.action_statement AS "Statement" FROM information_schema.triggers t WHERE t.event_object_table = ' .
            $this->driver->quote($table) . ' AND t.trigger_name = ' . $this->driver->quote($trigger);
        $rows = $this->driver->rows($query);
        return reset($rows);
    }

    /**
     * @inheritDoc
     */
    public function triggers(string $table)
    {
        $triggers = [];
        $query = "SELECT * FROM information_schema.triggers WHERE trigger_schema = current_schema() " .
            "AND event_object_table = " . $this->driver->quote($table);
        foreach ($this->driver->rows($query) as $row) {
            $triggers[$row["trigger_name"]] = new Trigger($row["action_timing"], $row["event_manipulation"]);
        }
        return $triggers;
    }

    /**
     * @inheritDoc
     */
    public function triggerOptions()
    {
        return [
            "Timing" => ["BEFORE", "AFTER"],
            "Event" => ["INSERT", "UPDATE", "DELETE"],
            "Type" => ["FOR EACH ROW", "FOR EACH STATEMENT"],
        ];
    }

    /**
     * @inheritDoc
     */
    public function tableHelp(string $name)
    {
        $links = array(
            "information_schema" => "infoschema",
            "pg_catalog" => "catalog",
        );
        $link = $links[$this->driver->schema()];
        if ($link) {
            return "$link-" . str_replace("_", "-", $name) . ".html";
        }
    }
}
