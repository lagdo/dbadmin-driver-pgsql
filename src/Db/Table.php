<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;

use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;

use Lagdo\DbAdmin\Driver\Db\Table as AbstractTable;

class Table extends AbstractTable
{
    /**
     * @param string $table
     *
     * @return array
     */
    private function queryStatus(string $table = '')
    {
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
        return $this->driver->rows($query);
    }

    /**
     * @param array $row
     *
     * @return TableEntity
     */
    private function makeStatus(array $row)
    {
        $status = new TableEntity($row['Name']);
        $status->engine = $row['Engine'];
        $status->schema = $row['nspname'];
        $status->dataLength = $row['Data_length'];
        $status->indexLength = $row['Index_length'];
        $status->oid = $row['Oid'];
        $status->rows = $row['Rows'];
        $status->comment = $row['Comment'];

        return $status;
    }

    /**
     * @inheritDoc
     */
    public function tableStatus(string $table, bool $fast = false)
    {
        $rows = $this->queryStatus($table);
        if (!($row = reset($rows))) {
            return null;
        }
        return $this->makeStatus($row);
    }

    /**
     * @inheritDoc
     */
    public function tableStatuses(bool $fast = false)
    {
        $tables = [];
        $rows = $this->queryStatus();
        foreach ($rows as $row) {
            $tables[$row["Name"]] = $this->makeStatus($row);
        }
        return $tables;
    }

    /**
     * @inheritDoc
     */
    public function tableNames()
    {
        $tables = [];
        $rows = $this->queryStatus();
        foreach ($rows as $row) {
            $tables[] = $row["Name"];
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
     * @inheritDoc
     */
    public function referencableTables(string $table)
    {
        $fields = []; // table_name => [field]
        foreach ($this->tableNames() as $tableName) {
            if ($tableName === $table) {
                continue;
            }
            foreach ($this->fields($tableName) as $field) {
                if ($field->primary) {
                    if (!isset($fields[$tableName])) {
                        $fields[$tableName] = $field;
                    } else {
                        // No multi column primary key
                        $fields[$tableName] = null;
                    }
                }
            }
        }
        return array_filter($fields, function($field) {
            return $field !== null;
        });
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
            // $relname = $row["relname"];
            if ($row["indisprimary"]) {
                foreach (explode(" ", $row["indkey"]) as $indkey) {
                    $indexes[] = $columns[$indkey];
                }
            }
        }
        return $indexes;
    }

    private function getFieldDefault(array $row)
    {
        $default = $row["default"];
        if ($row['identity'] === 'a') {
            $default = 'GENERATED ALWAYS AS IDENTITY';
        }
        if ($row['identity'] === 'd') {
            $default = 'GENERATED BY DEFAULT AS IDENTITY';
        }
        if (preg_match('~(.+)::[^,)]+(.*)~', $row["default"], $match)) {
            $match = array_pad($match, 3, '');
            $default = ($match[1] == "NULL") ? null : ((!empty($match[1]) && $match[1][0] == "'") ?
                $this->driver->unescapeId($match[1]) : $match[1]) . $match[2];
        }
        return $default;
    }

    private function getFieldTypes(array $row)
    {
        $aliases = [
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamptz',
        ];
        preg_match('~([^([]+)(\((.*)\))?([a-z ]+)?((\[[0-9]*])*)$~', $row["full_type"], $match);
        list(, $type, $_length, $length, $addon, $array) = $match;
        $length .= $array;
        $checkType = $type . $addon;
        if (isset($aliases[$checkType])) {
            $type = $aliases[$checkType];
            $fullType = $type . $_length . $array;
            return [$length, $type, $fullType];
        }
        $fullType = $type . $_length . $addon . $array;
        return [$length, $type, $fullType];
    }

    /**
     * @param array $row
     * @param array $primaryKeyColumns
     *
     * @return TableFieldEntity
     */
    private function makeFieldEntity(array $row, array $primaryKeyColumns)
    {
        $field = new TableFieldEntity();

        $field->name = $row["field"];
        $field->primary = \in_array($field->name, $primaryKeyColumns);
        $field->fullType = $row["full_type"];
        $field->default = $this->getFieldDefault($row);
        $field->comment = $row["comment"];
        //! No collation, no info about primary keys
        list($field->length, $field->type, $field->fullType) = $this->getFieldTypes($row);
        $field->null = !$row["attnotnull"];
        $field->autoIncrement = $row['identity'] || preg_match('~^nextval\(~i', $row["default"]);
        $field->privileges = ["insert" => 1, "select" => 1, "update" => 1];
        return $field;
    }

    /**
     * @inheritDoc
     */
    public function fields(string $table)
    {
        $fields = [];

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
            $fields[$row["field"]] = $this->makeFieldEntity($row, $primaryKeyColumns);
        }
        return $fields;
    }

    /**
     * @param array $row
     *
     * @return string
     */
    private function getIndexType(array $row)
    {
        if ($row['indispartial']) {
            return 'INDEX';
        }
        if ($row['indisprimary']) {
            return 'PRIMARY';
        }
        if ($row['indisunique']) {
            return 'UNIQUE';
        }
        return 'INDEX';
    }

    /**
     * @param array $row
     * @param array $columns
     *
     * @return IndexEntity
     */
    private function makeIndexEntity(array $row, array $columns)
    {
        $index = new IndexEntity();

        $index->type = $this->getIndexType($row);
        $index->columns = [];
        foreach (explode(' ', $row['indkey']) as $indkey) {
            $index->columns[] = $columns[$indkey];
        }
        $index->descs = [];
        foreach (explode(' ', $row['indoption']) as $indoption) {
            $index->descs[] = ($indoption & 1 ? '1' : null); // 1 - INDOPTION_DESC
        }
        $index->lengths = [];

        return $index;
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
        $query = "SELECT relname, indisunique::int, indisprimary::int, indkey, indoption, " .
            "(indpred IS NOT NULL)::int as indispartial FROM pg_index i, pg_class ci " .
            "WHERE i.indrelid = $table_oid AND ci.oid = i.indexrelid";
        foreach ($this->driver->rows($query, $connection) as $row)
        {
            $indexes[$row["relname"]] = $this->makeIndexEntity($row, $columns);
        }
        return $indexes;
    }

    /**
     * @param array $row
     *
     * @return ForeignKeyEntity
     */
    private function makeForeignKeyEntity(array $row)
    {
        if (!preg_match('~FOREIGN KEY\s*\((.+)\)\s*REFERENCES (.+)\((.+)\)(.*)$~iA', $row['definition'], $match)) {
            return null;
        }
        $onActions = $this->driver->actions();
        $match = array_pad($match, 5, '');

        $foreignKey = new ForeignKeyEntity();

        $foreignKey->source = array_map('trim', explode(',', $match[1]));
        $foreignKey->target = array_map('trim', explode(',', $match[3]));

        if (preg_match('~^(("([^"]|"")+"|[^"]+)\.)?"?("([^"]|"")+"|[^"]+)$~', $match[2], $match2)) {
            $match2 = array_pad($match2, 5, '');
            $foreignKey->schema = str_replace('""', '"', preg_replace('~^"(.+)"$~', '\1', $match2[2]));
            $foreignKey->table = str_replace('""', '"', preg_replace('~^"(.+)"$~', '\1', $match2[4]));
        }

        $foreignKey->onDelete = preg_match("~ON DELETE ($onActions)~", $match[4], $match2) ? $match2[1] : 'NO ACTION';
        $foreignKey->onUpdate = preg_match("~ON UPDATE ($onActions)~", $match[4], $match2) ? $match2[1] : 'NO ACTION';

        return $foreignKey;
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
            $foreignKey = $this->makeForeignKeyEntity($row);
            if ($foreignKey !== null) {
                $foreignKeys[$row['conname']] = $foreignKey;
            }
        }
        return $foreignKeys;
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $name, string $table = '')
    {
        if ($name == '') {
            return new TriggerEntity('', '', 'EXECUTE PROCEDURE ()');
        }
        if ($table === '') {
            $table = $this->util->input()->getTable();
        }
        $query = 'SELECT t.trigger_name AS "Trigger", t.action_timing AS "Timing", ' .
            '(SELECT STRING_AGG(event_manipulation, \' OR \') FROM information_schema.triggers ' .
            'WHERE event_object_table = t.event_object_table AND trigger_name = t.trigger_name ) AS "Events", ' .
            't.event_manipulation AS "Event", \'FOR EACH \' || t.action_orientation AS "Type", ' .
            't.action_statement AS "Statement" FROM information_schema.triggers t WHERE t.event_object_table = ' .
            $this->driver->quote($table) . ' AND t.trigger_name = ' . $this->driver->quote($name);
        $rows = $this->driver->rows($query);
        if (!($row = reset($rows))) {
            return null;
        }
        return new TriggerEntity($row['Timing'], $row['Event'], $row['Statement'], '', $row['Trigger']);
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
            $triggers[$row["trigger_name"]] = new TriggerEntity($row["action_timing"],
                $row["event_manipulation"], '', '', $row["trigger_name"]);
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
        $links = [
            "information_schema" => "infoschema",
            "pg_catalog" => "catalog",
        ];
        $link = $links[$this->driver->schema()];
        if ($link) {
            return "$link-" . str_replace("_", "-", $name) . ".html";
        }
    }
}
