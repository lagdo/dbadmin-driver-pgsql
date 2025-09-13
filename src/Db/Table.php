<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;
use Lagdo\DbAdmin\Driver\Db\ConnectionInterface;
use Lagdo\DbAdmin\Driver\Db\Table as AbstractTable;

class Table extends AbstractTable
{
    use TableTrait;

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
     * @inheritDoc
     */
    public function indexes(string $table)
    {
        $indexes = [];
        $table_oid = $this->driver->result("SELECT oid FROM pg_class WHERE " .
            "relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) " .
            "AND relname = " . $this->driver->quote($table));
        $columns = $this->driver->keyValues("SELECT attnum, attname FROM pg_attribute WHERE " .
            "attrelid = $table_oid AND attnum > 0");
        $query = "SELECT relname, indisunique::int, indisprimary::int, indkey, indoption, " .
            "(indpred IS NOT NULL)::int as indispartial FROM pg_index i, pg_class ci " .
            "WHERE i.indrelid = $table_oid AND ci.oid = i.indexrelid";
        foreach ($this->driver->rows($query) as $row)
        {
            $indexes[$row["relname"]] = $this->makeIndexEntity($row, $columns);
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
            $table = $this->utils->input->getTable();
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
