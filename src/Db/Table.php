<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Db\AbstractTable;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\PartitionEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TriggerEntity;

use function array_filter;
use function array_map;
use function array_pad;
use function explode;
use function implode;
use function in_array;
use function intval;
use function preg_match;
use function preg_replace;
use function preg_split;
use function str_replace;

class Table extends AbstractTable
{
    use Traits\TableOidTrait;

    /**
     * @param string $table
     *
     * @return array
     */
    private function queryStatus(string $table = ''): array
    {
        $query = "SELECT c.relname AS \"Name\", CASE c.relkind " .
            "WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE 'table' END AS \"Engine\", " .
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
    private function makeStatus(array $row): TableEntity
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
     * @param array $row
     *
     * @return string
     */
    private function getIndexType(array $row): string
    {
        if ($row['partial']) {
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
    private function makeIndexEntity(array $row, array $columns): IndexEntity
    {
        $index = new IndexEntity();

        $index->type = $this->getIndexType($row);
        $index->name = $row["relname"];
        $index->algorithm = $row["amname"];
		$index->partial = $row["partial"];
        $indexpr = preg_split('~(?<=\)), (?=\()~', $row["indexpr"] ?? ''); //! '), (' used in expression
        foreach (explode(" ", $row["indkey"]) as $indkey) {
            $index->columns[] = ($indkey ? $columns[$indkey] : array_shift($indexpr));
        }
        foreach (explode(" ", $row["indoption"]) as $indoption) {
            $index->descs[] = intval($indoption) & 1 ? '1' : null; // 1 - INDOPTION_DESC
        }
        // $index->lengths = [];

        return $index;
    }

    /**
     * @param array $row
     *
     * @return ForeignKeyEntity|null
     */
    private function makeForeignKeyEntity(array $row): ForeignKeyEntity|null
    {
        if (!preg_match('~FOREIGN KEY\s*\((.+)\)\s*REFERENCES (.+)\((.+)\)(.*)$~iA', $row['definition'], $match)) {
            return null;
        }

        $onActions = $this->driver->actions();
        $match = array_pad($match, 5, '');

        $foreignKey = new ForeignKeyEntity();
        $foreignKey->definition = $row['definition'];
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
    public function tableStatus(string $table, bool $fast = false): TableEntity|null
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
    public function tableStatuses(bool $fast = false): array
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
    public function tableNames(): array
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
    public function isView(TableEntity $tableStatus): bool
    {
        return in_array($tableStatus->engine, ["view", "materialized view"]);
    }

    /**
     * @inheritDoc
     */
    public function supportForeignKeys(TableEntity $tableStatus): bool
    {
        return true;
    }

    /**
     * @inheritDoc
     */
    public function referencableTables(string $table): array
    {
        $fields = []; // table_name => [field]
        foreach ($this->tableNames() as $tableName) {
            if ($tableName === $table) {
                continue;
            }
            foreach ($this->fields($tableName) as $field) {
                if ($field->primary) {
                    // No multi column primary key
                    $fields[$tableName] = !isset($fields[$tableName]) ? $field : null;
                }
            }
        }
        return array_filter($fields, fn($field) => $field !== null);
    }

    /**
     * @param array $row
     *
     * @return array
     */
    private function getFieldTypes(array $row): array
    {
        $aliases = [
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamptz',
        ];
        //! collation, primary
        preg_match('~([^([]+)(\((.*)\))?([a-z ]+)?((\[[0-9]*])*)$~', $row["full_type"], $match);
        // [, $type, $typeLength, $length, $addon, $array] = $match;
        $type = $match[1] ?? '';
        $typeLength = $match[2] ?? '';
        $length = $match[3] ?? '';
        $addon = $match[4] ?? '';
        $array = $match[5] ?? '';

        $checkType = "$type$addon";
        if (isset($aliases[$checkType])) {
            // [length, type, full type]
            $type = $aliases[$checkType];
            return ["{$length}{$array}", $type, "{$type}{$typeLength}{$array}"];
        }

        // [length, type, full type]
        return ["{$length}{$array}", $type, "{$type}{$typeLength}{$addon}{$array}"];
    }

    /**
     * @param array $row
     *
     * @return array
     */
    private function getFieldDefault(array $row): array
    {
        $default = $row["default"] ?? '';
        $attidentity = $row['attidentity'] ?? '';
        if (in_array($attidentity, ['a', 'd'])) {
            $default = 'GENERATED ' . ($attidentity == 'd' ? 'BY DEFAULT' : 'ALWAYS') . ' AS IDENTITY';
        }

        $autoIncrement = $attidentity !== '' ||
            preg_match('~^nextval\(~i', $default) ||
            preg_match('~^unique_rowid\(~', $default); // CockroachDB

        if (preg_match('~(.+)::[^,)]+(.*)~', $default, $match)) {
            $default = $match[1] === "NULL" ? null :
                $this->driver->unescapeId($match[1]) . $match[2];
        }

        return [$default, $autoIncrement];
    }

    /**
     * @param array $row
     *
     * @return TableFieldEntity
     */
    private function makeTableFieldEntity(array $row): TableFieldEntity
    {
        $field = new TableFieldEntity();

        $field->name = $row["field"];
        //! No collation, no info about primary keys
        // $field->primary = false;
        $field->null = !$row["attnotnull"];
        [$field->length, $field->type, $field->fullType] = $this->getFieldTypes($row);
        $field->generated = ($row["attgenerated"] ?? '') == "s" ? "STORED" : "";
        $field->privileges = ["insert" => 1, "select" => 1, "update" => 1, "where" => 1, "order" => 1];
        [$field->default, $field->autoIncrement] = $this->getFieldDefault($row);
        $field->comment = $row["comment"];

        return $field;
    }

    /**
     * @inheritDoc
     */
    public function fields(string $table): array
    {
        $fields = [];
        $tableOid = $this->tableOid($table);
        $optionalFields = ($this->driver->minVersion(10) ? ",a.attidentity" .
            ($this->driver->minVersion(12) ? ", a.attgenerated" : "") : "");
        $query = "SELECT a.attname AS field, format_type(a.atttypid, a.atttypmod) AS full_type,
pg_get_expr(d.adbin, d.adrelid) AS default, a.attnotnull::int,
col_description(a.attrelid, a.attnum) AS comment$optionalFields
FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = $tableOid AND NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum";
        foreach ($this->driver->rows($query) as $row)
        {
            $field = $this->makeTableFieldEntity($row);
            $fields[$field->name] = $field;
        }

        return $fields;
    }

    /**
     * @inheritDoc
     */
    public function indexes(string $table): array
    {
        $tableOid = $this->tableOid($table);
        $columns = $this->driver->keyValues("SELECT attnum, attname
FROM pg_attribute WHERE attrelid = $tableOid AND attnum > 0");

        $query = "SELECT relname, indisunique::int, indisprimary::int, indkey, indoption, amname,
pg_get_expr(indpred, indrelid, true) AS partial, pg_get_expr(indexprs, indrelid) AS indexpr
FROM pg_index JOIN pg_class ON indexrelid = oid JOIN pg_am ON pg_am.oid = pg_class.relam
WHERE indrelid = $tableOid ORDER BY indisprimary DESC, indisunique DESC";
        $indexes = [];
        foreach ($this->driver->rows($query) as $row)
        {
            $indexes[$row["relname"]] = $this->makeIndexEntity($row, $columns);
        }
        return $indexes;
    }

    /**
     * @inheritDoc
     */
    public function foreignKeys(string $table): array
    {
        $table = $this->driver->quote($table);
        $foreignKeys = [];
        $query = "SELECT conname, condeferrable::int AS deferrable, pg_get_constraintdef(oid)
AS definition FROM pg_constraint WHERE conrelid = (SELECT pc.oid FROM pg_class AS pc
INNER JOIN pg_namespace AS pn ON (pn.oid = pc.relnamespace) WHERE pc.relname = $table
AND pn.nspname = current_schema()) AND contype = 'f'::char ORDER BY conkey, conname";
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
    public function checkConstraints(TableEntity $status): array
    {
        // From driver.inc.php
        $database = $this->driver->quote($this->driver->database());
        $schema = $this->driver->quote($status->schema);
        $table = $this->driver->quote($status->name);
        $query = "SELECT c.CONSTRAINT_NAME, c.CHECK_CLAUSE
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS c
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t ON c.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
AND c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
WHERE t.TABLE_CATALOG = $database AND t.TABLE_SCHEMA = $schema AND t.TABLE_NAME = $table
AND c.CHECK_CLAUSE NOT LIKE '% IS NOT NULL'"; // ignore default IS NOT NULL checks in PostrgreSQL
        // MariaDB contains CHECK_CONSTRAINTS.TABLE_NAME, MySQL and PostrgreSQL not
        return $this->driver->keyValues($query);
    }

    /**
     * @inheritDoc
     */
    public function partitionsInfo(string $table): PartitionEntity|null
    {
        if (!$this->driver->minVersion(10)) {
            return null;
        }
        $query = "SELECT * FROM pg_partitioned_table WHERE partrelid = " . $this->tableOid($table);
        $row = $this->driver->execute($query)?->fetchAssoc();
        if (!$row) {
            return null;
        }

        $partId = $row['partrelid'];
        $query = "SELECT attname FROM pg_attribute WHERE attrelid = $partId AND attnum IN (" .
            str_replace(' ', ', ', $row['partattrs']) . ')'; //! ordering
        $attrs = $this->driver->values($query);
        $callback = fn($attr) => $this->driver->escapeId($attr);
        $partitionFields = implode(', ', array_map($callback, $attrs));

        $by = ['h' => 'HASH', 'l' => 'LIST', 'r' => 'RANGE'];
        return new PartitionEntity($by[$row['partstrat']], $partitionFields);
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $name, string $table = ''): TriggerEntity|null
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
        return new TriggerEntity($row['Timing'], $row['Event'],
            $row['Statement'], '', $row['Trigger'],
            $row['Type'], $row['Events']);
    }

    /**
     * @inheritDoc
     */
    public function triggers(string $table): array
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
    public function triggerOptions(): array
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
    public function tableHelp(string $name): string
    {
        $links = [
            "information_schema" => "infoschema",
            "pg_catalog" => "catalog",
        ];
        $link = $links[$this->driver->schema()];
        if ($link) {
            return "$link-" . str_replace("_", "-", $name) . ".html";
        }
        return '';
    }
}
