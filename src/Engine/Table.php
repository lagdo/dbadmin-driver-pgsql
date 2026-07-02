<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Engine;

use Lagdo\DbAdmin\Driver\PgSql\Traits\TableTrait;
use Lagdo\DbAdmin\Driver\Sql\Dto\ColumnDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\ForeignKeyDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\IndexDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\PartitionDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TriggerDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractTable;

use function array_combine;
use function array_filter;
use function array_map;
use function explode;
use function implode;
use function intval;
use function in_array;
use function is_a;
use function preg_match;
use function preg_split;
use function str_replace;
use function trim;

class Table extends AbstractTable
{
    use TableTrait;

    /**
     * @var bool|null
     */
    private bool|null $hasSize = null;

    /**
     * @param string $table
     *
     * @return array
     */
    private function queryStatus(string $table = ''): array
    {
        // https://github.com/cockroachdb/cockroach/issues/40391
        $this->hasSize ??= $this->_engine()->columnValue("SELECT 'pg_table_size'::regproc");

        $tableName = $this->_engine()->quote($table);
        $oidClause = $this->_engine()->minVersion(12) ? "''" :
            "CASE WHEN c.relhasoids THEN 'oid' ELSE '' END";
        $query = "SELECT c.relname AS \"Name\",
CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE 'table' END AS \"Engine\",
$oidClause AS \"Oid\", c.reltuples AS \"Rows\",";
        if ($this->hasSize) {
            $query .= "
pg_table_size(c.oid) AS \"Data_length\", pg_indexes_size(c.oid) AS \"Index_length\",";
        }
        if ($this->_engine()->minVersion(10)) {
            $query .= " c.relispartition::int AS partition,";
        }
        $query .= "
current_schema() AS nspname, obj_description(c.oid, 'pg_class') AS \"Comment\"
FROM pg_class c WHERE c.relkind IN ('r', 'm', 'v', 'f', 'p')
AND c.relnamespace = {$this->nsOid} " .
        ($table !== '' ? "AND c.relname = $tableName" : "ORDER BY c.relname");

        return $this->_engine()->rows($query);
    }

    /**
     * @param array $row
     * @param string $columnName
     *
     * @return int|null
     */
    private function getPositiveInt(array $row, string $columnName): int|null
    {
        if (!isset($row[$columnName])) {
            return null;
        }

        $value = (int)$row[$columnName];
        return $value < 0 ? null : $value;
    }

    /**
     * @param array $row
     *
     * @return TableDto
     */
    private function makeStatus(array $row): TableDto
    {
        $status = new TableDto($row['Name'], $this->_engine()->columns(...));
        $status->oid = $row['Oid'];
        $status->engine = $row['Engine'] ?? '';
        $status->schema = $row['nspname'];
        $status->dataLength = $this->getPositiveInt($row, 'Data_length');
        $status->indexLength = $this->getPositiveInt($row, 'Index_length');
        // Not provided.
        // $status->dataFree = $this->getPositiveInt($row, 'Data_free');
        $status->rowCount = $this->getPositiveInt($row, 'Rows');
        $status->comment = $row['Comment'] ?? null;

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
     *
     * @return ForeignKeyDto|null
     */
    private function makeForeignKeyDto(array $row): ForeignKeyDto|null
    {
        $columnRegex = '~FOREIGN KEY\s*\((.+)\)\s*REFERENCES (.+)\((.+)\)(.*)$~iA';
        if (!preg_match($columnRegex, $row['definition'], $columnMatches)) {
            return null;
        }

        $onActions = $this->_engine()->actions();

        $foreignKey = new ForeignKeyDto();
        $foreignKey->definition = $row['definition'];
        $unescapeId = fn(string $id) => $this->_statement()->unescapeId(trim($id));
        $foreignKey->source = array_map($unescapeId, explode(',', $columnMatches[1] ?? ''));
        $foreignKey->target = array_map($unescapeId, explode(',', $columnMatches[3] ?? ''));

        $schemaRegex = '~^(("([^"]|"")+"|[^"]+)\.)?"?("([^"]|"")+"|[^"]+)$~';
        if (preg_match($schemaRegex, $columnMatches[2] ?? '', $schemaMatches)) {
            $foreignKey->schema = $unescapeId($schemaMatches[2] ?? '');
            $foreignKey->table = $unescapeId($schemaMatches[4] ?? '');
        }

        $foreignKey->onDelete = preg_match("~ON DELETE ($onActions)~",
            $columnMatches[4] ?? '', $schemaMatches) ? $schemaMatches[1] : 'NO ACTION';
        $foreignKey->onUpdate = preg_match("~ON UPDATE ($onActions)~",
            $columnMatches[4] ?? '', $schemaMatches) ? $schemaMatches[1] : 'NO ACTION';

        return $foreignKey;
    }

    /**
     * @inheritDoc
     */
    public function tableStatus(string $table, bool $fast = false): TableDto|null
    {
        $rows = $this->queryStatus($table);
        return !($row = reset($rows)) ? null : $this->makeStatus($row);
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
    public function isView(TableDto $tableStatus): bool
    {
        return in_array($tableStatus->engine, ["view", "materialized view"]);
    }

    /**
     * @inheritDoc
     */
    public function supportForeignKeys(TableDto $tableStatus): bool
    {
        return true;
    }

    /**
     * @param array $row
     *
     * @return array
     */
    private function getColumnTypes(array $row): array
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
    private function getColumnDefault(array $row): array
    {
        $default = $row["default"] ?? null;
        $attidentity = $row['attidentity'] ?? '';
        if (in_array($attidentity, ['a', 'd'])) {
            $attr = $attidentity === 'd' ? 'BY DEFAULT' : 'ALWAYS';
            $default = "GENERATED $attr AS IDENTITY";
        }

        $autoIncrement = $attidentity !== '' ||
            ($default !== null && (preg_match('~^nextval\(~i', $default) ||
                preg_match('~^unique_rowid\(~', $default))); // CockroachDB

        if ($default !== null && preg_match('~(.+)::[^,)]+(.*)~', $default, $match)) {
            $default = $match[1] === "NULL" ? null :
                $this->_statement()->unescapeId($match[1]) . $match[2];
        }

        return [$default, $autoIncrement];
    }

    /**
     * @param array $row
     * @param string|TableDto $table
     *
     * @return ColumnDto
     */
    private function makeColumnDto(array $row, string|TableDto $table): ColumnDto
    {
        $column = new ColumnDto();

        $column->name = $row['name'];
        //! No collation, no info about primary keys
        // $column->primary = false;
        $column->nullable = !$row["attnotnull"];
        [$column->length, $column->type, $column->fullType] = $this->getColumnTypes($row);
        $column->generated = ($row["attgenerated"] ?? '') === "s" ? "STORED" : "";
        $column->privileges = ["insert" => 1, "select" => 1, "update" => 1, "where" => 1, "order" => 1];
        [$column->default, $column->autoIncrement] = $this->getColumnDefault($row);
        $column->comment = $row["comment"] ?? null;

        $sequenceName = is_a($table, TableDto::class) ? $this->getSequenceName($column) : '';
        if ($sequenceName !== '') {
            $table->hasAutoIncrement = true;
            $sequenceName = $this->_statement()->escapeTableName($sequenceName);
            $query = "SELECT last_value FROM $sequenceName";
            $table->autoIncrement = (int)($this->_engine()->columnValue($query) ?? 0);
        }

        return $column;
    }

    /**
     * @inheritDoc
     */
    public function columns(string|TableDto $table): array
    {
        $tableName = is_a($table, TableDto::class) ? $table->name : $table;

        $columns = [];
        $tableOid = $this->tableOid($tableName);
        $optionalColumns = ($this->_engine()->minVersion(10) ? ",a.attidentity" .
            ($this->_engine()->minVersion(12) ? ", a.attgenerated" : "") : "");
        $query = "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS full_type,
pg_get_expr(d.adbin, d.adrelid) AS default, a.attnotnull::int,
col_description(a.attrelid, a.attnum) AS comment$optionalColumns
FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = $tableOid AND NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum";
        $rows = $this->_engine()->rows($query);
        $columns = array_map(fn(array $row) => $this->makeColumnDto($row, $table), $rows);
        // Key by column name.
        $columns = array_combine(array_map(fn($column) => $column->name, $columns), $columns);
        // Set primary keys.
        $filter = fn(IndexDto $index) => $index->type === 'PRIMARY';
        foreach (array_filter($this->indexes($tableName), $filter) as $primaryKey) {
            foreach ($primaryKey->columns as $primaryKeyColumn) {
                if (isset($columns[$primaryKeyColumn])) {
                    $columns[$primaryKeyColumn]->primary = true;
                }
            }
        }
        return $columns;
    }

    /**
     * @param array $row
     * @param array $columns
     *
     * @return IndexDto
     */
    private function makeIndexDto(array $row, array $columns): IndexDto
    {
        $index = new IndexDto();

        $index->type = $this->getIndexType($row);
        $index->name = $row["relname"];
        $index->algorithm = $row["amname"] ?? '';
        $index->partial = $row["partial"] ?? '';
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
     * @inheritDoc
     */
    public function indexes(string $table): array
    {
        $tableOid = $this->tableOid($table);
        $columns = $this->_engine()->keyValues("SELECT attnum, attname
FROM pg_attribute WHERE attrelid = $tableOid AND attnum > 0");

        $query = "SELECT relname, indisunique::int, indisprimary::int, indkey, indoption, amname,
pg_get_expr(indpred, indrelid, true) AS partial, pg_get_expr(indexprs, indrelid) AS indexpr
FROM pg_index JOIN pg_class ON indexrelid = oid JOIN pg_am ON pg_am.oid = pg_class.relam
WHERE indrelid = $tableOid ORDER BY indisprimary DESC, indisunique DESC";
        $indexes = [];
        foreach ($this->_engine()->rows($query) as $row)
        {
            $indexes[$row["relname"]] = $this->makeIndexDto($row, $columns);
        }
        return $indexes;
    }

    /**
     * @inheritDoc
     */
    public function foreignKeys(string $table): array
    {
        $table = $this->_engine()->quote($table);
        $foreignKeys = [];
        $query = "SELECT conname, condeferrable::int AS deferrable, pg_get_constraintdef(oid)
AS definition FROM pg_constraint WHERE conrelid = (SELECT pc.oid FROM pg_class AS pc
INNER JOIN pg_namespace AS pn ON (pn.oid = pc.relnamespace) WHERE pc.relname = $table
AND pn.nspname = current_schema()) AND contype = 'f'::char ORDER BY conkey, conname";
        foreach ($this->_engine()->rows($query) as $row) {
            $foreignKey = $this->makeForeignKeyDto($row);
            if ($foreignKey !== null) {
                $foreignKeys[$row['conname']] = $foreignKey;
            }
        }
        return $foreignKeys;
    }

    /**
     * @inheritDoc
     */
    public function checkConstraints(TableDto $status): array
    {
        // From driver.inc.php
        $database = $this->_engine()->quote($this->_engine()->database());
        $schema = $this->_engine()->quote($status->schema);
        $table = $this->_engine()->quote($status->name);
        $query = "SELECT c.CONSTRAINT_NAME, c.CHECK_CLAUSE
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS c
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t ON c.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
AND c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
WHERE t.TABLE_CATALOG = $database AND t.TABLE_SCHEMA = $schema AND t.TABLE_NAME = $table
AND c.CHECK_CLAUSE NOT LIKE '% IS NOT NULL'"; // ignore default IS NOT NULL checks in PostrgreSQL
        // MariaDB contains CHECK_CONSTRAINTS.TABLE_NAME, MySQL and PostrgreSQL not
        return $this->_engine()->keyValues($query);
    }

    /**
     * @inheritDoc
     */
    public function partitionsInfo(string $table): PartitionDto|null
    {
        if (!$this->_engine()->minVersion(10)) {
            return null;
        }

        $query = "SELECT * FROM pg_partitioned_table WHERE partrelid = " . $this->tableOid($table);
        $result = $this->_engine()->executeQuery($query);
        if ($result->hasError() || $result->rowCount() === 0) {
            return null;
        }

        $row = $result->fetchAssoc();
        $partId = $row['partrelid'];
        $query = "SELECT attname FROM pg_attribute WHERE attrelid = $partId AND attnum IN (" .
            str_replace(' ', ', ', $row['partattrs']) . ')'; //! ordering
        $attrs = $this->_engine()->columnValues($query);
        $partitionColumns = implode(', ', array_map($this->_statement()->escapeId(...), $attrs));

        $by = ['h' => 'HASH', 'l' => 'LIST', 'r' => 'RANGE'];
        return new PartitionDto($by[$row['partstrat']], $partitionColumns);
    }

    /**
     * @inheritDoc
     */
    public function trigger(string $name, string $table): TriggerDto|null
    {
        if ($name === '') {
            return new TriggerDto('', '', 'EXECUTE PROCEDURE ()');
        }

        $query = 'SELECT t.trigger_name AS "Trigger", t.action_timing AS "Timing", ' .
            '(SELECT STRING_AGG(event_manipulation, \' OR \') FROM information_schema.triggers ' .
            'WHERE event_object_table = t.event_object_table AND trigger_name = t.trigger_name ) AS "Events", ' .
            't.event_manipulation AS "Event", \'FOR EACH \' || t.action_orientation AS "Type", ' .
            't.action_statement AS "Statement" FROM information_schema.triggers t WHERE t.event_object_table = ' .
            $this->_engine()->quote($table) . ' AND t.trigger_name = ' . $this->_engine()->quote($name);
        $rows = $this->_engine()->rows($query);
        if (!($row = reset($rows))) {
            return null;
        }
        return new TriggerDto($row['Timing'], $row['Event'],
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
            "AND event_object_table = " . $this->_engine()->quote($table);
        foreach ($this->_engine()->rows($query) as $row) {
            $triggers[$row["trigger_name"]] = new TriggerDto($row["action_timing"],
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
        $link = $links[$this->_engine()->schema()];
        return !$link ? '' : "$link-" . str_replace("_", "-", $name) . ".html";
    }
}
