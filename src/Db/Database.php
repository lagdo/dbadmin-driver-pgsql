<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Db\AbstractDatabase;
use Lagdo\DbAdmin\Driver\Dto\FieldType;
use Lagdo\DbAdmin\Driver\Dto\RoutineDto;
use Lagdo\DbAdmin\Driver\Dto\RoutineInfoDto;
use Lagdo\DbAdmin\Driver\Dto\TableFieldDto;
use Lagdo\DbAdmin\Driver\Dto\UserTypeDto;

use function array_filter;
use function array_map;
use function array_merge;
use function array_reverse;
use function array_unshift;
use function array_values;
use function count;
use function implode;
use function is_object;
use function strtoupper;
use function uniqid;

class Database extends AbstractDatabase
{
    use Traits\TableOidTrait;

    /**
     * PostgreSQL system schemas
     *
     * @var array
     */
    protected $systemSchemas = ['information_schema', 'pg_catalog', 'pg_temp_1', 'pg_toast', 'pg_toast_temp_1'];

    /**
     * @inheritDoc
     */
    public function alterIndexes(string $table, array $alter, array $drop): bool
    {
        $queries = [];
        $columns = [];
        foreach (array_reverse($drop) as $index) {
            if ($index->type === 'INDEX') {
                $queries[] = 'DROP INDEX ' . $this->driver->escapeId($index);
            } else {
                $columns[] = 'DROP CONSTRAINT ' . $this->driver->escapeId($index->name);
            }
        }
        foreach ($alter as $index) {
            if ($index->type === 'INDEX') {
                $queries[] = 'CREATE INDEX ' .
                    $this->driver->escapeId($index->name != '' ? $index->name : uniqid($table . '_')) .
                    ' ON ' . $this->driver->escapeTableName($table) .
                    ' (' . implode(', ', $index->columns) . ')';
            } else {
                //! descending UNIQUE indexes results in syntax error
                $constraint = ($index->name != '' ? ' CONSTRAINT ' . $this->driver->escapeId($index->name) : '');
                $columns[] = "ADD$constraint " . ($index->type == 'PRIMARY' ? 'PRIMARY KEY' : $index->type) .
                    ' (' . implode(', ', $index->columns) . ')';
            }
        }
        if (!empty($columns)) {
            array_unshift($queries, 'ALTER TABLE ' .
                $this->driver->escapeTableName($table) . implode(', ', $columns));
        }
        foreach ($queries as $query) {
            $this->driver->execute($query);
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function tables(): array
    {
        $query = 'SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()';
        if ($this->driver->support('materializedview')) {
            $query .= " UNION ALL SELECT matviewname, 'MATERIALIZED VIEW' FROM pg_matviews WHERE schemaname = current_schema()";
        }
        $query .= ' ORDER BY 1';
        return $this->driver->keyValues($query);
    }

    /**
     * @inheritDoc
     */
    public function sequences(): array
    {
        // From db.inc.php
        $query = 'SELECT sequence_name FROM information_schema.sequences ' .
            'WHERE sequence_schema = selectedSchema() ORDER BY sequence_name';
        return $this->driver->values($query);
    }

    /**
     * @inheritDoc
     */
    public function countTables(array $databases): array
    {
        $counts = [];
        $query = "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('" .
            implode("','", $this->systemSchemas) . "')";
        foreach ($databases as $database) {
            $counts[$database] = 0;
            $connection = $this->driver->newConnection($database); // New connection
            if (!$connection) {
                continue;
            }
            $statement = $connection->query($query);
            if (is_object($statement) && ($row = $statement->fetchRow())) {
                $counts[$database] = intval($row[0]);
            }
        }
        return $counts;
    }

    /**
     * @inheritDoc
     */
    public function dropViews(array $views): bool
    {
        return $this->dropTables($views);
    }

    /**
     * @inheritDoc
     */
    public function dropTables(array $tables): bool
    {
        foreach ($tables as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute('DROP ' . strtoupper($status->engine) . ' ' . $this->driver->escapeTableName($table))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function schemas(): array
    {
        $query = "SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('" .
            implode("','", $this->systemSchemas) . "') ORDER BY nspname";
        return $this->driver->values($query);
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type): RoutineInfoDto|null
    {
        $quotedName = $this->driver->quote($name);
        $query = 'SELECT routine_definition AS definition, LOWER(external_language) AS language, * ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ' .
            "AND specific_name = $quotedName";
        $rows = $this->driver->rows($query);
        if (!isset($rows[0])) {
            return null;
        }

        $definition = $rows[0]['definition'];
        $language = $rows[0]['language'];
        $type = $rows[0]['type_udt_name'];

        $query = 'SELECT parameter_name AS name, data_type AS type, character_maximum_length AS length, ' .
            'parameter_mode AS inout FROM information_schema.parameters WHERE specific_schema = current_schema() ' .
            "AND specific_name = $quotedName ORDER BY ordinal_position";
        $rows = $this->driver->rows($query);
        $paramPosition = 0;
        $params = array_map(function(array $param) use(&$paramPosition) {
            $paramPosition++;
            $name = $param['name'] ?: $paramPosition;
            $type = $param['type'] ?: '';
            $length = $param['length'] ?: '';
            $inout = $param['inout'] ?: '';
            return new FieldType(name: $name, type: $type, length: $length, inout: $inout);
        }, $this->driver->rows($query));

        return new RoutineInfoDto($definition, $language,
            $params, new FieldType(type: $type));
    }

    /**
     * @inheritDoc
     */
    public function routines(): array
    {
        $query = 'SELECT specific_name AS "SPECIFIC_NAME", routine_type AS "ROUTINE_TYPE", ' .
            'routine_name AS "ROUTINE_NAME", type_udt_name AS "DTD_IDENTIFIER" ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ORDER BY SPECIFIC_NAME';
        $rows = $this->driver->rows($query);
        // The ROUTINE_TYPE field can have NULL as value
        return array_map(fn($row) =>
            new RoutineDto($row['ROUTINE_NAME'], $row['SPECIFIC_NAME'],
                $row['ROUTINE_TYPE'] ?: '', $row['DTD_IDENTIFIER']), $rows);
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row): string
    {
        $types = [];
        foreach ($row['fields'] as $field) {
            $types[] = $field->type;
        }
        return $this->driver->escapeId($name) . '(' . implode(', ', $types) . ')';
    }

    /**
     * @inheritDoc
     */
    public function userTypes(bool $withValues): array
    {
        $query = "SELECT oid, typname AS name FROM pg_type
WHERE typnamespace = {$this->nsOid} AND typtype IN ('b','d','e') AND typelem = 0";
        $rows = $this->driver->rows($query);
        $types = [];
        foreach ($rows as $row) {
            $types[$row['name']] = new UserTypeDto($row['oid'], $row['name']);
        }

        if (!$withValues || count($types) === 0) {
            return $types;
        }

        $typeOids = implode("','", array_map(fn($type) => $type->oid, $types));
        $query = "SELECT enumtypid, enumlabel FROM pg_enum
WHERE enumtypid IN ('$typeOids') ORDER BY enumsortorder";
        foreach ($this->driver->rows($query) as $enum) {
            foreach ($types as &$type) {
                if ($type->oid === $enum['enumtypid']) {
                    $type->enums[] = $enum['enumlabel'];
                    break;
                }
            }
        }
        return $types;
    }

    /**
     * @inheritDoc
     */
    public function enumValues(TableFieldDto $field): array
    {
        $types = array_filter(array_values($this->userTypes(true)),
            fn(UserTypeDto $type) => $type->name === $field->type);
        return isset($types[0]) ? $types[0]->enums : [];
    }

    /**
     * @inheritDoc
     */
    public function moveTables(array $tables, array $views, string $target): bool
    {
        foreach (array_merge($tables, $views) as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute('ALTER ' . strtoupper($status->engine) . ' ' .
                $this->driver->escapeTableName($table) . ' SET SCHEMA ' . $this->driver->escapeId($target))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function truncateTables(array $tables): bool
    {
        $tables = array_map(fn($table) => $this->driver->escapeTableName($table), $tables);
        $this->driver->execute('TRUNCATE ' . implode(', ', $tables));
        return true;
    }
}
