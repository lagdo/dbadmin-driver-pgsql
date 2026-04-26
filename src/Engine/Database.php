<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Engine;

use Lagdo\DbAdmin\Driver\Sql\Connection\StatementInterface;
use Lagdo\DbAdmin\Driver\Sql\Dto\FieldType;
use Lagdo\DbAdmin\Driver\Sql\Dto\RoutineDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\RoutineInfoDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\TableFieldDto;
use Lagdo\DbAdmin\Driver\Sql\Dto\UserTypeDto;
use Lagdo\DbAdmin\Driver\Sql\Specific\Engine\AbstractDatabase;

use function array_filter;
use function array_map;
use function array_values;
use function count;
use function implode;
use function intval;
use function in_array;
use function is_a;
use function is_object;

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
    public function databases(bool $flush): array
    {
        $query = "SELECT datname FROM pg_database WHERE has_database_privilege(datname, 'CONNECT') " .
            "AND datname not in ('postgres','template0','template1') ORDER BY datname";
        return $this->_engine()->columnValues($query);
    }

    /**
     * @inheritDoc
     */
    public function databaseSize(string $database): int
    {
        $statement = $this->_engine()->execute("SELECT pg_database_size(" . $this->_engine()->quote($database) . ")");
        if (is_a($statement, StatementInterface::class) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation(string $database, array $collations): string
    {
        return $this->_engine()->result("SELECT datcollate FROM pg_database WHERE datname = " .
            $this->_engine()->quote($database));
    }

    /**
     * @inheritDoc
     */
    public function isInformationSchema(string $database): bool
    {
        return $database == "information_schema";
    }

    /**
     * @inheritDoc
     */
    public function isSystemSchema(string $database): bool
    {
        return in_array($database, ['information_schema',
            'pg_catalog', 'pg_toast', 'postgres', 'template0', 'template1']);
    }

    /**
     * @inheritDoc
     */
    public function tables(): array
    {
        $query = 'SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()';
        if ($this->_engine()->support('materializedview')) {
            $query .= " UNION ALL SELECT matviewname, 'MATERIALIZED VIEW' FROM pg_matviews WHERE schemaname = current_schema()";
        }
        $query .= ' ORDER BY 1';
        return $this->_engine()->keyValues($query);
    }

    /**
     * @inheritDoc
     */
    public function sequences(): array
    {
        // From db.inc.php
        $query = 'SELECT sequence_name FROM information_schema.sequences ' .
            'WHERE sequence_schema = selectedSchema() ORDER BY sequence_name';
        return $this->_engine()->columnValues($query);
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
            $connection = $this->_engine()->openNewConnection($database); // New connection
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
    public function schemas(): array
    {
        $query = "SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('" .
            implode("','", $this->systemSchemas) . "') ORDER BY nspname";
        return $this->_engine()->columnValues($query);
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type): RoutineInfoDto|null
    {
        $quotedName = $this->_engine()->quote($name);
        $query = 'SELECT routine_definition AS definition, LOWER(external_language) AS language, * ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ' .
            "AND specific_name = $quotedName";
        $rows = $this->_engine()->rows($query);
        if (!isset($rows[0])) {
            return null;
        }

        $definition = $rows[0]['definition'];
        $language = $rows[0]['language'];
        $type = $rows[0]['type_udt_name'];

        $query = 'SELECT parameter_name AS name, data_type AS type, character_maximum_length AS length, ' .
            'parameter_mode AS inout FROM information_schema.parameters WHERE specific_schema = current_schema() ' .
            "AND specific_name = $quotedName ORDER BY ordinal_position";
        $rows = $this->_engine()->rows($query);
        $paramPosition = 0;
        $params = array_map(function(array $param) use(&$paramPosition) {
            $paramPosition++;
            $name = $param['name'] ?: $paramPosition;
            $type = $param['type'] ?: '';
            $length = $param['length'] ?: '';
            $inout = $param['inout'] ?: '';
            return new FieldType(name: $name, type: $type, length: $length, inout: $inout);
        }, $this->_engine()->rows($query));

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
        $rows = $this->_engine()->rows($query);
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
        return $this->_statement()->escapeId($name) . '(' . implode(', ', $types) . ')';
    }

    /**
     * @inheritDoc
     */
    public function userTypes(bool $withValues): array
    {
        $query = "SELECT oid, typname AS name FROM pg_type
WHERE typnamespace = {$this->nsOid} AND typtype IN ('b','d','e') AND typelem = 0";
        $rows = $this->_engine()->rows($query);
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
        foreach ($this->_engine()->rows($query) as $enum) {
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
}
