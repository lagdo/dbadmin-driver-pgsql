<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\RoutineEntity;

use Lagdo\DbAdmin\Driver\Db\Server as AbstractServer;

class Server extends AbstractServer
{
    /**
     * @inheritDoc
     */
    public function databases(bool $flush)
    {
        return $this->driver->values("SELECT datname FROM pg_database WHERE " .
            "has_database_privilege(datname, 'CONNECT') ORDER BY datname");
    }

    /**
     * @inheritDoc
     */
    public function databaseSize(string $database)
    {
        $statement = $this->connection->query("SELECT pg_database_size(" . $this->driver->quote($database) . ")");
        if (is_object($statement) && ($row = $statement->fetchRow())) {
            return intval($row[0]);
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function databaseCollation(string $database, array $collations)
    {
        return $this->connection->result("SELECT datcollate FROM pg_database WHERE datname = " . $this->driver->quote($database));
    }

    /**
     * @inheritDoc
     */
    public function tables()
    {
        $query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()";
        if ($this->driver->support('materializedview')) {
            $query .= " UNION ALL SELECT matviewname, 'MATERIALIZED VIEW' FROM pg_matviews WHERE schemaname = current_schema()";
        }
        $query .= " ORDER BY 1";
        return $this->driver->keyValues($query);
    }

    /**
     * @inheritDoc
     */
    public function countTables(array $databases)
    {
        $connection = $this->driver->createConnection(); // New connection
        $counts = [];
        $systemSchemas = ['information_schema', 'pg_catalog', 'pg_temp_1', 'pg_toast', 'pg_toast_temp_1'];
        $query = "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('" .
            implode("','", $systemSchemas) . "')";
        foreach ($databases as $database) {
            $counts[$database] = 0;
            if (!$connection->open($database)) {
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
    public function truncateTables(array $tables)
    {
        $this->driver->execute("TRUNCATE " . implode(", ", array_map(function ($table) {
            return $this->driver->table($table);
        }, $tables)));
        return true;
    }

    /**
     * @inheritDoc
     */
    public function dropViews(array $views)
    {
        return $this->dropTables($views);
    }

    /**
     * @inheritDoc
     */
    public function dropTables(array $tables)
    {
        foreach ($tables as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute("DROP " . strtoupper($status->engine) . " " . $this->driver->table($table))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function moveTables(array $tables, array $views, string $target)
    {
        foreach (array_merge($tables, $views) as $table) {
            $status = $this->driver->tableStatus($table);
            if (!$this->driver->execute("ALTER " . strtoupper($status->engine) . " " .
                $this->driver->table($table) . " SET SCHEMA " . $this->driver->escapeId($target))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @inheritDoc
     */
    public function collations()
    {
        //! supported in CREATE DATABASE
        return [];
    }

    /**
     * @inheritDoc
     */
    public function isInformationSchema($database)
    {
        return ($database == "information_schema");
    }

    /**
     * @inheritDoc
     */
    public function createDatabase(string $database, string $collation)
    {
        return $this->driver->execute("CREATE DATABASE " . $this->driver->escapeId($database) .
            ($collation ? " ENCODING " . $this->driver->escapeId($collation) : ""));
    }

    /**
     * @inheritDoc
     */
    public function dropDatabases(array $databases)
    {
        $this->connection->close();
        return $this->driver->applyQueries("DROP DATABASE", $databases, function($database) {
            return $this->driver->escapeId($database);
        });
    }

    /**
     * @inheritDoc
     */
    public function renameDatabase(string $name, string $collation)
    {
        //! current database cannot be renamed
        $this->driver->execute("ALTER DATABASE " . $this->driver->escapeId($this->driver->database()) .
            " RENAME TO " . $this->driver->escapeId($name));
        return true;
    }

    /**
     * @inheritDoc
     */
    public function routine(string $name, string $type)
    {
        $query = 'SELECT routine_definition AS definition, LOWER(external_language) AS language, * ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ' .
            'AND specific_name = ' . $this->driver->quote($name);
        $rows = $this->driver->rows($query);
        $routines = $rows[0];
        $routines["returns"] = ["type" => $routines["type_udt_name"]];
        $query = 'SELECT parameter_name AS field, data_type AS type, character_maximum_length AS length, ' .
            'parameter_mode AS inout FROM information_schema.parameters WHERE specific_schema = current_schema() ' .
            'AND specific_name = ' . $this->driver->quote($name) . ' ORDER BY ordinal_position';
        $routines["fields"] = $this->driver->rows($query);
        return $routines;
    }

    /**
     * @inheritDoc
     */
    public function routines()
    {
        $query = 'SELECT specific_name AS "SPECIFIC_NAME", routine_type AS "ROUTINE_TYPE", ' .
            'routine_name AS "ROUTINE_NAME", type_udt_name AS "DTD_IDENTIFIER" ' .
            'FROM information_schema.routines WHERE routine_schema = current_schema() ORDER BY SPECIFIC_NAME';
        $rows = $this->driver->rows($query);
        return array_map(function($row) {
            return new RoutineEntity($row['ROUTINE_NAME'], $row['SPECIFIC_NAME'], $row['ROUTINE_TYPE'], $row['DTD_IDENTIFIER']);
        }, $rows);
    }

    /**
     * @inheritDoc
     */
    public function routineLanguages()
    {
        return $this->driver->values("SELECT LOWER(lanname) FROM pg_catalog.pg_language");
    }

    /**
     * @inheritDoc
     */
    public function routineId(string $name, array $row)
    {
        $routine = [];
        foreach ($row["fields"] as $field) {
            $routine[] = $field->type;
        }
        return $this->driver->escapeId($name) . "(" . implode(", ", $routine) . ")";
    }

    /**
     * @inheritDoc
     */
    public function userTypes()
    {
        $query = "SELECT typname FROM pg_type WHERE typnamespace = " .
            "(SELECT oid FROM pg_namespace WHERE nspname = current_schema()) " .
            "AND typtype IN ('b','d','e') AND typelem = 0";
        return $this->driver->values($query);
    }

    /**
     * @inheritDoc
     */
    public function schemas()
    {
        return $this->driver->values("SELECT nspname FROM pg_namespace ORDER BY nspname");
    }

    /**
     * @inheritDoc
     */
    public function variables()
    {
        return $this->driver->keyValues("SHOW ALL");
    }

    /**
     * @inheritDoc
     */
    public function processes()
    {
        return $this->driver->rows("SELECT * FROM pg_stat_activity ORDER BY " . ($this->driver->minVersion(9.2) ? "pid" : "procpid"));
    }

    /**
     * @inheritDoc
     */
    public function statusVariables()
    {
    }

    /**
     * @inheritDoc
     */
    public function killProcess($val)
    {
        return $this->driver->execute("SELECT pg_terminate_backend(" . $this->util->number($val) . ")");
    }

    /**
     * @inheritDoc
     */
    public function connectionId()
    {
        return "SELECT pg_backend_pid()";
    }

    /**
     * @inheritDoc
     */
    public function maxConnections()
    {
        return $this->connection->result("SHOW max_connections");
    }
}
