<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

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
    public function collations()
    {
        //! supported in CREATE DATABASE
        return [];
    }

    /**
     * @inheritDoc
     */
    public function isInformationSchema(string $database)
    {
        return ($database == "information_schema");
    }

    /**
     * @inheritDoc
     */
    public function createDatabase(string $database, string $collation)
    {
        $result = $this->driver->execute("CREATE DATABASE " . $this->driver->escapeId($database) .
            ($collation ? " ENCODING " . $this->driver->escapeId($collation) : ""));
        return $result !== false;
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
        $currName = $this->driver->escapeId($this->driver->database());
        $nextName = $this->driver->escapeId($name);
        $result = $this->driver->execute("ALTER DATABASE $currName RENAME TO $nextName");
        return $result !== false;
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
    public function processAttr(array $process, string $key, string $val): string
    {
        if ($key == "current_query" && $val != "<IDLE>") {
            return '<code>' . $this->shortenUtf8($val, 50) . '</code>' . $this->lang('Clone');
        }
        return parent::processAttr($process, $key, $val);
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
    // public function killProcess($val)
    // {
    //     return $this->driver->execute("SELECT pg_terminate_backend(" . $this->util->number($val) . ")");
    // }

    /**
     * @inheritDoc
     */
    // public function connectionId()
    // {
    //     return "SELECT pg_backend_pid()";
    // }

    /**
     * @inheritDoc
     */
    // public function maxConnections()
    // {
    //     return $this->connection->result("SHOW max_connections");
    // }
}
