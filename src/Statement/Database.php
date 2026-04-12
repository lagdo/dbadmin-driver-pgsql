<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractDatabase;

use function array_map;
use function implode;
use function preg_match;

class Database extends AbstractDatabase
{
    /**
     * @inheritDoc
     */
    public function getAutoIncrementModifier(): string
    {
        return '';
    }

    /**
     * @param string $database
     * @param string $style
     *
     * @return string
     */
    private function getInitDatabaseQuery(string $database, string $style = ''): string
    {
        if (!preg_match('~CREATE~', $style)) {
            return '';
        }

        $drop = $style !== 'DROP+CREATE' ? '' : "DROP DATABASE IF EXISTS $database;\n";
        $create = "CREATE DATABASE $database;\n";
        return "{$drop}{$create}";
    }

    /**
     * @inheritDoc
     */
    public function getUseDatabaseQuery(string $database, string $style = ''): string
    {
        $name = $this->_statement()->escapeId($database);
        return $this->getInitDatabaseQuery($name, $style) . "\\connect $name;";
    }

    /**
     * @inheritDoc
     */
    public function getCreateDatabaseQuery(string $database, string $collation): string
    {
        return "CREATE DATABASE " . $this->_statement()->escapeId($database) .
            ($collation ? " ENCODING " . $this->_statement()->escapeId($collation) : "");
    }

    /**
     * @inheritDoc
     */
    public function getDropDatabaseQuery(string $database): string
    {
        return 'DROP DATABASE ' . $this->_statement()->escapeId($database);
    }

    /**
     * @inheritDoc
     */
    public function getDropViewsQueries(array $views): array
    {
        return $this->getDropTablesQueries($views);
    }

    /**
     * @inheritDoc
     */
    public function getDropTablesQueries(array $tables): array
    {
        return array_map(function(string $table) {
            $status = $this->_engine()->tableStatus($table);
            $engine = strtoupper($status->engine);
            $tableName = $this->_statement()->escapeTableName($table);
            return "DROP $engine $tableName";
        }, $tables);
    }

    /**
     * @inheritDoc
     */
    public function getTruncateTablesQueries(array $tables): array
    {
        return [
            'TRUNCATE ' . implode(', ', array_map($this->_statement()->escapeTableName(...), $tables)),
        ];
    }
}
