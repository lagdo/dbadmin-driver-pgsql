<?php

namespace Lagdo\DbAdmin\Driver\PgSql;

use Lagdo\DbAdmin\Driver\Db\Driver as AbstractDriver;

class Driver extends AbstractDriver
{
    public function insertOrUpdate($table, $rows, $primary)
    {
        foreach ($rows as $set) {
            $update = [];
            $where = [];
            foreach ($set as $key => $val) {
                $update[] = "$key = $val";
                if (isset($primary[$this->server->unescapeId($key)])) {
                    $where[] = "$key = $val";
                }
            }
            if (!(
                ($where && $this->db->queries("UPDATE " . $this->server->table($table) .
                " SET " . implode(", ", $update) . " WHERE " . implode(" AND ", $where)) &&
                $this->db->affectedRows()) ||
                $this->db->queries("INSERT INTO " . $this->server->table($table) .
                " (" . implode(", ", array_keys($set)) . ") VALUES (" . implode(", ", $set) . ")")
            )) {
                return false;
            }
        }
        return true;
    }

    public function slowQuery($query, $timeout)
    {
        $this->connection->query("SET statement_timeout = " . (1000 * $timeout));
        $this->connection->timeout = 1000 * $timeout;
        return $query;
    }

    public function convertSearch($idf, $val, $field)
    {
        return (preg_match('~char|text' . (!preg_match('~LIKE~', $val["op"]) ?
            '|date|time(stamp)?|boolean|uuid|' . $this->db->numberRegex() : '') . '~', $field->type) ?
            $idf : "CAST($idf AS text)"
        );
    }

    public function tableHelp($name)
    {
        $links = array(
            "information_schema" => "infoschema",
            "pg_catalog" => "catalog",
        );
        $link = $links[$this->server->selectedSchema()];
        if ($link) {
            return "$link-" . str_replace("_", "-", $name) . ".html";
        }
    }
}
