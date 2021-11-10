<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use Lagdo\DbAdmin\Driver\Entity\TableFieldEntity;
use Lagdo\DbAdmin\Driver\Entity\TableEntity;
use Lagdo\DbAdmin\Driver\Entity\IndexEntity;
use Lagdo\DbAdmin\Driver\Entity\ForeignKeyEntity;

trait TableTrait
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

    /**
     * @param array $row
     *
     * @return string
     */
    private function getFieldDefault(array $row)
    {
        $values = [
            'a' => 'GENERATED ALWAYS AS IDENTITY',
            'd' => 'GENERATED BY DEFAULT AS IDENTITY',
        ];
        $default = isset($values[$row['identity']]) ? $values[$row['identity']] : $row["default"];
        if (!preg_match('~(.+)::[^,)]+(.*)~', $row["default"], $match)) {
            return $default;
        }
        $match = array_pad($match, 3, '');
        if ($match[1] == "NULL") {
            return null;
        }
        if (!empty($match[1]) && $match[1][0] == "'") {
            return $this->driver->unescapeId($match[1]) . $match[2];
        }
        return $match[1] . $match[2];
    }

    /**
     * @param array $row
     *
     * @return array
     */
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
}
