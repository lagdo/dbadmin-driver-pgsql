<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Statement;

use Lagdo\DbAdmin\Driver\Sql\Specific\Statement\AbstractSyntax;

use function str_replace;

class Syntax extends AbstractSyntax
{
    /**
     * @inheritDoc
     */
    public function escapeId(string $idf): string
    {
        return '"' . str_replace('"', '""', $idf) . '"';
    }

    /**
     * @inheritDoc
     */
    public function getAutoIncrementType(string $type): string
    {
        return match($type) {
            'bigint' => 'bigserial',
            'smallint' => 'smallserial',
            default => 'serial',
        };
    }

    /**
     * @inheritDoc
     */
    public function processAttr(array $process, string $key, string $val): string
    {
        return $key !== "current_query" || $val === "<IDLE>" ?
            parent::processAttr($process, $key, $val) :
            '<code>' . $this->_utils()->str->shortenUtf8($val, 50) .
                '</code>' . $this->_utils()->lang('Clone');
    }
}
