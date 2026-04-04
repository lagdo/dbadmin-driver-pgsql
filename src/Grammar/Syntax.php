<?php

namespace Lagdo\DbAdmin\Support\PgSql\Grammar;

use Lagdo\DbAdmin\Support\Db\Engine\Grammar\AbstractSyntax;

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
}
