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

    /**
     * @inheritDoc
     */
    public function processAttr(array $process, string $key, string $val): string
    {
        if ($key === "current_query" && $val !== "<IDLE>") {
            return '<code>' . $this->utils->str->shortenUtf8($val, 50) .
                '</code>' . $this->utils->trans->lang('Clone');
        }
        return parent::processAttr($process, $key, $val);
    }
}
