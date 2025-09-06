<?php

namespace Lagdo\DbAdmin\Driver\PgSql\Db;

use function preg_replace;

trait ConnectionTrait
{
    /**
     * @return string
     */
    public function error(): string
    {
        $message = parent::error();
        if (preg_match('~^(.*\n)?([^\n]*)\n( *)\^(\n.*)?$~s', $message, $match)) {
            $match = array_pad($match, 5, '');
            $message = $match[1] . preg_replace('~((?:[^&]|&[^;]*;){' .
                strlen($match[3]) . '})(.*)~', '\1<b>\2</b>', $match[2]) . $match[4];
        }
        return $message;
    }
}
