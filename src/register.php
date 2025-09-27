<?php

use Lagdo\DbAdmin\Driver;

Driver\Driver::registerDriver('pgsql',
    function($di, array $options): Driver\DriverInterface {
        $utils = $di->g(Driver\Utils\Utils::class);
        return new Driver\PgSql\Driver($utils, $options);
    });
