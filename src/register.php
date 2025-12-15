<?php

use Lagdo\DbAdmin\Driver\AbstractDriver;
use Lagdo\DbAdmin\Driver\DriverInterface;
use Lagdo\DbAdmin\Driver\PgSql\Driver;
use Lagdo\DbAdmin\Driver\Utils\Utils;

AbstractDriver::registerDriver('pgsql', fn($di, array $options): DriverInterface =>
    new Driver($di->g(Utils::class), $options));
