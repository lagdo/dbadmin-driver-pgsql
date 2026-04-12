<?php

use Lagdo\DbAdmin\Driver\Driver;
use Lagdo\DbAdmin\Driver\PgSql\Engine;
use Lagdo\DbAdmin\Driver\PgSql\Statement;
use Lagdo\DbAdmin\Driver\Utils\Utils;

Driver::registerBuilder('pgsql', fn(Utils $utils, array $options) =>
    [new Engine($utils, $options), new Statement($utils)]);
