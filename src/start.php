<?php

$di = \jaxon()->di();
// Register the database classes in the dependency container
$di->auto(Lagdo\DbAdmin\Driver\PgSql\Server::class);
$di->alias('adminer_server_pgsql', Lagdo\DbAdmin\Driver\PgSql\Server::class);
