<?php

// Register the database classes in the dependency container
\jaxon()->di()->set('adminer_server_pgsql', function($di) {
    return new Lagdo\DbAdmin\Driver\PgSql\Server(
        $di->get(Lagdo\Adminer\Driver\DbInterface::class),
        $di->get(Lagdo\Adminer\Driver\UtilInterface::class));
});
