<?php

if(class_exists(Lagdo\Adminer\DbAdmin::class))
{
    Lagdo\Adminer\DbAdmin::addServer("pgsql", Lagdo\DbAdmin\Driver\PgSql\Server::class);
}
