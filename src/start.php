<?php

if(class_exists(Lagdo\Adminer\DbAdmin::class))
{
    Lagdo\Adminer\DbAdmin::addServer("pgsql", Lagdo\Adminer\Driver\PgSql\Server::class);
}
