<?php

namespace Lagdo\DbAdmin\Driver\MySql\Tests;

use PHPUnit\Framework\TestCase;
use Exception;

/**
 * @covers Lagdo\DbAdmin\Driver\PgSql\Server
 */
final class ServerTest extends TestCase
{
    /**
     * The Jaxon Adminer package
     *
     * @var Package
     */
    protected $package;

    /**
     * The facade to database functions
     *
     * @var DbAdmin
     */
    protected $dbAdmin;

    public static function setUpBeforeClass()
    {
    }

    /**
     * @expectedException Exception
     */
    public function testException()
    {
        throw new Exception('');
    }

    public function testFunction()
    {
    }
}
