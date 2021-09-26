DbAdmin drivers for PostgreSQL
==============================

This package is based on [Adminer](https://github.com/vrana/adminer).

It provides PostgreSQL drivers for [Jaxon Adminer](https://github.com/lagdo/jaxon-dbadmin), and implements the interfaces defined in [https://github.com/lagdo/dbadmin-driver](https://github.com/lagdo/dbadmin-driver).

It requires either the `php-pgsql` or the `php-pdo_pgsql` PHP extension to be installed, and uses the former by default.

**Installation**

Install with Composer.

```
composer require lagdo/dbadmin-driver-pgsql
```

**Configuration**

Declare the PostgreSQL servers in the `packages` section on the `Jaxon` config file. Set the `driver` option to `pgsql`.

```php
    'app' => [
        'packages' => [
            Lagdo\DbAdmin\Package::class => [
                'servers' => [
                    'server_id' => [
                        'driver' => 'pgsql',
                        'name' => '',     // The name to be displayed in the dashboard UI.
                        'host' => '',     // The database host name or address.
                        'port' => 0,      // The database port. Optional.
                        'username' => '', // The database user credentials.
                        'password' => '', // The database user credentials.
                    ],
                ],
            ],
        ],
    ],
```

Check the [Jaxon Adminer](https://github.com/lagdo/jaxon-dbadmin) documentation for more information about the package usage.
