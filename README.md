[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/lagdo/dbadmin-driver-pgsql/badges/quality-score.png?b=main)](https://scrutinizer-ci.com/g/lagdo/dbadmin-driver-pgsql/?branch=main)
[![StyleCI](https://styleci.io/repos/400390231/shield?branch=main)](https://styleci.io/repos/400390231)
[![Build Status](https://api.travis-ci.com/lagdo/dbadmin-driver-pgsql.svg?branch=main)](https://app.travis-ci.com/github/lagdo/dbadmin-driver-pgsql)
[![Coverage Status](https://coveralls.io/repos/github/lagdo/dbadmin-driver-pgsql/badge.svg?branch=main)](https://coveralls.io/github/lagdo/dbadmin-driver-pgsql?branch=main)

[![Latest Stable Version](https://poser.pugx.org/lagdo/dbadmin-driver-pgsql/v/stable)](https://packagist.org/packages/lagdo/dbadmin-driver-pgsql)
[![Total Downloads](https://poser.pugx.org/lagdo/dbadmin-driver-pgsql/downloads)](https://packagist.org/packages/lagdo/dbadmin-driver-pgsql)
[![License](https://poser.pugx.org/lagdo/dbadmin-driver-pgsql/license)](https://packagist.org/packages/lagdo/dbadmin-driver-pgsql)

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
