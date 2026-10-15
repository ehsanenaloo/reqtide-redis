<?php

declare(strict_types=1);

/**
 * Loads the Composer autoloader for the scripts in this directory (development repository). Not part of the Reqtide API.
 *
 * The Docker setup keeps vendor/ outside the repository (COMPOSER_VENDOR_DIR=/opt/reqtide-vendor); a plain
 * checkout has vendor/ at the repository root. In your own project this file is replaced by
 * `require __DIR__ . '/vendor/autoload.php';`.
 */

(static function (): void {
    $candidates = array_filter([
        getenv('COMPOSER_VENDOR_DIR') !== false ? getenv('COMPOSER_VENDOR_DIR') . '/autoload.php' : null,
        '/opt/reqtide-vendor/autoload.php',
        __DIR__ . '/../../../vendor/autoload.php',
    ]);
    foreach ($candidates as $file) {
        if (is_file($file)) {
            require $file;

            return;
        }
    }
    fwrite(STDERR, "No Composer autoloader found. Run `composer install` (or set COMPOSER_VENDOR_DIR).\n");
    exit(2);
})();
