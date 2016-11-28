<?php

/*
 * This file is part of the Sqlbox package.
 *
 * (c) Solutions Infini <info@solutionsinfini.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code
 */

namespace Turbo\Database\Util;

/**
 * @author sankar <sankar.suda@gmail.com>
 */
class Log
{
    public static function info($message, $write = false)
    {
        $message = '[INFO] '.self::format($message);
        if ($write) {
            self::write('logger', $message);
        } else {
            echo $message;
        }
    }

    public static function debug($message, $write = false)
    {
        $message = '[DEBU] '.self::format($message);
        if ($write) {
            self::write('logger', $message);
        } else {
            echo $message;
        }
    }

    public static function warning($message, $write = false)
    {
        $message = '[WARN] '.self::format($message);
        if ($write) {
            self::write('logger', $message);
        } else {
            echo $message;
        }
    }

    public static function error($message, $write = false)
    {
        $message = '[ERRO] '.self::format($message);
        if ($write) {
            self::write('logger', $message);
        } else {
            echo $message;
        }
    }

    public static function critical($message, $write = false)
    {
        $message = '[CRIT] '.self::format($message);

        if ($write) {
            self::write('logger', $message);
        } else {
            echo $message;
        }
    }

    public static function format($message, $context = [])
    {
        $data = '['.date('M d H:i:s').'] ';
        $data .= $message.($context ? str_replace("\n", '', var_export($context, true)) : '');
        $data .= "\r\n";

        return $data;
    }

    public static function write($type, $message)
    {
        $path = LOG.$type.'.log';
        self::rotateFile($path);
        file_put_contents($path, self::format($message), FILE_APPEND);
    }

    public static function append($type, $message)
    {
        $path = LOG.$type.'.log';
        self::rotateFile($path);
        file_put_contents($path, $message, FILE_APPEND);
    }

    /**
     * Rotate log file if size specified in config is reached.
     * Also if `rotate` count is reached oldest file is removed.
     *
     * @param string $filename Log file name
     *
     * @return mixed True if rotated successfully or false in case of error.
     *               Void if file doesn't need to be rotated
     */
    public static function rotateFile($filepath)
    {
        $config = [
            'rotate' => 10,
            'size'   => 1024, //500MB
        ];

        $size = $config['size'] * 1024 * 1024;

        clearstatcache(true, $filepath);

        if (!file_exists($filepath) ||
            filesize($filepath) < $size
        ) {
            return;
        }

        if ($config['rotate'] === 0) {
            return unlink($filepath);
        }

        if ($config['rotate']) {
            $files = glob($filepath.'.*');
            if (count($files) >= $config['rotate']) {
                $files = array_slice($files, 0, -$config['rotate']);
                foreach ($files as $file) {
                    unlink($file);
                }
            }
        }

        return rename($filepath, $filepath.'.'.date('Ydmhis'));
    }
}
