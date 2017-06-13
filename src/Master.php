<?php

/*
 * This file is part of the Speedwork package.
 *
 * (c) Sankar <sankar.suda@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code
 */

namespace Turbo\Database;

/**
 * @author sankar <sankar.suda@gmail.com>
 */
class Master extends Database
{
    protected $connections = [];
    protected $params      = [];
    protected $transaction = false;

    public function __construct($params = [])
    {
        if (!isset($params['masters']) && !isset($params['master'])) {
            throw new \InvalidArgumentException('Master or masters configuration is missing.');
        }

        $config = [];

        $master  = $params['master'];
        $masters = $params['masters'];

        unset($params['master'], $params['masters']);

        if (!is_array($masters)) {
            $masters = [$master];
        }

        $config['master'] = array_merge($params, ['connections' => $masters]);

        $this->params = $config;
    }

    public function connect()
    {
        return $this->connectTo();
    }

    public function connectTo($name = null)
    {
        $name = $name ?: 'master';

        if (isset($this->connections[$name])) {
            return $this->setDriver($this->connections[$name]);
        }

        if ($name !== 'master') {
            throw new \InvalidArgumentException('Invalid option to connect(), only master allowed.');
        }

        $config      = $this->params[$name];
        $connections = $config['connections'];
        $strategy    = $config['strategy'];

        if ($strategy == 'random') {
            $connections = [$connections[mt_rand(0, count($connections) - 1)]];
        }

        foreach ($connections as $connection) {
            $this->setConfig(array_merge($config, $connection));
            $this->connections[$name] = parent::connect();
            $connected                = parent::isConnected();

            if ($connected) {
                break;
            }
        }

        return $this->connections[$name];
    }

    public function getFromDB($sql)
    {
        $this->connectTo();

        return parent::getFromDB($sql);
    }

    public function query($sql)
    {
        $this->connectTo();

        return parent::query($sql);
    }
}
