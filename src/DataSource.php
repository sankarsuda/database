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

use Turbo\Database\Util\Log;

/**
 * @author sankar <sankar.suda@gmail.com>
 */
class DataSource
{
    protected $connection = false;

    /**
     * Are we connected to the DataSource?
     *
     * @var bool
     */
    protected $connected = false;

    /**
     * The default configuration of a specific DataSource.
     *
     * @var array
     */
    protected $_baseConfig = [];

    /**
     * Holds references to descriptions loaded by the DataSource.
     *
     * @var array
     */
    protected $_descriptions = [];

    /**
     * Holds a list of sources (tables) contained in the DataSource.
     *
     * @var array
     */
    protected $_sources = null;

    /**
     * The DataSource configuration.
     *
     * @var array
     */
    protected $config = [];

    /**
     * Whether or not this DataSource is in the middle of a transaction.
     *
     * @var bool
     */
    protected $transaction = false;

    /**
     * Whether or not source data like available tables and schema descriptions
     * should be cached.
     *
     * @var bool
     */
    protected $cacheSources = true;

    /**
     * Constructor.
     *
     * @param array $config Array of configuration information for the datasource
     */
    public function __construct($config = [])
    {
        $this->setConfig($config);
    }

    /**
     * Begin a transaction.
     *
     * @return bool Returns true if a transaction is not in progress
     */
    public function begin()
    {
        return !$this->transaction;
    }

    /**
     * Commit a transaction.
     *
     * @return bool Returns true if a transaction is in progress
     */
    public function commit()
    {
        return $this->transaction;
    }

    /**
     * Rollback a transaction.
     *
     * @return bool Returns true if a transaction is in progress
     */
    public function rollback()
    {
        return $this->transaction;
    }

    /**
     * Returns the ID generated from the previous INSERT operation.
     *
     * @param mixed $source
     *
     * @return mixed Last ID key generated in previous INSERT
     */
    public function lastInsertId()
    {
        return false;
    }

    /**
     * Returns the number of rows returned by last operation.
     *
     * @param mixed $source
     *
     * @return int Number of rows returned by last operation
     */
    public function lastNumRows()
    {
        return false;
    }

    /**
     * Returns the number of rows affected by last query.
     *
     * @param mixed $source
     *
     * @return int Number of rows affected by last query
     */
    public function lastAffected()
    {
        return false;
    }

    /**
     * Check whether the conditions for the Datasource being available
     * are satisfied. Often used from connect() to check for support
     * before establishing a connection.
     *
     * @return bool Whether or not the Datasources conditions for use are met
     */
    public function enabled()
    {
        return true;
    }

    /**
     * Sets the configuration for the DataSource.
     * Merges the $config information with the _baseConfig and the existing $config property.
     *
     * @param array $config The configuration array
     */
    public function setConfig($config = [])
    {
        $this->config = array_merge($this->_baseConfig, $this->config, $config);
    }

    /**
     * Returns the schema name. Override this in subclasses.
     *
     * @return string schema name
     */
    public function getSchemaName()
    {
    }

    /**
     * Closes a connection. Override in subclasses.
     *
     * @return bool
     */
    public function close()
    {
        return $this->connected = false;
    }

    protected function logSqlError($sql)
    {
        $message = $this->lastError() . ' : ' . $sql;

        Log::write('sql', $message);

        return true;
    }

    protected function table($name)
    {
        return str_replace('#__', $this->prefix, $name);
    }

    /**
     * Closes the current datasource.
     */
    public function __destruct()
    {
        if ($this->transaction) {
            $this->rollback();
        }
        if ($this->connected) {
            $this->close();
        }
    }
}
