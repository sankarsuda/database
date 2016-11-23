<?php

/*
 * This file is part of the Speedwork package.
 *
 * (c) Sankar <sankar.suda@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code
 */

namespace Turbo\Database\Drivers;

use Turbo\Database\DboSource;

/**
 * @author sankar <sankar.suda@gmail.com>
 */
class SqliteDriver extends DboSource
{
    /**
     * Start quote.
     *
     * @var string
     */
    protected $startQuote = '"';

    /**
     * End quote.
     *
     * @var string
     */
    protected $endQuote = '"';

    /**
     * Keeps the transaction statistics of CREATE/UPDATE/DELETE queries.
     *
     * @var array
     */
    protected $_queryStats = [];

    /**
     * Base configuration settings for MySQL driver.
     *
     * @var array
     */
    protected $_baseConfig = [
        'persistent' => true,
        'database'   => null,
    ];

    /**
     * Index of basic SQL commands.
     *
     * @var array
     */
    protected $_commands = [
        'begin'    => 'BEGIN TRANSACTION',
        'commit'   => 'COMMIT TRANSACTION',
        'rollback' => 'ROLLBACK TRANSACTION',
    ];

    /**
     * Connects to the database using options in the given configuration array.
     *
     * @return bool True if the database could be connected, else false
     */
    public function connect()
    {
        $config = $this->config;
        $config = array_merge($this->_baseConfig, $config);
        if (!$config['persistent']) {
            $this->connection = sqlite_open($config['database']);
        } else {
            $this->connection = sqlite_popen($config['database']);
        }
        $this->connected = is_resource($this->connection);

        if ($this->connected) {
            $this->query('PRAGMA count_changes = 1;');
        }

        return $this->connection;
    }

    /**
     * Check whether the MySQL extension is installed/loaded.
     *
     * @return bool
     */
    public function enabled()
    {
        return extension_loaded('sqlite');
    }

    /**
     * Disconnects from database.
     *
     * @return bool True if the database could be disconnected, else false
     */
    public function disconnect()
    {
        @sqlite_close($this->connection);
        $this->connected = false;

        return $this->connected;
    }

    public function fetch($sql)
    {
        $data          = [];
        $this->_result = sqlite_query($this->connection, $sql);

        while ($row = @sqlite_fetch_array($this->_result, SQLITE_ASSOC)) {
            $data[] = $row;
        }

        return $data;
    }

    /**
     * Executes given SQL statement.
     *
     * @param string $sql SQL statement
     *
     * @return resource Result resource identifier
     */
    public function query($sql)
    {
        if (preg_match('/^(INSERT|UPDATE|DELETE)/', $sql)) {
            list($this->_queryStats) = $this->fetch($sql);

            return    $this->_result;
        }
        $this->_result = sqlite_query($this->connection, $sql);

        return $this->_result;
    }

    /**
     * Returns a formatted error message from previous database operation.
     *
     * @return string Error message with error number
     */
    public function lastError()
    {
        $error = sqlite_last_error($this->connection);
        if ($error) {
            return $error.': '.sqlite_error_string($error);
        }
    }

    /**
     * Returns the ID generated from the previous INSERT operation.
     *
     * @param unknown_type $source
     *
     * @return in
     */
    public function lastInsertId()
    {
        return sqlite_last_insert_rowid($this->connection);
    }

    /**
     * Returns number of affected rows in previous database operation. If no previous operation exists,
     * this returns false.
     *
     * @return int Number of affected rows
     */
    public function lastAffected()
    {
        if (!empty($this->_queryStats)) {
            foreach (['rows inserted', 'rows updated', 'rows deleted'] as $key) {
                if (array_key_exists($key, $this->_queryStats)) {
                    return $this->_queryStats[$key];
                }
            }
        }

        return false;
    }

    /**
     * Returns number of rows in previous resultset. If no previous resultset exists,
     * this returns false.
     *
     * @return int Number of rows in resultset
     */
    public function lastNumRows()
    {
        if ($this->hasResult()) {
            sqlite_num_rows($this->_result);
        }

        return false;
    }

    /**
     * Returns a limit statement in the correct format for the particular database.
     *
     * @param int $limit  Limit of results returned
     * @param int $offset Offset from which to start results
     *
     * @return string SQL limit/offset statement
     */
    public function limit($limit, $offset = null)
    {
        if ($limit) {
            $rt = '';
            if (!strpos(strtolower($limit), 'limit') || strpos(strtolower($limit), 'limit') === 0) {
                $rt = ' LIMIT';
            }
            $rt .= ' '.$limit;
            if ($offset) {
                $rt .= ' OFFSET '.$offset;
            }

            return $rt;
        }
    }

    /**
     * Sets the database encoding.
     *
     * @param string $enc Database encoding
     */
    public function setEncoding($enc)
    {
        if (!in_array($enc, ['UTF-8', 'UTF-16', 'UTF-16le', 'UTF-16be'])) {
            return false;
        }

        return $this->query("PRAGMA encoding = \"{$enc}\"") !== false;
    }

    /**
     * Gets the database encoding.
     *
     * @return string The database encoding
     */
    public function getEncoding()
    {
        return $this->fetchRow('PRAGMA encoding');
    }

    /**
     * Overrides DboSource::renderStatement to handle schema generation with SQLite-style indexes.
     *
     * @param string $type
     * @param array  $data
     *
     * @return string
     */
    public function renderStatement($type, $data)
    {
        switch (strtolower($type)) {
            case 'schema':
                extract($data);

                foreach (['columns', 'indexes'] as $var) {
                    if (is_array(${$var})) {
                        ${$var} = "\t".implode(",\n\t", array_filter(${$var}));
                    }
                }

                return "CREATE TABLE {$table} (\n{$columns});\n{$indexes}";
            break;
            default:
                return parent::renderStatement($type, $data);
            break;
        }
    }

    /**
     * Helper function to clean the incoming values.
     **/
    public function escape($str)
    {
        if ($str == '') {
            return '';
        }

        if (function_exists('sqlite_escape_string')) {
            $str = sqlite_escape_string($str);
        } else {
            $str = addslashes($str);
        }

        return trim($str);
    }
}
