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
class MssqlDriver extends DboSource
{
    protected $startQuote = '[';
    protected $endQuote   = ']';

    protected $_baseConfig = [
        'persistent' => true,
        'host'       => 'localhost',
        'username'   => 'root',
        'password'   => '',
        'database'   => 'logics',
        'port'       => '1433',
    ];

    /**
     * Index of basic SQL commands.
     *
     * @var array
     */
    protected $_commands = [
        'begin'    => 'BEGIN TRANSACTION',
        'commit'   => 'COMMIT',
        'rollback' => 'ROLLBACK',
    ];

    /**
     * Define if the last query had error.
     *
     * @var string
     */
    protected $__lastQueryHadError = false;

    /**
     * Connects to the database using options in the given configuration array.
     *
     * @return bool True if the database could be connected, else false
     */
    public function connect()
    {
        $config = $this->config;
        $config = array_merge($this->_baseConfig, $config);

        $os = env('OS');
        if (!empty($os) && strpos($os, 'Windows') !== false) {
            $sep = ',';
        } else {
            $sep = ':';
        }
        $this->connected = false;

        if (is_numeric($config['port'])) {
            $port = $sep.$config['port'];    // Port number
        } elseif ($config['port'] === null) {
            $port = '';                        // No port - SQL Server 2005
        } else {
            $port = '\\'.$config['port'];    // Named pipe
        }

        if (!$config['persistent']) {
            $this->connection = mssql_connect($config['host'].$port, $config['username'], $config['password'], true);
        } else {
            $this->connection = mssql_pconnect($config['host'].$port, $config['username'], $config['password']);
        }

        if (mssql_select_db($config['database'], $this->connection)) {
            $this->qery('SET DATEFORMAT ymd');
            $this->connected = true;
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
        return extension_loaded('mssql');
    }

    /**
     * Disconnects from database.
     *
     * @return bool True if the database could be disconnected, else false
     */
    public function disconnect()
    {
        @mssql_free_result($this->results);
        $this->connected = !@mssql_close($this->connection);

        return !$this->connected;
    }

    public function fetch($sql)
    {
        $data          = [];
        $this->_result = mssql_query($sql, $this->connection);

        while ($row = @mssql_fetch_array($this->_result)) {
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
        $result                    = @mssql_query($sql, $this->connection);
        $this->__lastQueryHadError = ($result === false);

        return $result;
    }

    /**
     * Returns a formatted error message from previous database operation.
     *
     * @return string Error message with error number
     */
    public function lastError()
    {
        if ($this->__lastQueryHadError) {
            $error = mssql_get_last_message();
            if ($error && !preg_match('/contexto de la base de datos a|contesto di database|changed database|contexte de la base de don|datenbankkontext/i', $error)) {
                return $error;
            }
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
        return mssql_result(mysql_query('select SCOPE_IDENTITY()', $this->connection), 0, 0);
    }

    /**
     * Returns number of affected rows in previous database operation. If no previous operation exists,
     * this returns false.
     *
     * @return int Number of affected rows
     */
    public function lastAffected()
    {
        if ($this->_result) {
            return mssql_rows_affected($this->connection);
        }
    }

    /**
     * Returns number of rows in previous resultset. If no previous resultset exists,
     * this returns false.
     *
     * @return int Number of rows in resultset
     */
    public function lastNumRows()
    {
        if ($this->_result) {
            return mssql_num_rows($this->_result);
        }
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
            if (!strpos(strtolower($limit), 'top') || strpos(strtolower($limit), 'top') === 0) {
                $rt = ' TOP';
            }
            $rt .= ' '.$limit;
            if (is_int($offset) && $offset > 0) {
                $rt .= ' OFFSET '.$offset;
            }

            return $rt;
        }
    }

    /**
     * Builds final SQL statement.
     *
     * @param string $type Query type
     * @param array  $data Query data
     *
     * @return string
     */
    public function renderStatement($type, $data)
    {
        switch (strtolower($type)) {
            case 'select':
                extract($data);
                $fields = trim($fields);

                if (strpos($limit, 'TOP') !== false && strpos($fields, 'DISTINCT ') === 0) {
                    $limit  = 'DISTINCT '.trim($limit);
                    $fields = substr($fields, 9);
                }

                if (preg_match('/offset\s+([0-9]+)/i', $limit, $offset)) {
                    $limit = preg_replace('/\s*offset.*$/i', '', $limit);
                    preg_match('/top\s+([0-9]+)/i', $limit, $limitVal);
                    $offset                = intval($offset[1]) + intval($limitVal[1]);
                    $rOrder                = $this->__switchSort($order);
                    list($order2, $rOrder) = [$this->__mapFields($order), $this->__mapFields($rOrder)];

                    return "SELECT * FROM (SELECT {$limit} * FROM (SELECT TOP {$offset} {$fields} FROM {$table} {$alias} {$joins} {$conditions} {$group} {$order}) AS Set1 {$rOrder}) AS Set2 {$order2}";
                } else {
                    return "SELECT {$limit} {$fields} FROM {$table} {$alias} {$joins} {$conditions} {$group} {$order}";
                }
            break;
            case 'schema':
                extract($data);

                foreach ($indexes as $i => $index) {
                    if (preg_match('/PRIMARY KEY/', $index)) {
                        unset($indexes[$i]);
                        break;
                    }
                }

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
     * Reverses the sort direction of ORDER statements to get paging offsets to work correctly.
     *
     * @param string $order
     *
     * @return string
     */
    public function __switchSort($order)
    {
        $order = preg_replace('/\s+ASC/i', '__tmp_asc__', $order);
        $order = preg_replace('/\s+DESC/i', ' ASC', $order);

        return preg_replace('/__tmp_asc__/', ' DESC', $order);
    }

    /**
     * Translates field names used for filtering and sorting to shortened names using the field map.
     *
     * @param string $sql A snippet of SQL representing an ORDER or WHERE statement
     *
     * @return string The value of $sql with field names replaced
     */
    public function __mapFields($sql)
    {
        if (empty($sql) || empty($this->__fieldMappings)) {
            return $sql;
        }
        foreach ($this->__fieldMappings as $key => $val) {
            $sql = preg_replace('/'.preg_quote($val).'/', $this->name($key), $sql);
            $sql = preg_replace('/'.preg_quote($this->name($val)).'/', $this->name($key), $sql);
        }

        return $sql;
    }

    /**
     * Sets the database encoding.
     *
     * @param string $enc Database encoding
     */
    public function setEncoding()
    {
    }

    /**
     * Gets the database encoding.
     *
     * @return string The database encoding
     */
    public function getEncoding()
    {
    }

    /**
     * Helper function to clean the incoming values.
     **/
    public function escape($str)
    {
        if ($str == '') {
            return '';
        }

        if (function_exists('mssql_real_escape_string')) {
            $str = mssql_real_escape_string($str);
        } else {
            $str = addslashes($str);
        }

        return trim($str);
    }
}
