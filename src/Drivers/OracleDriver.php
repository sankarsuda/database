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
class OracleDriver extends DboSource
{
    /**
     * Starting Quote.
     *
     * @var string
     */
    protected $startQuote = '';

    /**
     * Ending Quote.
     *
     * @var string
     */
    protected $endQuote = '';

    /**
     * Query limit.
     *
     * @var int
     */
    protected $_limit = -1;

    /**
     * Query offset.
     *
     * @var int
     */
    protected $_offset = 0;

    /**
     * Enter description here...
     *
     * @var unknown_type
     */
    protected $_map;

    /**
     * Current Row.
     *
     * @var mixed
     */
    protected $_currentRow;

    /**
     * Number of rows.
     *
     * @var int
     */
    protected $_numRows;

    /**
     * Query results.
     *
     * @var mixed
     */
    protected $_results;

    /**
     * Last error issued by oci extension.
     *
     * @var unknown_type
     */
    protected $_error;

    /**
     * Base configuration settings for MySQL driver.
     *
     * @var array
     */
    protected $_baseConfig = [
        'persistent' => true,
        'host'       => 'localhost',
        'username'   => 'system',
        'password'   => '',
        'database'   => '',
        'nls_sort'   => '',
        'nls_comp'   => '',
    ];

/*
 * Table-sequence map
 *
 * @var unknown_type
 */
    protected $_sequenceMap = [];

    /**
     * Connects to the database using options in the given configuration array.
     *
     * @return bool True if the database could be connected, else false
     */
    public function connect()
    {
        $config = $this->config;
        $config = array_merge($this->_baseConfig, $config);

        $config['charset'] = !empty($config['charset']) ? $config['charset'] : null;

        if (!$config['persistent']) {
            $this->connection = @ocilogon($config['username'], $config['password'], $config['database'], $config['charset']);
        } else {
            $this->connection = @ociplogon($config['username'], $config['password'], $config['database'], $config['charset']);
        }

        if ($this->connection) {
            $this->connected = true;
            if (!empty($config['nls_sort'])) {
                $this->query('ALTER SESSION SET NLS_SORT='.$config['nls_sort']);
            }

            if (!empty($config['nls_comp'])) {
                $this->query('ALTER SESSION SET NLS_COMP='.$config['nls_comp']);
            }
            $this->execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
        } else {
            $this->connected = false;
            $this->lastError();

            return false;
        }

        return $this->connection;
    }

    /**
     * Check whether the Oracle extension is installed/loaded.
     *
     * @return bool
     */
    public function enabled()
    {
        return extension_loaded('oci8');
    }

    /**
     * Disconnects from database.
     *
     * @return bool True if the database could be disconnected, else false
     */
    public function disconnect()
    {
        if ($this->connection) {
            $this->connected = !ocilogoff($this->connection);

            return !$this->connected;
        }
    }

    public function fetch($sql)
    {
        $data          = [];
        $this->_result = $stid = oci_parse($this->connection, $sql);
        oci_execute($stid);
        $data = oci_fetch_array($this->_result, OCI_ASSOC);

        oci_free_statement($this->_result);
        oci_close($this->connection);

        return $data;
    }

    /**
     * Returns a formatted error message from previous database operation.
     *
     * @return string Error message with error number
     */
    public function error($source = null, $clear = false)
    {
        if ($source) {
            $e = ocierror($source);
        } else {
            $e = ocierror();
        }
        $this->_error = $e['message'];
        if ($clear) {
            $this->_error = null;
        }

        return $this->_error;
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
        $this->_statementId = @ociparse($this->connection, $sql);
        if (!$this->_statementId) {
            $this->_setError($this->connection);

            return false;
        }

        if ($this->_transaction) {
            $mode = OCI_DEFAULT;
        } else {
            $mode = OCI_COMMIT_ON_SUCCESS;
        }

        if (!@ociexecute($this->_statementId, $mode)) {
            $this->_setError($this->_statementId);

            return false;
        }

        $this->_setError(null, true);

        switch (ocistatementtype($this->_statementId)) {
            case 'DESCRIBE':
            case 'SELECT':
                $this->scrapeSql($sql);
                break;
            default:
                return $this->_statementId;
            break;
        }

        if ($this->_limit >= 1) {
            ocisetprefetch($this->_statementId, $this->_limit);
        } else {
            ocisetprefetch($this->_statementId, 3000);
        }
        $this->_numRows    = ocifetchstatement($this->_statementId, $this->_results, $this->_offset, $this->_limit, OCI_NUM | OCI_FETCHSTATEMENT_BY_ROW);
        $this->_currentRow = 0;
        $this->limit();

        return $this->_statementId;
    }

    /**
     * Returns the ID generated from the previous INSERT operation.
     *
     * @param unknown_type $source
     *
     * @return in
     */
    public function lastInsertId($source)
    {
        $sequence = $this->_sequenceMap[$source];
        $sql      = "SELECT $sequence.currval FROM dual";

        if (!$this->execute($sql)) {
            return false;
        }

        while ($row = $this->fetchRow()) {
            return $row[$sequence]['currval'];
        }

        return false;
    }

    /**
     * Returns number of affected rows in previous database operation. If no previous operation exists,
     * this returns false.
     *
     * @return int Number of affected rows
     */
    public function lastAffected()
    {
        return $this->_statementId ? ocirowcount($this->_statementId) : false;
    }

    /**
     * Returns number of rows in previous resultset. If no previous resultset exists,
     * this returns false.
     *
     * @return int Number of rows in resultset
     */
    public function lastNumRows()
    {
        return $this->_numRows;
    }

    /**
     * Sets the database encoding.
     *
     * @param string $enc Database encoding
     */
    public function setEncoding($enc)
    {
        return $this->query('ALTER SESSION SET NLS_LANGUAGE='.$enc) != false;
    }

    /**
     * Gets the database encoding.
     *
     * @return string The database encoding
     */
    public function getEncoding()
    {
        $sql = 'SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER=\'NLS_LANGUAGE\'';
        if (!$this->query($sql)) {
            return false;
        }

        if (!$row = $this->fetchRow()) {
            return false;
        }

        return $row[0]['VALUE'];
    }

    /**
     * Fetch result row.
     *
     * @return array
     */
    public function fetchRow()
    {
        if ($this->_currentRow >= $this->_numRows) {
            ocifreestatement($this->_statementId);
            $this->_map        = null;
            $this->_results    = null;
            $this->_currentRow = null;
            $this->_numRows    = null;

            return false;
        }
        $resultRow = [];

        foreach ($this->_results[$this->_currentRow] as $index => $field) {
            list($table, $column) = $this->_map[$index];

            if (strpos($column, ' count')) {
                $resultRow[0]['count'] = $field;
            } else {
                $resultRow[$table][$column] = $this->_results[$this->_currentRow][$index];
            }
        }
        ++$this->_currentRow;

        return $resultRow;
    }

    /**
     * Fetches the next row from the current result set.
     *
     * @return unknown
     */
    public function fetchResult()
    {
        return $this->fetchRow();
    }

    /**
     * Checks to see if a named sequence exists.
     *
     * @param string $sequence
     *
     * @return bool
     */
    public function sequenceExists($sequence)
    {
        $sql = "SELECT SEQUENCE_NAME FROM USER_SEQUENCES WHERE SEQUENCE_NAME = '$sequence'";
        if (!$this->query($sql)) {
            return false;
        }

        return $this->fetchRow();
    }

    /**
     * Creates a database sequence.
     *
     * @param string $sequence
     *
     * @return bool
     */
    public function createSequence($sequence)
    {
        $sql = "CREATE SEQUENCE $sequence";

        return $this->query($sql);
    }

    /**
     * Create trigger.
     *
     * @param string $table
     *
     * @return mixed
     */
    public function createTrigger($table)
    {
        $sql = 'CREATE OR REPLACE TRIGGER pk_'.$table."_trigger BEFORE INSERT ON $table FOR EACH ROW BEGIN SELECT pk_$table.NEXTVAL INTO :NEW.ID FROM DUAL; END;";

        return $this->query($sql);
    }

    /**
     * Scrape the incoming SQL to create the association map. This is an extremely
     * experimental method that creates the association maps since Oracle will not tell us.
     *
     * @param string $sql
     *
     * @return false if sql is nor a SELECT
     */
    public function scrapeSql($sql)
    {
        $sql           = str_replace('"', '', $sql);
        $preFrom       = preg_split('/\bFROM\b/', $sql);
        $preFrom       = $preFrom[0];
        $find          = ['SELECT'];
        $replace       = [''];
        $fieldList     = trim(str_replace($find, $replace, $preFrom));
        $fields        = preg_split('/,\s+/', $fieldList); //explode(', ', $fieldList);
        $lastTableName = '';

        foreach ($fields as $key => $value) {
            if ($value != 'COUNT(*) AS count') {
                if (preg_match('/\s+(\w+(\.\w+)*)$/', $value, $matches)) {
                    $fields[$key] = $matches[1];

                    if (preg_match('/^(\w+\.)/', $value, $matches)) {
                        $fields[$key]  = $matches[1].$fields[$key];
                        $lastTableName = $matches[1];
                    }
                }
                /*
                if (preg_match('/(([[:alnum:]_]+)\.[[:alnum:]_]+)(\s+AS\s+(\w+))?$/i', $value, $matches)) {
                    $fields[$key]	= isset($matches[4]) ? $matches[2] . '.' . $matches[4] : $matches[1];
                }
                */
            }
        }
        $this->_map = [];

        foreach ($fields as $f) {
            $e = explode('.', $f);
            if (count($e) > 1) {
                $table = $e[0];
                $field = strtolower($e[1]);
            } else {
                $table = 0;
                $field = $e[0];
            }
            $this->_map[] = [$table, $field];
        }
    }

    /**
     * Modify a SQL query to limit (and offset) the result set.
     *
     * @param int $limit  Maximum number of rows to return
     * @param int $offset Row to begin returning
     *
     * @return modified SQL Query
     */
    public function limit($limit = -1, $offset = 0)
    {
        $this->_limit  = (int) $limit;
        $this->_offset = (int) $offset;
    }

    /**
     * This method should quote Oracle identifiers. Well it doesn't.
     * It would break all scaffolding and all of Cake's default assumptions.
     *
     * @param unknown_type $var
     *
     * @return unknown
     */
    public function name($name)
    {
        if (strpos($name, '.') !== false && strpos($name, '"') === false) {
            list($model, $field) = explode('.', $name);
            if ($field[0] == '_') {
                $name = "$model.\"$field\"";
            }
        } else {
            if ($name[0] == '_') {
                $name = "\"$name\"";
            }
        }

        return $name;
    }

    /**
     * Begin a transaction.
     *
     * @param unknown_type $model
     *
     * @return bool True on success, false on fail
     *              (i.e. if the database/model does not support transactions)
     */
    public function begin()
    {
        $this->_transaction = true;

        return true;
    }

    /**
     * Rollback a transaction.
     *
     * @param unknown_type $model
     *
     * @return bool True on success, false on fail
     *              (i.e. if the database/model does not support transactions,
     *              or a transaction has not started)
     */
    public function rollback()
    {
        return ocirollback($this->connection);
    }

    /**
     * Commit a transaction.
     *
     * @param unknown_type $model
     *
     * @return bool True on success, false on fail
     *              (i.e. if the database/model does not support transactions,
     *              or a transaction has not started)
     */
    public function commit()
    {
        $this->_transaction = false;

        return ocicommit($this->connection);
    }

    /**
     * Renders a final SQL statement by putting together the component parts in the correct order.
     *
     * @param string $type
     * @param array  $data
     *
     * @return string
     */
    public function renderStatement($type, $data)
    {
        extract($data);
        $aliases = null;

        switch (strtolower($type)) {
            case 'select':
                return "SELECT {$fields} FROM {$table} {$alias} {$joins} {$conditions} {$group} {$order} {$limit}";
            break;
            case 'create':
                return "INSERT INTO {$table} ({$fields}) VALUES ({$values})";
            break;
            case 'update':
                if (!empty($alias)) {
                    $aliases = "{$this->alias}{$alias} ";
                }

                return "UPDATE {$table} {$aliases}SET {$fields} {$conditions}";
            break;
            case 'delete':
                if (!empty($alias)) {
                    $aliases = "{$this->alias}{$alias} ";
                }

                return "DELETE FROM {$table} {$aliases}{$conditions}";
            break;
            case 'schema':
                foreach (['columns', 'indexes'] as $var) {
                    if (is_array(${$var})) {
                        ${$var} = "\t".implode(",\n\t", array_filter(${$var}));
                    }
                }
                if (trim($indexes) != '') {
                    $columns .= ',';
                }

                return "CREATE TABLE {$table} (\n{$columns}{$indexes})";
            break;
            case 'alter':
                break;
        }
    }
}
