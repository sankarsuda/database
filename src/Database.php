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

use Exception;
use Turbo\Database\Util\Pagination;

/**
 * @author sankar <sankar.suda@gmail.com>
 */
class Database
{
    protected $cache     = false;
    protected $connected = false;
    protected $driver;
    protected $config = [];
    protected $prefix;
    protected $sql;

    public function __construct($config = [])
    {
        $this->setConfig($config);
    }

    public function setConfig($config = [])
    {
        $this->config = $config;
    }

    public function getConfig()
    {
        return $this->config;
    }

    public function connect()
    {
        $config = $this->config;

        $driver       = ($config['driver']) ? $config['driver'] : 'mysqli';
        $this->cache  = $config['cache'];
        $this->prefix = $config['prefix'];

        if (false !== strpos($driver, '\\')) {
            $database = $driver;
        } else {
            $database = '\\Turbo\\Database\\Drivers\\'.ucfirst($driver).'Driver';
        }

        $driver = new $database($config);
        $this->setDriver($driver);
        $this->connected = $driver->connect();

        return $driver;
    }

    protected function setDriver($driver)
    {
        $this->driver = $driver;

        return $driver;
    }

    protected function getDriver()
    {
        return $this->driver;
    }

    public function isConnected()
    {
        if ($this->connected === false) {
            return false;
        }

        $this->connected = $this->driver->query('SELECT 1');

        return $this->connected;
    }

    /**
     * disconnect from driver.
     */
    public function disConnect()
    {
        $this->driver->disConnect();
        $this->connected = false;
    }

    /**
     * Reconnects to database server with optional new settings.
     *
     * @param array $config An array defining the new configuration settings
     *
     * @return bool True on success, false on failure
     */
    public function reConnect($config = [])
    {
        if (!empty($config)) {
            $this->disConnect();
            $this->setConfig($config);

            return $this->connect();
        }

        $connected = $this->driver->query('SELECT 1');
        if ($connected) {
            return true;
        }

        $this->disConnect();

        return $this->connect();
    }

    /**
     * fetch() is used to retrieve a dataset. fetch() determines whether to use the
     * cache or not, and queries either the database or the cache file accordingly.
     *
     * @param string $sql
     * @param int|string cache time
     * @param string $name(optional) if name is empty i will take md5 of the sql
     *
     * @return array|false
     **/
    public function fetch($sql, $duration = null, $name = null)
    {
        if ($duration === 'daily') {
            $duration = '+1 DAY';
        }

        $sql = str_replace('#__', $this->prefix, $sql);

        $this->sql = $sql;

        if ($this->cache == false || empty($duration)) {
            return $this->getFromDb($sql);
        }

        $cache_key = $name ?: md5($sql);
        $cache_key = 'db_' . $cache_key;

        return $this->get('cache')->remember($cache_key, function () use ($sql) {
            return $this->getFromDb($sql);
        }, $duration);

        return $this->getFromDb($sql);
    }

    public function fetchAssoc($sql, $duration = null, $name = null)
    {
        $data = $this->fetch($sql, $duration, $name);

        return $data[0];
    }

    public function fetchAll($sql, $duration = null, $name = null)
    {
        return $this->fetch($sql, $duration, $name);
    }

    protected function getFromDb($sql)
    {
        return $this->driver->fetch($sql);
    }

    /**
     * Begin a transaction.
     *
     * @param model $model
     *
     * @return bool True on success, false on fail
     *              (i.e. if the database/model does not support transactions,
     *              or a transaction has not started)
     */
    public function begin()
    {
        return $this->driver->begin();
    }

    /**
     * Commit a transaction.
     *
     * @param model $model
     *
     * @return bool True on success, false on fail
     *              (i.e. if the database/model does not support transactions,
     *              or a transaction has not started)
     */
    public function commit()
    {
        return $this->driver->commit();
    }

    /**
     * Rollback a transaction.
     *
     * @param model $model
     *
     * @return bool True on success, false on fail
     *              (i.e. if the database/model does not support transactions,
     *              or a transaction has not started)
     */
    public function rollback()
    {
        return $this->driver->rollback();
    }

    /**
     * Use query() to execute INSERT, UPDATE, DELETE statements.
     *
     * @param stirng $sql
     *
     * @return bool
     **/
    public function query($sql)
    {
        $sql       = str_replace('#__', $this->prefix, $sql);
        $this->sql = $sql;

        return $this->driver->query($sql);
    }

    public function lastError()
    {
        return $this->driver->lastError();
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
        return $this->driver->lastInsertId();
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
        return $this->driver->lastNumRows();
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
        return $this->driver->lastAffected();
    }

    public function getTableList()
    {
        return $this->driver->getTableList();
    }

    public function getTableFields($tables, $typeonly = true)
    {
        return $this->driver->getTableFields($tables, $typeonly);
    }

    /**
     * This is like fetch this will generate query and outout the results.
     *
     * @param string $table
     * @param string $type
     * @param array  $params
     *
     * @return mixed
     **/
    public function find($table, $type = 'first', $params = [])
    {
        if (is_array($table)) {
            return $this->findTables($table, $type, $params);
        }

        $cache      = ($params['cache']) ? $params['cache'] : '';
        $cache_name = ($params['cache_name']) ? $params['cache_name'] : '';

        $params['table'] = $table;
        $params['type']  = $type;
        //dont' conside nagitive values
        if ($params['limit'] < 0) {
            unset($params['limit']);
        }

        $helpers = [];

        if (is_array($params['helpers'])) {
            $helpers = array_merge($helpers, $params['helpers']);
        }

        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);

            if ($help) {
                $res = $help->beforeFind($params);
                if ($res === false) {
                    return [];
                }
                //if stop further
                if ($params['stop'] === true) {
                    break;
                }

                $results = $params['result'];
            }
        }

        $type  = $params['type'];
        $table = $params['table'];

        $params['table'] = $table;
        $params['type']  = $type;

        switch ($type) {
            case 'count':

                $params['fields'] = ['count(*) as total'];
                unset($params['limit'], $params['order'], $params['offset'], $params['page']);
                $query = $this->driver->buildStatement($params, $table);

                if ($params['group']) {
                    $query = 'SELECT COUNT(*) AS total FROM (' . $query . ') as tmp';
                }
                $data    = $this->fetch($query, $cache, $cache_name);
                $results = intval($data[0]['total']);
                unset($data);
                break;

            case 'all':
                $query   = $this->driver->buildStatement($params, $table);
                $results = $this->fetch($query, $cache, $cache_name);
                break;
            /*
             * array('fields' => array('value','group','title'),
             *       'empty' => array('module','mod_view'),
             *       'replace' => array('parent_id','name')
             *       );
             */
            case 'list':
                $query = $this->driver->buildStatement($params, $table);
                $rows  = $this->fetch($query, $cache, $cache_name);

                $fields = [];
                if (is_array($rows[0])) {
                    $fields = array_keys($rows[0]);
                }

                if (count($fields) == 0) {
                    $results = &$rows;
                } else {
                    $empty   = $params['empty'];
                    $replace = $params['replace'];

                    $value  = $fields[0];
                    $option = $fields[1];
                    $optgrp = $fields[2];
                    $res    = [];
                    foreach ($rows as $row) {
                        if (count($fields) == 1) {
                            $res[$row[$value]] = $row[$value];
                        } elseif (count($fields) == 2) {
                            $res[$row[$value]] = $row[$option];
                        } else {
                            if ($empty) {
                                $v  = $empty[0];
                                $re = $empty[1];

                                if (!$row[$v] && $option == $re) {
                                    $res[$row[$re]][$row[$value]] = $row[$option];
                                } elseif (!$row[$v] && $optgrp == $re) {
                                    $res[$row[$optgrp]][$row[$value]] = $row[$re];
                                } else {
                                    $res[$row[$optgrp]][$row[$value]] = $row[$option];
                                }
                            } else {
                                $res[$row[$optgrp]][$row[$value]] = $row[$option];
                            }

                            if ($replace) {
                                $res[$row[$option]][$row[$value]] = $row[$optgrp];
                            }
                        }
                    }
                    unset($value, $option, $rows, $optgrp, $v, $re);
                    $results = &$res;
                }
                break;
            /*
            $database->find('menu','neighbors',array('field'=>ordering,'value'=>1));
             */
            case 'neighbors':
            case 'siblings':

                $field = $params['field'];
                $value = $params['value'];
                unset($params['value'], $params['field']);

                if (empty($params['limit'])) {
                    $params['limit'] = 1;
                }

                if (!is_array($params['conditions'])) {
                    $conditions = [];
                } else {
                    $conditions = $params['conditions'];
                }

                //build first query
                $params['order']      = [$field . ' DESC'];
                $params['conditions'] = array_merge([$field . ' < ' => $value], $conditions);

                $query = $this->driver->buildStatement($params, $table);
                $data  = $this->fetch($query, $cache, $cache_name);

                $results['prev'] = ($params['limit']) ? $data[0] : $data;

                //build second query
                $params['order']      = [$field];
                $params['conditions'] = array_merge([$field . ' > ' => $value], $conditions);

                $query = $this->driver->buildStatement($params, $table);
                $data  = $this->fetch($query, $cache, $cache_name);

                $results['next'] = ($params['limit']) ? $data[0] : $data;

                break;
            /*
            $html =  '<li><input type="checkbox" name="category[]" class="liChild" value="{menuID}"/>{name}';
            $database->find('menu','threaded',
            array('order'=>array('parent_id', 'ordering'),
            'html'=>array($html,'</li>')
            'parent'=>0,
            'field'=>array('primary_key','parent_id'),
            'parent_tag'=>array('<ul>','</ul>'))
            );
             */
            case 'threaded':

                if (!$params['field']) {
                    throw new Exception('field not found');
                }

                $query = $this->driver->buildStatement($params, $table);
                $data  = $this->fetch($query, $cache, $cache_name);

                $menuData = ['items' => [], 'parents' => []];
                foreach ($data as $menuItem) {
                    // Creates entry into items array with current menu item id ie. $menuData['items'][1]
                    $menuData['items'][$menuItem[$params['field'][0]]] = $menuItem;
                    // Creates entry into parents array. Parents array contains a list of all items with children
                    $menuData['parents'][$menuItem[$params['field'][1]]][] = $menuItem[$params['field'][0]];
                }
                $results = $this->buildThreaded($params['parent'], $menuData, $params['html'], $params['parent_tag']);
                break;

            case 'first':
                $params['limit'] = 1;
                $query           = $this->driver->buildStatement($params, $table);
                $results         = $this->fetch($query, $cache, $cache_name);
                $results         = $results[0];
                break;

            case 'field':
                $query   = $this->driver->buildStatement($params, $table);
                $rows    = $this->fetch($query, $cache, $cache_name);
                $results = [];
                foreach ($rows as $key => $v) {
                    $results[$key][] = $v;
                }
                unset($rows);
                break;
        }

        //if method exists
        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);
            if ($help && method_exists($help, 'afterFind')) {
                $results = $help->afterFind($results, $params);
            }
        }

        return $results;
    }

    /*
     * bulid threaded items
     * @param mixed $parent
     * @param array $menuData
     * @param string $ht ($html)
     * @param array $parentTag
     */

    protected function buildThreaded($parent, $menuData, $replace, $parentTag)
    {
        $html = '';
        $html .= $parentTag[0];

        $replace = (array) $replace;
        $ht      = $replace[0];
        $end_tag = $replace[1];

        if (!$ht) {
            throw new Exception('html not found');
        }
        if (isset($menuData['parents'][$parent])) {
            foreach ($menuData['parents'][$parent] as $itemId) {
                $data    = $menuData['items'][$itemId];
                $hts     = $ht;
                $matches = [];
                preg_match_all('~\{([^{}]+)\}~', $ht, $matches, PREG_SET_ORDER);
                foreach ($matches as $match) {
                    $k   = $match[0];
                    $v   = $data[$match[1]];
                    $hts = str_replace($k, $v, $hts);
                }
                $html .= $hts;
                if (isset($menuData['parents'][$itemId])) {
                    $html .= $this->buildThreaded($itemId, $menuData, $replace, $parentTag);
                }
                $html .= $end_tag;
            }
        }
        $html .= $parentTag[1];

        return $html;
    }

    public function findTables($tables = [], $type = 'all', $params = [])
    {
        if ($type == 'count') {
            $total = 0;

            foreach ($tables as $table) {
                $total += $this->find($table, $type, $params);
            }

            return $total;
        }

        if ($type == 'first') {
            foreach ($tables as $table) {
                $rows = $this->find($table, $type, $params);
                if (!empty($rows)) {
                    return $rows;
                }
            }
        }

        if ($type == 'all') {
            $total = 0;
            $data  = [];
            $limit = $params['limit'];

            foreach ($tables as $table) {
                $rows = $this->find($table, 'all', $params);

                $data = array_merge($data, $rows);

                if ($limit) {
                    $total += count($rows);

                    if ($total >= $limit) {
                        break;
                    } else {
                        // get remaings results from other tables
                        $params['limit'] = $limit - $total;
                    }
                }
            }

            unset($rows);

            return $data;
        }

        return $this->find($table, $type, $params);
    }

    /**
     * paginate.
     */
    public function paginate($tables, $type = 'all', $params = [])
    {
        if (is_array($type)) {
            $params = $type;
        }

        $paging = $params['paging'] ?: $params['pagingtype'];
        $paging = $paging ?: 'mixed';
        $page   = $_REQUEST['page'];
        $dlimit = $_REQUEST['limit'];

        $page = ($params['page'] && is_numeric($params['page']))
        ? $params['page']
        : !empty($page) && is_numeric($page)
        ? $page
        : 1;

        $limit = $params['limit'];

        if ($limit === false) {
            $limit = $params['llimit'];
        }

        $limit = ($limit && is_numeric($limit)) ? $limit : 25;

        if (!empty($dlimit) && is_numeric($dlimit)) {
            $limit = $dlimit;
        }

        $limit_start = $limit * ($page - 1);

        $params['limit'] = $limit;
        $params['page']  = $page;

        $hasTotal = false;
        $total    = 0;
        $nowTotal = 0;
        $data     = [];

        if (!is_array($tables)) {
            $tables = [$tables];
        }

        if ($params['total']) {
            $total    = $params['total'];
            $hasTotal = true;
        } else {
            if (($paging != 'scroll' && $paging != 'mixed') ||
                (($paging == 'scroll' || $paging == 'mixed') &&
                    $page == 1)) {
                foreach ($tables as $table) {
                    $total += $this->find($table, 'count', $params);
                }

                $hasTotal = true;
            }
        }

        foreach ($tables as $table) {
            $rows = $this->find($table, 'all', $params);

            if ($nowTotal) {
                $data = array_merge($data, $rows);
            } else {
                $data = $rows;
            }

            $nowTotal += count($rows);

            if ($nowTotal >= $limit) {
                break;
            } else {
                // get remaings results from other tables
                $params['limit'] = $limit - $nowTotal;
            }
        }

        unset($rows);

        $i = $limit_start;
        if (is_array($data)) {
            foreach ($data as &$row) {
                $row['serial'] = ++$i;
            }
        }

        $total = ($hasTotal) ? $total : $this->data['total'];
        $total = $total ?: $nowTotal;

        $pagination = new Pagination();
        $pagination->setType($paging);

        $respose           = [];
        $respose['total']  = $total;
        $respose['data']   = $data;
        $respose['paging'] = $pagination->render($page, $total, $limit, $nowTotal);

        return $respose;
    }

    /**
     * function save is used to save the associate array to database.
     *
     * @param string $table
     * @param array  $data
     *
     * @return bool
     **/
    public function save($table, $data = [], $details = [])
    {
        if (count($data) == 0) {
            return false;
        }

        $helpers = [];

        if (is_array($details['helpers'])) {
            $helpers = array_merge($helpers, $details['helpers']);
        }

        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);

            if ($help) {
                $res = $help->beforeSave($data, $table, $details);
                if ($res === false) {
                    return true;
                }

                if ($details['stop'] === true) {
                    break;
                }
            }
        }
        //end of helpers

        $k = $v = [];
        foreach ($data as $key => $value) {
            if (is_array($value)) {
                $va = [];
                foreach ($value as $k2 => $v2) {
                    $va[] = ($v2) ? $this->driver->value($v2) : "''";
                    $k[]  = $this->driver->name($k2);
                }
                $v[] = '(' . @implode(',', $va) . ')';
            } else {
                $v[] = ($value) ? $this->driver->value($value) : "''";
                $k[] = $this->driver->name($key);
            }
        }

        $k = array_unique($k);

        $params           = [];
        $params['table']  = $table;
        $params['fields'] = $k;
        $params['values'] = $v;

        $query = $this->driver->buildStatement($params, $table, 'insert');

        $results = $this->query($query);

        //if method exists
        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);
            if ($help && method_exists($help, 'afterSave')) {
                $results = $help->afterSave($results, $params, $details, $query);
            }
        }

        return $results;
    }

    /**
     * Update Table based on condition with data.
     *
     * @param string $table
     * @param array  $data
     * @param array  $conditions
     *
     * @return bool
     **/
    public function update($table, $data = [], $conditions = [], $details = [])
    {
        if (count($data) == 0) {
            return false;
        }

        $params               = [];
        $params['table']      = $table;
        $params['fields']     = $data;
        $params['conditions'] = $conditions;

        //check is there any callback helper in this Query
        $helpers = [];

        if (is_array($details['helpers'])) {
            $helpers = array_merge($helpers, $details['helpers']);
        }

        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);

            if ($help) {
                $res = $help->beforeUpdate($params, $details);
                if ($res === false) {
                    return true;
                }

                if ($details['stop'] === true) {
                    break;
                }
            }
        }
        //end of helpers

        $k = [];
        foreach ($params['fields'] as $key => $value) {
            if ($key && !is_numeric($key)) {
                $k[] = $this->driver->name($key) . ' = ' . $this->driver->value($value);
            } else {
                $k[] = $value;
            }
        }
        $params['fields'] = $k;

        $query = $this->driver->buildStatement($params, $params['table'], 'update');

        $results = $this->query($query);

        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);
            if ($help && method_exists($help, 'afterUpdate')) {
                $results = $help->afterUpdate($results, $params, $details, $query);
            }
        }

        return $results;
    }

    public function cascade($table, $data = [], $conditions = [], $details = [])
    {
        $rows = $this->find($table, 'count', [
            'conditions' => $conditions,
        ]);

        if ($rows > 0) {
            return $this->update($table, $data, $conditions, $details);
        }

        if (count(array_filter($data))) {
            return $this->save($table, $data, $details);
        }
    }

    /**
     * DELETE Records from table based on condition.
     *
     * @param string             $table
     * @param array() (optional) $conditions
     *
     * @return bool
     **/
    public function delete($table, $conditions = [], $details = [])
    {
        $params               = [];
        $params['table']      = $table;
        $params['conditions'] = $conditions;
        if ($details['limit']) {
            $params['limit'] = $details['limit'];
        }

        //check is there any callback helper in this Query
        $helpers = [];

        if (is_array($details['helpers'])) {
            $helpers = array_merge($helpers, $details['helpers']);
        }

        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);

            if ($help) {
                $res = $help->beforeDelete($params, $details);
                if ($res === false) {
                    return true;
                }

                if ($params['stop'] === true) {
                    break;
                }
            }
        }
        //end of helpers

        $query = $this->driver->buildStatement($params, $params['table'], 'delete');

        $results = $this->query($query);

        foreach ($helpers as $helper) {
            $help = $this->get('resolver')->helper($helper);
            if ($help && method_exists($help, 'afterDelete')) {
                $results = $help->afterDelete($results, $params, $details, $query);
            }
        }

        return $results;
    }

    /**
     * Deletes all the records in a table and resets the count of the auto-incrementing
     * primary key, where applicable.
     *
     * @param mixed $table A string or model class representing the table to be truncated
     *
     * @return bool SQL TRUNCATE TABLE statement, false if not applicable
     */
    public function truncate($table)
    {
        return $this->query('TRUNCATE TABLE ' . $table);
    }

    public function escape($value)
    {
        return $this->driver->escape($value);
    }

    /**
     * Alias function for buildStatement.
     *
     * @param string $table
     * @param array  $params
     */
    public function buildQuery($table, $params = [], $type = 'select')
    {
        return $this->driver->buildStatement($params, $table, $type);
    }

    /**
     * Alias function for buildStatement.
     *
     * @param string $table
     * @param array  $params
     */
    public function buildConditions($conditions = [])
    {
        return $this->driver->conditions($conditions);
    }

    /**
     * Output information about an SQL query. The SQL statement, number of rows in resultset,
     * and execution time in microseconds. If the query fails, an error is output instead.
     *
     * @param string $sql Query to show information on
     */
    public function showQuery($echo = false)
    {
        $error = $this->lastError();
        $r     = '<p>';
        if ($error) {
            $r .= "<span style = \"color:Red;\"><b>SQL Error:</b> {$error}</span>";
        }
        $r .= '<b>Query:</b> ' . $this->sql;
        $r .= '</p>';

        if ($echo) {
            echo $r;
        } else {
            return $r;
        }
    }
}
