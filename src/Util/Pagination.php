<?php

/*
 * This file is part of the Speedwork package.
 *
 * (c) Sankar <sankar.suda@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code
 */

namespace Turbo\Util;

/**
 * @author sankar <sankar.suda@gmail.com>
 */
class Pagination
{
    protected $target;
    protected $type;

    public function __construct($type = null)
    {
        $this->type = $type;
    }

    public function setType($type)
    {
        $this->type = $type;

        return $this;
    }

    public function setTarget($url = null)
    {
        if (empty($url)) {
            $url = $this->currentUrl();
        }

        $prefix = (strpos($url, '?') === false ? '?' : '&');

        if (strpos($url, 'page=', 0) === false) {
            $url .= $prefix.'page=1';
        }

        $this->target = $url;

        return $this;
    }

    public function getTarget()
    {
        return $this->target;
    }

    public function paginate($page = 1, $total = 0, $limit = 25, $now_total = 0, $ajax = true)
    {
        $adjacents = 1;
        if (!$limit) {
            $limit = 15;
        }
        if (!$page) {
            $page = 1;
        }

        $this->setTarget();

        $prev     = $page - 1;
        $next     = $page + 1;
        $lastpage = ceil($total / $limit);
        $lpm1     = $lastpage - 1;

        $pagination = '';

        $pagination .= '<div class="ui-load-more-results" data-now="'.$now_total.'" ';
        $pagination .= 'data-total="'.$total.'" data-limit="'.$limit.'" data-current-page="'.$page.'"';
        $pagination .= ' data-next-page = "'.($next).'"';
        $pagination .= ' data-prev-page = "'.($prev).'"';
        $pagination .= '>';

        $pagination .= '<input type="hidden" id="total" name="total" value="'.$total.'"/>';
        $pagination .= '<input type="hidden" id="page" name="page" value="'.$page.'"/>';

        $pagination .= '<ul class="pagination '.(($ajax) ? 'ac-ajax-pagination' : '').'">';

        if ($lastpage > 1) {
            //previous button
            if ($page > 1) {
                $pagination .= $this->linkUrl($prev, '&laquo; prev');
            } else {
                $pagination .= '<li class="disabled"><a href="#">&laquo; prev</a></li>';
            }

            //pages
            if ($lastpage < 7 + ($adjacents * 2)) {
                for ($counter = 1; $counter <= $lastpage; ++$counter) {
                    if ($counter == $page) {
                        $pagination .= "<li class=\"current\"><a href=\"#\" onclick=\"return false\" data-page=\"$counter\">$counter</a></li>";
                    } else {
                        $pagination .= $this->linkUrl($counter);
                    }
                }
            } elseif ($lastpage >= 7 + ($adjacents * 2)) {
                if ($page < 2 + ($adjacents * 2)) {
                    for ($counter = 1; $counter < 4 + ($adjacents * 2); ++$counter) {
                        if ($counter == $page) {
                            $pagination .= "<li class=\"current\"><a href=\"#\" onclick=\"return false\"  data-page=\"$counter\">$counter</a></li>";
                        } else {
                            $pagination .= $this->linkUrl($counter);
                        }
                    }

                    $pagination .= '<li><a href="#" onclick="return false">...</a></li>';
                    $pagination .= $this->linkUrl($lpm1);
                    $pagination .= $this->linkUrl($lastpage);
                } elseif ($lastpage - ($adjacents * 2) > $page && $page > ($adjacents * 2)) {
                    $pagination .= $this->linkUrl('1');
                    $pagination .= $this->linkUrl('2');

                    $pagination .= '<li><a href="#" onclick="return false">...</a></li>';
                    for ($counter = $page - $adjacents; $counter <= $page + $adjacents; ++$counter) {
                        if ($counter == $page) {
                            $pagination .= "<li class=\"current\"><a href=\"#\" onclick=\"return false\"  data-page=\"$counter\">$counter</a></li>";
                        } else {
                            $pagination .= $this->linkUrl($counter);
                        }
                    }
                    $pagination .= '<li><a href="#" onclick="return false">...</a></li>';
                    $pagination .= $this->linkUrl($lpm1);
                    $pagination .= $this->linkUrl($lastpage);
                } else {
                    $pagination .= $this->linkUrl('1');
                    $pagination .= $this->linkUrl('2');

                    $pagination .= '<li><a href="#" onclick="return false">...</a></li>';
                    for ($counter = $lastpage - (1 + ($adjacents * 3)); $counter <= $lastpage; ++$counter) {
                        if ($counter == $page) {
                            $pagination .= "<li class=\"current\"><a href=\"#\" onclick=\"return false\"  data-page=\"$counter\">$counter</a></li>";
                        } else {
                            $pagination .= $this->linkUrl($counter, $counter);
                        }
                    }
                }
            }

            //next button
            if ($page < $counter - 1) {
                $pagination .= $this->linkUrl($next, 'next &raquo;');
            } else {
                $pagination .= '<li class="disabled"><a href="#">next &raquo;</a></li>';
            }
        }
        $pagination .= '</ul>';
        $pagination .= '</div>';

        return $pagination;
    }

    private function linkUrl($find, $print = '')
    {
        $url = preg_replace('#page=([0-9]*)#', 'page='.$find, $this->getTarget());

        $print = ($print) ? $print : $find;

        return '<li><a href="'.$url.'"  data-page="'.$find.'">'.$print.'</a></li>';
    }

    public function paginate2($page = 1, $total = 0, $limit = 10, $now_total = 0)
    {
        $data = '<div class="pagination pagination-minimal">';
        $data .= '<input type="hidden" id="total" name="total" value="'.$total.'"/>';
        $data .= '<input type="hidden" id="page" name="page" value="'.$page.'"/>';

        $details = 'data-now="'.$now_total.'"';
        $details .= ' data-total="'.$total.'" data-limit="'.$limit.'" data-current-page="'.$page.'"';
        $details .= ' data-next-page = "'.($page + 1).'"';

        if ($total && $now_total >= $limit) {
            $data .= '<div class="ui-load-more-results pagination-show-more ac-load-more" '.$details.' data-page="'.($page + 1).'" style="cursor:pointer">Show more results...</div>';
            $data .= '<div class="ui-load-more-results pagination-showing-more ac-load-more-loading" style="display:none" data-total="'.$total.'">';
            $data .= '<span class="ui-results-loader"></span>Loading more results...</div>';
        } else {
            if ($total > 0) {
                $data .= '<div class="ui-load-more-results pagination-finished" '.$details.'><span>No more results to display.</span></div>';
            } else {
                $data .= '<div class="ui-load-more-results pagination-results" '.$details.'><span>Sorry no results found.</span></div>';
            }
        }

        $data .= '</div>';

        return $data;
    }

    public function render($page, $total, $limit, $current)
    {
        $paging = null;
        if ($this->type == 'mixed' && $total <= 500) {
            $paging = $this->paginate2($page, $total, $limit, $current);
        }

        if ($this->type == 'mixed' && $total >= 500) {
            $paging = $this->paginate($page, $total, $limit, $current);
        }

        if ($this->type == 'normal') {
            $paging = $this->paginate($page, $total, $limit, $current);
        }

        if ($this->type == 'scroll') {
            $paging = $this->paginate2($page, $total, $limit, $current);
        }

        if (null === $paging && $this->type != 'api') {
            $paging = $this->paginate($page, $total, $limit, $current);
        }

        $next  = ($current >= $limit) ? $page + 1 : 0;
        $start = $limit * ($page - 1);

        return [
            'now'        => intval($current),
            'next'       => intval($next),
            'page'       => intval($page),
            'limit'      => intval($limit),
            'start'      => intval($start),
            'total'      => intval($total),
            'html'       => $paging,
            'limitstart' => intval($start),
            'nowTotal'   => intval($current),
        ];
    }

    public function currentUrl()
    {
        $pageURL = 'http';
        if ($_SERVER['HTTPS'] == 'on') {
            $pageURL .= 's';
        }
        $pageURL .= '://';
        if ($_SERVER['SERVER_PORT'] != '80') {
            $pageURL .= $_SERVER['HTTP_HOST'].':'.$_SERVER['SERVER_PORT'].$_SERVER['REQUEST_URI'];
        } else {
            $pageURL .= $_SERVER['HTTP_HOST'].$_SERVER['REQUEST_URI'];
        }

        return $pageURL;
    }
}
