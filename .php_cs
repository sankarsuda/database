<?php

$header = <<< EOF
This file is part of the Speedwork package.

(c) Sankar <sankar.suda@gmail.com>

For the full copyright and license information, please view the LICENSE
file that was distributed with this source code
EOF;

return PhpCsFixer\Config::create()
    ->setUsingCache(false)
    ->setRules(array(
        '@Symfony' => true,
        'align_equals' => true,
        'align_double_arrow' => true,
        'short_array_syntax' => true,
        'ordered_imports' => true,
        'no_useless_return' => true,
        'phpdoc_order' => true,
        'no_short_echo_tag' => true,
        'header_comment' => array('header' => $header),
        'combine_consecutive_unsets' => true,
        'unalign_double_arrow' => false,
        'unalign_equals' => false,
    ))
    ->finder(
        PhpCsFixer\Finder::create()
            ->exclude(__DIR__.'/tests')
            ->in(__DIR__.'/src'))
;
