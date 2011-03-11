<?php
class GooglePageRank {
  var $url;
  var $detail;
  var $base_url = 'http://josh-fowler.com/prapi/?';

  var $pagerank;
  var $cache;
  var $querycount;

  var $status = true;

  function __construct($url = '', $detail='1') {
    $this->url = $url;
    $this->detail = $detail;
  }

  function get() {
    if(!empty($this->url)) {
      $url = $this->base_url . 'd=' . $this->detail . '&url=' . $this->url;
      $googlepr = file_get_contents($url);
      $expandedpr = explode(':', $googlepr);
      $this->pagerank = $expandedpr[0];
      $this->cache = $expandedpr[1];
      $this->querycount = $expandedpr[2];
    }
    else {
      $this->status = false;
    }

  }

}

/* End of file GooglePageRank.php */
/* Location: ./classes/GooglePageRank.php */