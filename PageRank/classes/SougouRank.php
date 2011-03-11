<?php
class SougouRank {
    // Example: http://rank.ie.sogou.com/sogourank.php?ur=http://www.sina.com.cn
    var $url;
    var $host = 'rank.ie.sogou.com';
    var $path = '/sogourank.php';
    var $user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.98 Safari/534.13';

    var $response;
    var $content;

    var $status = true;
    //var $error  = array();

    function __construct($url = '') {
        $this->url = urlencode($url);
        $this->path .= '?ur=' . $this->url;
    }

    function get() {
        if(!empty($this->url)) {
            $fp = fsockopen($this->host, 80, $errno, $errstr, 30);
            if(!$fp) {
                $this->status = false;
                //$this->error['error_number'] = $errno;
                //$this->error['error_msg']    = $errstr;
            }
            else {
                // here, we use double quotes to replace single quotes, or 400 error.
                fputs($fp, 'GET ' . $this->path . " HTTP/1.1\r\n");
                fputs($fp, 'User-Agent: ' . $this->user_agent . "\r\n");
                fputs($fp, 'Host: ' . $this->host . "\r\n");
                fputs($fp, "Connection: close\r\n\r\n");

                while(!feof($fp)) {
                    $line = fgets($fp);
                    $this->content .= $line;
                }
                fclose($fp);

                if(preg_match('/sogourank=(\d+)/', $this->content, $matches) > 0) {
                    $this->response .= $matches[1];
                }
            }
        }
        else {
            $this->status = false;
        }
    }

}

/* End of file SougouRank.php */
/* Location: ./classes/SougouRank.php */