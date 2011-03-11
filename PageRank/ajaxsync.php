<?php
$at      = isset($_REQUEST['at'])?$_REQUEST['at']:'';
$iswords = isset($_REQUEST['iswords'])?$_REQUEST['iswords']:'';
$addval  = isset($_REQUEST['addval'])?$_REQUEST['addval']:'';
$delval  = isset($_REQUEST['delval'])?$_REQUEST['delval']:'';

if(!empty($at)&&!empty($iswords)) {
    // cookie name
    $cookiename = $at;
    if($iswords == 'false') {
        $cookiename .= '_urls';
    }
    else if($iswords == 'true') {
        $cookiename .= '_words';
    }

    // cookie value
    $cookieval = '';
    if(!empty($addval)) {
        // add a value to the cookie
        if($_COOKIE[$cookiename] == "") {
            // if the cookie does not exist
            $cookieval = $addval;
        }
        else {
            // if the cookie exist
            $cookieval = $_COOKIE[$cookiename] . '|' . $addval;
        }
    }
    else if (!empty($delval)) {
        // delete a value from the cookie
        $cookievalarr = explode('|', $_COOKIE[$cookiename]);
        while(list($key, $value) = each($cookievalarr)) {
            if(strtoupper($delval) == strtoupper($value)) {
                array_splice($cookievalarr, intval($key), 1);
            }
        }
        $cookieval = implode('|', $cookievalarr);
    }

    // set cookie to client
    if(setcookie($cookiename, $cookieval, time()+60*60*24*30)) {
        echo "set cookie - $cookiename successfully";
    }

}

/* End of file ajaxsync.php */
/* Location: ./ajaxsync.php */