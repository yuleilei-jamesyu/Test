<?php
$praddress = isset($_REQUEST['PRAddress'])?$_REQUEST['PRAddress']:'';

$googlepagerankval = 0;
$sougourankval = 0;
if(!empty($praddress)) {
    require_once('classes/GooglePageRank.php');
    require_once('classes/SougouRank.php');

    // Google PageRank
    $googlepagerank = new GooglePageRank($praddress);
    $googlepagerank->get();
    if($googlepagerank->status) {
        $googlepagerankval = intval($googlepagerank->pagerank);
    }

    // Sohu Rank
    $sougourank = new SougouRank($praddress);
    $sougourank->get();
    if($sougourank->status) {
        $sougourankval = intval($sougourank->response);
    }
}
?>
<html>
    <head>
        <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
        <script type="text/javascript" src="javascript/globals.js"></script>
        <title>Google/Sougou PR查询</title>
        <style type="text/css" media="all">
            body {
                font-family: Tahoma, Arial, sans-serif;
                font-size: 12px;
            }
            input {
                color: #333333;
                vertical-align: middle;
            }
            img {
                border: 0 none;
                vertical-align: middle;
            }
            a {
                text-decoration: none;
            }
            a:hover {
                text-decoration: underline;
            }
            .box {
                clear: both;
                width: 800px;
            }
            .box h1 {
                background: url("images/h1-bg.gif") repeat-x;
                border-style: solid solid none;
                border-width: 1px 1px 0;
                border-color: #C5E2F2;
                color: #0066CC;
                font-size: 14px;
                font-weight: normal;
                height: 37px;
                line-height: 37px;
                padding-left: 20px;
                margin: 0;
                padding: 0;
            }
            .box .titleft {
                float: left;
                margin-left: 10px;
            }
            .box .titright {
                float: right;
                margin-right: 10px;
            }
            .box1 {
                background: url("images/box1-bg.gif") repeat-x;
                border-style: none solid solid;
                border-width: 0 1px 1px;
                border-color: #BBD7E6;
                line-height: 30px;
                padding: 3px 15px;
            }
            .info2, .info3 {
                color: #333333;
                display: block;
                font-size: 12px;
                line-height: 22px;
                margin-bottom: 5px;
                margin-top: 5px;
                padding-bottom: 5px;
                text-align: left;
            }
            .info3 {
                border-bottom: 1px solid #E5EFF8;
            }
            .info3 .input {
                background: none repeat scroll 0 0 #FFFFFF;
                border: 1px solid #94C6E1;
                color: #22AC38;
                font-weight: bold;
                margin-bottom: 5px;
                padding: 5px;
                width: 350px;
                ime-mode: disabled;
            }
            .but {
                background: url("images/but.gif") repeat-x scroll 50% top #CDE4F2;
                border: 1px solid #C5E2F2;
                cursor: pointer;
                cursor: hand;
                margin-left: 5px;
                margin-bottom: 5px;
                width: 90px;
                height: 30px;
            }
            #pr_results span {
                font-size: 12px;
                text-align: center;
                line-height: 30px;
            }
            #ToolBox { border:#BFC2D3 1px solid;width:220px;position:absolute; background-color:#fff; }
            #ToolBox ul { text-align:left; padding:0; margin:2px;}
            #ToolBox ul li{ list-style-type:none; line-height:25px; background-color:#FAFAFA; font-size: 12px;}
            #ToolBox ul li a{ display:block;cursor:pointer; width:99%; padding-left:2px; font-family:Arial; color: #22AC38;text-decoration: none;}
            #ToolBox ul li a:hover{  background-color:#E8F0FB; color:Blue;}

            .bot-nav {
                background: none repeat scroll 0 0 #FFFFFF;
                border: 1px solid #BBD7E6;
                clear: both;
                color: #DDDDDD;
                line-height: 32px;
                text-align: center;
                width: 800px;
            }

            .bot-nav a {
                color: #0269AC;
            }

            .interval {
                height: 5px;
            }

            .foot {
                clear: both;
                color: #888888;
                height: 40px;
                padding: 10px 0;
                text-align: center;
            }
        </style>
    </head>
    <body>
        <center>
        <div class="box">
            <h1>
                <div class="titleft"><a title="PR查询" href="#">Google PR查询 | 搜狗 PR查询</a></div>
                <!-- <div class="titright"><a title="联系作者" href=" #">联系作者</a></div> -->
            </h1>
            <div class="box1">
                <span class="info3">
                    <form method="post" action="index.php">
                    请输入要查询的网址：<input id="PRAddress" class="input" type="text" name="PRAddress" url="true" autocomplete="off" value="<?php echo $praddress; ?>"><input class="but" type="submit" value="查询">
                    </form>
                </span>
                <?php
                if(!empty($praddress)) {
                    echo "<div id=\"pr_results\">\n";
                    if($googlepagerankval >= 0) {
                        $googlepagerankimg = 'Rank_' . $googlepagerankval . '.gif';
                        echo "<span id=\"pr\">Google <img src=\"images/ranks/$googlepagerankimg\" alt=\"Google PageRank\"></span>\n";
                    }
                    if($sougourankval >= 0) {
                        $sougourankimg = 'sRank_' . $sougourankval . '.gif';
                        echo "<span id=\"sougoupr\">搜狗 <img src=\"images/ranks/$sougourankimg\" alt=\"Sougou Rank\"></span>\n";
                    }
                    echo "</div>\n";
                }
                ?>
            </div>
        </div>
        <div onmouseout='BoxHide()' onmouseover="this.style.display='block'" id="ToolBox">
            <div id="xhead"></div>
                <ul id="xlist"></ul>
            <div id="xfoot"></div>
        </div>
        <div class="interval"></div>
        <div class="box">
            <h1>
                <div class="titleft"><a title="工具简介" href="#">工具简介</a></div>
            </h1>
            <div class="box1">
                <span class="info2">Google PR全称为Google PageRank(网页级别)是Google 搜索引擎用于评测一个网页“重要性”的一种方法。Google 通过 PageRank 来调整搜索结果，使那些更具“重要性”的网页在搜索结果中排名获得提升，从而提高搜索结果的相关性和质量。</span>
                <span class="info2">搜狗 PR是搜狗衡量网页重要性的指标，不仅考察了网页之间链接关系，同时考察了链接质量、链接之间的相关性等特性，是机器根据Sogou Rank算法自动计算出来的，值从0至10不等。网页评级越高，该网页在搜索中越容易被检索到。</span>
            </div>
        </div>
        <div class="interval"></div>
        <div class="bot-nav">
            <a target="_blank" href="#">联系我们</a>
            |
            <a target="_blank" href="#">版权声明</a>
        </div>
        <div class="foot">
        <?php
          $from_year = '2011';
          $this_year = date('Y');
          echo "&copy; CopyRight $from_year-$this_year. All Rights Reserved";
        ?>
        </div>
        <script type="text/javascript">Init();</script>
        </center>
    </body>
</html>
<?php

/* End of file index.php */
/* Location: ./index.php */
?>