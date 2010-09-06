#!/usr/bin/perl -w
use strict;
#======================================================================
# Description: manipulate and sent out the scorecard team monthly report.
# Author: jameyu(jameyu@cisco.com)
# Date: 2010/08/23
#======================================================================
use Getopt::Std;
use Date::Manip;
use Template;
use Data::Dumper;

use lib '../../../../../utils/lib';
use MyDB;
use ParseConfig;
use FileUtils;
#----------------------------------------------------------------------
# get the environment details ... will determine which conf file to use
#----------------------------------------------------------------------

# check options
my %opts;
getopts( "re:", \%opts );

if ( (!defined $opts{'r'}) || (!defined $opts{'e'}) ) {
    usage();
    exit;
}
my $env      = $opts{'e'};
my %env_opts = (
                'development' =>
                    '../conf/team_monthly_report_development.conf',
                'test'        =>
                    '../conf/team_monthly_report_test.conf',
                'production'  =>
                    '../conf/team_monthly_report_production.conf',
               );

if ( !defined $env_opts{$env} ) {
    usage();
    exit;
}
my $config_file = $env_opts{$env};
if ( ! -e $config_file ) {
    print "Config File not defined\n";
    usage();
    exit;
}
#----------------------------------------------------------------------
# lets put the config values into a hash - %config_vars_rh
#----------------------------------------------------------------------
my ( $stat, $err ) = ParseConfig::colon($config_file);

if ( !$stat ) {
    die $err;
}
my $config_vars_rh = $err;

# datetime stuff
my $month_name  = $config_vars_rh->{'month_name'};
my $month_start = $config_vars_rh->{'month_start'};
my $month_end   = $config_vars_rh->{'month_end'};

die "month_name not defined"  unless ( defined $month_name );
die "month_start not defined" unless ( defined $month_start );
die "month_end not defined"   unless ( defined $month_end );

# -- database stuff --
my $db_alias = $config_vars_rh->{'db_alias'};
die "db_alias not defined" unless ( defined $db_alias );

my $query_scores_sql  = $config_vars_rh->{'query_scores_sql'};
my $query_scores_path = $config_vars_rh->{'query_scores_path'};

die "query_scores_sql not defined"  unless ( defined $query_scores_sql );
die "query_scores_path not defined" unless ( defined $query_scores_path );

# template stuff
my $tmpl_dir    = $config_vars_rh->{'template_dir'};
my $report_tmpl = $config_vars_rh->{'report_template'};
my $header_tmpl = $config_vars_rh->{'header_template'};
my $footer_tmpl = $config_vars_rh->{'footer_template'};

die "tmpl_dir not defined"    unless ( defined $tmpl_dir );
die "report_tmpl not defined" unless ( defined $report_tmpl );
die "header_tmpl not defined" unless ( defined $header_tmpl );
die "footer_tmpl not defined" unless ( defined $footer_tmpl );

# mail stuff
my $email_from = $config_vars_rh->{'email_from'};
my $email_to   = $config_vars_rh->{'email_to'};

die "email_from not defined" unless ( defined $email_from );
die "email_to not defined"   unless ( defined $email_to );

#----------------------------------------------------------------------
# create connection to the kb db
#----------------------------------------------------------------------
my ($kb_stat, $kb_err) = MyDB::getDBH($db_alias);

if (!$kb_stat) {
    die $kb_err;
}
my $kb_dbh = $kb_err;

#----------------------------------------------------------------------
# pull out the scores from the kb.scorecard_scores table and put data
# into a hash - %scorecard_report
#----------------------------------------------------------------------
my %scorecard_report = ();

# parse the SQL sentences from *.sql
my $query_scores_file = $query_scores_path . "/" . $query_scores_sql;
my ( $sql_stat, $sql_err ) = FileUtils::file2string(
    {   file             => $query_scores_file,
        comment_flag     => 1,
        blank_lines_flag => 1
    }
);
if ( !$sql_stat ) {
    die($sql_err);
}
my $select_scores_sql = $sql_err;
my $select_scores_sth = $kb_dbh->prepare($select_scores_sql)
    or die $kb_dbh->errstr;

$select_scores_sth->execute($month_start, $month_end);
while ( my $select_scores_rh = $select_scores_sth->fetchrow_hashref ) {

    my $unique_id   = $select_scores_rh->{'unique_id'};
    my $article_id  = $select_scores_rh->{'article_id'};
    my $scorer_id   = $select_scores_rh->{'scorer_id'};
    my $score_value = $select_scores_rh->{'score_value'};
    my $created_ts  = $select_scores_rh->{'created_ts'};

    my $week_ending = "WE "
                      . $select_scores_rh->{'week_ending'}
                      . " SQL";

    my %score = ();
    $score{'score'}   = $score_value;
    $score{'created'} = $created_ts;

    my $author_id = get_article_owner($article_id, $kb_dbh);

    # query the email addr for the author, manager and coach id from
    # the scorecard_users table based on the author id

    # if the corresponding article's author is not a CSE,
    # then ignore this score.
    my $author_email = "";
    my $coach_id     = "";
    my $mgr_email    = "";
    my $coach_y      = 0;

    my $author_email_sql
    = "SELECT
         primary_email, manager, role, coach, coach_y
       FROM
         scorecard_users
       WHERE
        _del = '0'
        AND (role = 'lead' OR role = 'cse' OR role = 'sonata' OR role = 'e4e')
        AND user_id=?";
    my $author_email_sth
    = $kb_dbh->prepare($author_email_sql) or die $kb_dbh->errstr;

    $author_email_sth->execute($author_id);
    my $author_email_rh = $author_email_sth->fetchrow_hashref;
    if ($author_email_rh) {
        $author_email = $author_email_rh->{'primary_email'};
        if ($author_email_rh->{'coach'}) {
            $coach_id  = $author_email_rh->{'coach'};
        }
        $mgr_email = $author_email_rh->{'manager'};

        $coach_y   = $author_email_rh->{'coach_y'};
    }
    else {
        next;
    }
    # if the coach field in the scorecard_users table for a coach is null,
    # then set himself/herself as the local coach here
    if ($coach_y && ($coach_id eq "") ) {
       $coach_id = $author_id;
    }

    my $author_name = $author_email;
    $author_name =~ s/@.+//g;

    # query local coach's email addr based on the coach id
    my $coach_email     = "cics-kcscouncil\@cisco.com";
    my $coach_mgr_email = "";
    my $coach_team      = "";

    my $coach_email_sql
    = "SELECT
         primary_email, manager, team
       FROM
         scorecard_users
       WHERE
         user_id=? AND _del='0'";
    my $coach_email_sth = $kb_dbh->prepare($coach_email_sql)
        or die $kb_dbh->errstr;

    $coach_email_sth->execute($coach_id);
    my $coach_email_rh = $coach_email_sth->fetchrow_hashref;
    if ($coach_email_rh) {
        $coach_email = $coach_email_rh->{'primary_email'};
        $coach_team  = $coach_email_rh->{'team'};

        my $coach_mgr_id = $coach_email_rh->{'manager'};
        my $cisco_email_sql
        = "SELECT
             primary_email
           FROM
             scorecard_users
           WHERE
             email = ? and _del = '0'";
        my $cisco_email_sth = $kb_dbh->prepare($cisco_email_sql)
            or die $kb_dbh->errstr;

        $cisco_email_sth->execute($coach_mgr_id);
        my $cisco_email_rh = $cisco_email_sth->fetchrow_hashref;
        if ($cisco_email_rh->{'primary_email'}) {
            $coach_mgr_email = $cisco_email_rh->{'primary_email'};
        }
    }
    my $coach_name = $coach_email;
    $coach_name =~ s/@.+//g;

    my $coach = "";
    if ($scorer_id eq $coach_id) {
        $coach = "local_coach";
    }
    else {
        $coach = "other_coach";
    }
    $scorecard_report{$coach_name}{$coach}{$author_name}{$week_ending}
{$article_id}{$unique_id}
    = \%score;

    push @{$scorecard_report{$coach_name}{$coach}{$author_name}{$week_ending}
{$article_id}{'sorter'}},
    $unique_id;

    # sorting author names
    if ( !grep { $_ eq $author_name } @{$scorecard_report{$coach_name}{$coach}
{'sorter'}}  ) {
        push @{$scorecard_report{$coach_name}{$coach}{'sorter'}}, $author_name;
    }

    # sorting weekends
    if( !grep { $_ eq $week_ending } @{$scorecard_report{$coach_name}{$coach}
{$author_name}{'sorter'}}) {
        push @{$scorecard_report{$coach_name}{$coach}{$author_name}{'sorter'}},
        $week_ending;
    }

    # sorting article ids
    if ( !grep { $_ eq $article_id } @{$scorecard_report{$coach_name}{$coach}
{$author_name}{$week_ending}{'sorter'}} ) {
         push @{$scorecard_report{$coach_name}{$coach}{$author_name}
{$week_ending}{'sorter'}}, $article_id;
    }

    $scorecard_report{$coach_name}{'manager_email'} = $coach_mgr_email;
    $scorecard_report{$coach_name}{'coach_email'}   = $coach_email;
    $scorecard_report{$coach_name}{'coach_id'}      = $coach_id;
    $scorecard_report{$coach_name}{'team'}          = $coach_team;
}
#print Dumper(%scorecard_report);
$select_scores_sth->finish;

#----------------------------------------------------------------------
# re-organize the data in hash - %scorecard_report
#----------------------------------------------------------------------
foreach my $coach_name ( keys %scorecard_report ) {
    my $local_coach_new_article_scored    = 0;
    my $local_coach_edited_article_scored = 0;
    my $local_coach_total_scores          = 0;
    my $local_coach_unique_article_scored = 0;

    foreach my $coach ( keys %{$scorecard_report{$coach_name}} ) {
        next if $coach eq "manager_email";
        next if $coach eq "coach_email";
        next if $coach eq "coach_id";
        next if $coach eq "team";

        if ( $coach eq "local_coach" ) {
            $scorecard_report{$coach_name}{$coach}{'title'}
            = "Scoring by $coach_name";
        }
        else {
            $scorecard_report{$coach_name}{$coach}{'title'}
            = "Scoring by Other Coaches";
        }

        @{$scorecard_report{$coach_name}{$coach}{'metrics'}}
        = ( 'new articles',
            'edited articles',
            'total updated articles',
            'new articles scored',
            'edited articles scored',
            'total times scored',
            'unique articles scored'
          );
        @{$scorecard_report{$coach_name}{$coach}{'special_metrics'}}
        = ( 'Monthly Average');

        foreach my $author_name
            ( @{$scorecard_report{$coach_name}{$coach}{'sorter'}} ) {
            my $new_articles                 = 0;
            my $edited_articles              = 0;
            my $total_updated_articles       = 0;
            my $new_article_scored_amount    = 0;
            my $edited_article_scored_amount = 0;
            my $total_times_scored_amount    = 0;
            my $unique_articles_scored       = 0;
            my $author_score_amount          = 0;

            foreach my $week ( @{$scorecard_report{$coach_name}{$coach}
{$author_name}{'sorter'}} ) {

                foreach my $article_id ( @{$scorecard_report{$coach_name}
{$coach}{$author_name}{$week}{'sorter'}} ) {

                    # pull out an article's created datetime from the history
                    # table
                    my $article_created_ts_sql
                    = "SELECT rowmtime FROM history WHERE article_id=? AND
                       status='6'";
                    my $article_created_ts_sth
                    = $kb_dbh->prepare($article_created_ts_sql)
                      or die $kb_dbh->errstr;

                    $article_created_ts_sth->execute($article_id);
                    my $article_created_ts_rh
                    = $article_created_ts_sth->fetchrow_hashref;
                    my $article_created_ts
                    = $article_created_ts_rh->{'rowmtime'};

                    # pull out an article's first edited datetime from the
                    # history table.
                    my $article_edited_ts_sql
                    = "SELECT rowmtime FROM history WHERE article_id=?
                       AND status='5' ORDER BY rowmtime asc LIMIT 0, 1";
                    my $article_edited_ts_sth
                    = $kb_dbh->prepare($article_edited_ts_sql)
                        or die $kb_dbh->errstr;

                    $article_edited_ts_sth->execute($article_id);
                    my $article_edited_ts_rh
                    = $article_edited_ts_sth->fetchrow_hashref;
                    my $article_edited_ts = $article_edited_ts_rh->{'rowmtime'};

                    # figure out new article scored, edited article scored,
                    # total number of times scored for per article
                    my $new_article_scored    = 0;
                    my $edited_article_scored = 0;
                    my $total_times_scored    = 0;

                    $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'new articles'}
                    = "0";
                    $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'edited articles'}
                    = "0";

                    foreach my $unique_id ( @{$scorecard_report{$coach_name}
{$coach}{$author_name}{$week}{$article_id}{'sorter'}} ) {

                        my $created_ts = $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{$unique_id}{'created'};

                        # 1 - if an article haven't been edited before, then
                        # this score belongs to new article scored.
                        # 2 - if an article have been edited ever:
                        # a, this score's created datetime is former than
                        # article's first edited datetime, then the score
                        # belongs to new article scored.
                        # b, on the contrary, this score belongs to edited
                        # article scored.
                        my $created_flag = 0;
                        my $edited_flag  = 0;
                        if ( defined $article_edited_ts ) {
                            $created_flag
                            = Date_Cmp($article_created_ts, $created_ts);
                            $edited_flag
                            = Date_Cmp($created_ts, $article_edited_ts);
                            if ($created_flag < 0 && $edited_flag < 0) {
                                $new_article_scored ++;
                                $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{'new articles'} = "1";
                            }
                            elsif ($edited_flag > 0) {
                                $edited_article_scored ++;
                                $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{'edited articles'} = "1";
                            }
                        }
                        else {
                            $created_flag
                            = Date_Cmp($article_created_ts, $created_ts);
                            if ($created_flag < 0) {
                                $new_article_scored ++;
                                $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{'new articles'} = "1";
                            }
                        }
                    }

                    $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'new article scored'}
                    = $new_article_scored;
                    $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'edited article scored'}
                    = $edited_article_scored;

                    $total_times_scored
                    = $new_article_scored + $edited_article_scored;
                    $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'total times scored'}
                    = $total_times_scored;

                    $new_article_scored_amount    += $new_article_scored;
                    $edited_article_scored_amount += $edited_article_scored;

                    # use the latest score as this article's score
                    my $score_num = 0;
                    foreach my $unique_id ( @{$scorecard_report{$coach_name}
{$coach}{$author_name}{$week}{$article_id}{'sorter'}} ) {
                        $score_num ++;

                        if ($score_num == 1) {
                            my $created_ts
                            = $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{$unique_id}{'created'};
                            my $score
                            = $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{$unique_id}{'score'};

                            $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'created'}
                            = $created_ts;
                            $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'score'}
                            = $score;
                        }
                        delete $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{$unique_id};
                    }
                    delete $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'sorter'};

                    if ($scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'new articles'} eq "1") {
                        $new_articles ++;
                    }
                    if ($scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id}{'edited articles'} eq "1") {
                        $edited_articles ++;
                    }

                }

                my $score_amount = 0;
                foreach my $article_id ( @{$scorecard_report{$coach_name}
{$coach}{$author_name}{$week}{'sorter'}} ) {

                    my $score = $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{$article_id}{'score'};
                    $score_amount += $score;

                    delete $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{$article_id};
                }

                my $article_count = scalar @{$scorecard_report{$coach_name}
{$coach}{$author_name}{$week}{'sorter'}};
                $unique_articles_scored += $article_count;

                my $score_avg = sprintf "%.2f", $score_amount/$article_count;
                $scorecard_report{$coach_name}{$coach}{$author_name}{$week}
{'score'}
                = $score_avg;
                $author_score_amount += $score_avg;

                delete $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}{'sorter'};

                my $week_score = $scorecard_report{$coach_name}{$coach}
{$author_name}{$week}{'score'};
                $scorecard_report{$coach_name}{$coach}{$author_name}
{$week}
                = $week_score;
            }

            # new articles
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'new articles'}
            = $new_articles;

            # edited articles
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'edited articles'}
            = $edited_articles;

            # total updated articles
            $total_updated_articles = $new_articles + $edited_articles;
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'total updated articles'}
            = $total_updated_articles;

            # new articles scored
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'new articles scored'}
            = $new_article_scored_amount;

            # edited articles scored
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'edited articles scored'}
            = $edited_article_scored_amount;

            # total times scored
            $total_times_scored_amount = $new_article_scored_amount
                                         + $edited_article_scored_amount;
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'total times scored'}
            = $total_times_scored_amount;

            # unique articles scored
            $scorecard_report{$coach_name}{$coach}{$author_name}
{'unique articles scored'}
            = $unique_articles_scored;

            if ( $coach eq "local_coach" ) {
                $local_coach_new_article_scored += $new_article_scored_amount;

                $local_coach_edited_article_scored
                += $edited_article_scored_amount;

                $local_coach_total_scores
                += $total_times_scored_amount;

                $local_coach_unique_article_scored += $unique_articles_scored;
            }

            # Monthly Average
            my $week_amount = scalar @{$scorecard_report{$coach_name}{$coach}
{$author_name}{'sorter'}};
            my $author_score_avg = $author_score_amount/$week_amount;

            $scorecard_report{$coach_name}{$coach}{$author_name}
{'Monthly Average'}
            = $author_score_avg;

            # sort the metrics
            foreach my $week ( @{$scorecard_report{$coach_name}{$coach}
{$author_name}{'sorter'}} ) {
                if ( !grep { $_ eq $week }
                    @{$scorecard_report{$coach_name}{$coach}{'metrics'}} ) {
                    push @{$scorecard_report{$coach_name}{$coach}{'metrics'}},
                    $week;
                }
                if ( !grep { $_ eq $week } @{$scorecard_report{$coach_name}
{$coach}{'special_metrics'}} ) {

                    push @{$scorecard_report{$coach_name}{$coach}
{'special_metrics'}}, $week;
                }
            }
            delete $scorecard_report{$coach_name}{$coach}{$author_name}
{'sorter'};

        }
        push @{$scorecard_report{$coach_name}{$coach}{'metrics'}},
        "Monthly Average";

        my %metrics_values = ();

        my $team = ucfirst($scorecard_report{$coach_name}{'team'});
        my $total_title = "$team Total";
        my $avg_title   = "$team Average";
        foreach my $metric
            ( @{$scorecard_report{$coach_name}{$coach}{'metrics'}} ) {

            foreach my $author_name
                ( @{$scorecard_report{$coach_name}{$coach}{'sorter'}} ) {

                if (exists $scorecard_report{$coach_name}{$coach}{$author_name}
{$metric}) {
                    $metrics_values{$metric} += $scorecard_report{$coach_name}
{$coach}{$author_name}{$metric};
                }
            }

            my $author_count = scalar @{$scorecard_report{$coach_name}{$coach}
{'sorter'}};
            my $metric_value = $metrics_values{$metric};
            my $metric_avg   = $metrics_values{$metric}/$author_count;

            $scorecard_report{$coach_name}{$coach}{$total_title}{$metric}
            = $metric_value;
            $scorecard_report{$coach_name}{$coach}{$avg_title}{$metric}
            = $metric_avg;
        }

        if ( !grep { $_ eq $total_title } @{$scorecard_report{$coach_name}
{$coach}{'sorter'}} ) {
                push @{$scorecard_report{$coach_name}{$coach}{'sorter'}},
                $total_title;
        }
        if ( !grep { $_ eq $avg_title } @{$scorecard_report{$coach_name}
{$coach}{'sorter'}} ) {
                push @{$scorecard_report{$coach_name}{$coach}{'sorter'}},
                $avg_title;
        }

        foreach my $author_name
            ( @{$scorecard_report{$coach_name}{$coach}{'sorter'}} ) {

            foreach my $metric ( keys
                %{$scorecard_report{$coach_name}{$coach}{$author_name}} ) {

                my $metric_value = $scorecard_report{$coach_name}{$coach}
{$author_name}{$metric};
                if ( $metric_value == 0 ) {
                    $metric_value = "";
                }
                else {
                    $metric_value = sprintf "%.2f", $metric_value;
                    if ( grep { $_ eq $metric } @{$scorecard_report{$coach_name}
{$coach}{'special_metrics'}} ) {
                        $metric_value .= "%";
                    }
                }
                $scorecard_report{$coach_name}{$coach}{$author_name}{$metric}
                = $metric_value;
            }
        }
        delete $scorecard_report{$coach_name}{$coach}{'special_metrics'};
    }

    # coach scoring
    my $coach_id = $scorecard_report{$coach_name}{'coach_id'};
    my $other_team_articles_scored = 0;

    my $articles_scored_sql
    = "SELECT
         DISTINCT(article_id)
       FROM scorecard_scores WHERE scorer_id=?";
    my $articles_scored_sth
    = $kb_dbh->prepare($articles_scored_sql) or die $kb_dbh->errstr;

    $articles_scored_sth->execute($coach_id);
    while ( my $articles_scored_rh = $articles_scored_sth->fetchrow_hashref ) {
        my $article_id = $articles_scored_rh->{'article_id'};

        my $author_id  = get_article_owner($article_id, $kb_dbh);

        my $local_coach_id = "";
        my $local_coach_sql
        = "SELECT
             coach, coach_y
           FROM scorecard_users WHERE _del='0' AND user_id=?";
        my $local_coach_sth = $kb_dbh->prepare($local_coach_sql)
            or die $kb_dbh->errstr;

        $local_coach_sth->execute($author_id);
        my $local_coach_rh = $local_coach_sth->fetchrow_hashref;
        if ( $local_coach_rh->{'coach'} ) {
            $local_coach_id = $local_coach_rh->{'coach'};
        }
        else {
            if ( $local_coach_rh->{'coach_y'} eq "1" ) {
                $local_coach_id = $author_id;
            }
        }

        if ( $local_coach_id ne $coach_id ) {
            $other_team_articles_scored += 1;
        }
    }

    my $coach_title = "coach_scoring";
    $scorecard_report{$coach_name}{$coach_title}{'title'} = "Coach scoring";
    @{$scorecard_report{$coach_name}{$coach_title}{'metrics'}}
    = ( 'new articles scored',
        'edited articles scored',
        'other team articles scored',
        'total scores',
        'unique articles scored'
      );

    $scorecard_report{$coach_name}{$coach_title}{'new articles scored'}
    = $local_coach_new_article_scored;
    $scorecard_report{$coach_name}{$coach_title}{'edited articles scored'}
    = $local_coach_edited_article_scored;
    $scorecard_report{$coach_name}{$coach_title}{'other team articles scored'}
    = $other_team_articles_scored;
    $scorecard_report{$coach_name}{$coach_title}{'total scores'}
    = $local_coach_total_scores;
    $scorecard_report{$coach_name}{$coach_title}{'unique articles scored'}
    = $local_coach_unique_article_scored;

    delete $scorecard_report{$coach_name}{'coach_id'};
    delete $scorecard_report{$coach_name}{'team'};

}
$kb_dbh->disconnect;

#----------------------------------------------------------------------
# output the report email
#----------------------------------------------------------------------
my $tt = Template->new(
    {   INCLUDE_PATH => $tmpl_dir, 
        EVAL_PERL    => 1,
    }
) || die $Template::ERROR, "\n";

foreach my $coach_name ( keys %scorecard_report ) {

    my $mgr_email   = $scorecard_report{$coach_name}{'manager_email'};
    my $coach_email = $scorecard_report{$coach_name}{'coach_email'};

    # generate the body of report email
    my $output;
    my %input_vars;
    %{$input_vars{'items'}} = %{$scorecard_report{$coach_name}};
    #print Dumper(\%{$input_vars{'items'}});

    $tt->process($report_tmpl, \%input_vars, \$output);
    #print "$output";
    my $digest = get_email($header_tmpl, $output, $footer_tmpl);

    if ($env =~ /development|test/i) {
        $digest .= "<p>manager: $mgr_email</p>";
    }

    my $from = $coach_email;
    my $to;
    if ($env =~ /development|test/i) {
        $to = $email_to;
    }
    elsif ($env =~ /production/i) {
        $to = $mgr_email;
    }

    my $cc;
    if ($env =~ /development|test/i) {
        $cc = "";
    }
    elsif ($env =~ /production/i) {
        $cc = $coach_email;
    }

    my $bcc;
    if ($env =~ /development|test/i) {
        $bcc = "";
    }
    elsif ($env =~ /production/i) {
        $bcc = $email_to;
    }

    my $subject = "KCS Article Scorecard Monthly Team Summary for $coach_name"
                  ." for $month_name";
    email_results($from, $to, $cc, $bcc, $subject, $digest);

    #last;
}

#----------------------------------------------------------------------
# Subroutines...
#----------------------------------------------------------------------
#----------------------------------------------------------------------
# figure out an article's author/owner
#----------------------------------------------------------------------
sub get_article_owner {

    my $article_id = shift;
    my $dbh        = shift;

    # firstly, we query an articles's author from the articles table,
    # then query the real author from the realowner_article table.
    # if records exist in the latter, we overwrite the former

    # set 'chaag' as the default author
    my $author_id = 1;

    my $article_owner_sql = "SELECT owner FROM articles WHERE id=?";
    my $article_owner_sth = $dbh->prepare($article_owner_sql)
        or die $dbh->errstr;

    $article_owner_sth->execute($article_id);
    my $article_owner_rh = $article_owner_sth->fetchrow_hashref;
    if ($article_owner_rh) {
        $author_id = $article_owner_rh->{'owner'};
    }
    $article_owner_sth->finish;

    my $realowner_sql
    = "SELECT user_id FROM realowner_article WHERE article_id=?";
    my $realowner_sth = $dbh->prepare($realowner_sql) or die $dbh->errstr;

    $realowner_sth->execute($article_id);
    my $realowner_rh = $realowner_sth->fetchrow_hashref;
    if ($realowner_rh) {
        $author_id = $realowner_rh->{'user_id'};
    }
    $realowner_sth->finish;

    return $author_id;
}

#----------------------------------------------------------------------
# put together message
#----------------------------------------------------------------------
sub get_email {

    my ($header_path, $content, $footer_path) = @_;

    # header
    my ( $stat, $err ) = FileUtils::file2string(
        {   file             => $header_path,
            comment_flag     => 0,
            blank_lines_flag => 0
        }
    );
    if ( !$stat ) {
        email_errors($err);
        die;
    }
    my $header = $err;

    # footer
    ( $stat, $err ) = FileUtils::file2string(
        {   file             => $footer_path,
            comment_flag     => 0,
            blank_lines_flag => 0
        }
    );
    if ( !$stat ) {
        email_errors($err);
        die;
    }
    my $footer = $err;

    my $digest = $header . $content . $footer;

    return $digest;
}

#----------------------------------------------------------------------
# email out results
#----------------------------------------------------------------------
sub email_results {

    my ($from, $to, $cc, $bcc, $subject, $html) = @_;

    my %mail_config = (
        'reply_to' => $from,
        'from'     => $from,
        'to'       => $to, 
        'cc'       => $cc,
        'bcc'      => $bcc,
        'subject'  => $subject, 
        'text'     => '',
        'html'     => $html,
    );

    my ( $stat, $err ) = SendMail::multi_mail( \%mail_config );
    if ( !$stat ) {
        die "could not send out metrics report";
    }

}

#----------------------------------------------------------------------
# email out errors
#----------------------------------------------------------------------
sub email_errors {

    my $errMsg = shift;

     my $reply_to = $email_from;
     my $from     = $email_from;
     my $to       = $email_to;
     my $cc       = '';
     my $bcc      = '';
     my $subject  = "Errors - $0";
     my $text     = "$errMsg";

    my ( $stat, $err )
      = SendMail::text( $reply_to, $from, $to, $cc, $bcc, $subject, $text);
    if ( !$stat ) {
        die "Can not send email. $err\n $errMsg\n";
    }
}

#----------------------------------------------------------------------
# usage
#----------------------------------------------------------------------
sub usage {

    print << "EOP";

  USAGE:
    $0 -r -e < environment > 

  DESCRIPTION:
    this script used to send out team monthly reports to scorecard in iKbase.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/team_monthly_report_development.conf
      ../conf/team_monthly_report_test.conf
      ../conf/team_monthly_report_production.conf

  Examples:
  $0 -r -e development

EOP
}
