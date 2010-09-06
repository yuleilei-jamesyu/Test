#!/usr/bin/perl -w
use strict;
#======================================================================
# Description: manipulate and sent out the scorecard individual quarterly
# report(two versions - local coach report and other coaches report).
# Author: jameyu(jameyu@cisco.com)
# Date: 2010/08/20
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
                    '../conf/individual_quarterly_report_development.conf',
                'test'        =>
                    '../conf/individual_quarterly_report_test.conf',
                'production'  =>
                    '../conf/individual_quarterly_report_production.conf',
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
my $quarter_name = $config_vars_rh->{'quarter_name'};
die "quarter_name not defined"   unless ( defined $quarter_name );

my $month1_name  = $config_vars_rh->{'month1_name'};
my $month1_start = $config_vars_rh->{'month1_start'};
my $month1_end   = $config_vars_rh->{'month1_end'};

die "month1_name not defined"  unless ( defined $month1_name );
die "month1_start not defined" unless ( defined $month1_start );
die "month1_end not defined"   unless ( defined $month1_end );

my $month2_name  = $config_vars_rh->{'month2_name'};
my $month2_start = $config_vars_rh->{'month2_start'};
my $month2_end   = $config_vars_rh->{'month2_end'};

die "month2_name not defined"  unless ( defined $month2_name );
die "month2_start not defined" unless ( defined $month2_start );
die "month2_end not defined"   unless ( defined $month2_end );

my $month3_name  = $config_vars_rh->{'month3_name'};
my $month3_start = $config_vars_rh->{'month3_start'};
my $month3_end   = $config_vars_rh->{'month3_end'};

die "month3_name not defined"  unless ( defined $month3_name );
die "month3_start not defined" unless ( defined $month3_start );
die "month3_end not defined"   unless ( defined $month3_end );

my $months = { $month1_name => { 'start_datetime' => $month1_start,
                                 'stop_datetime' => $month1_end,
                               },
               $month2_name => { 'start_datetime' => $month2_start,
                                 'stop_datetime' => $month2_end,
                               },
               $month3_name => { 'start_datetime' => $month3_start,
                                 'stop_datetime' => $month3_end,
                               },
               'sorter' => [$month1_name, $month2_name, $month3_name],
             };
#print Dumper($months);

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
# pull out the scores from the kb.scorecard_scores table and take
# author id as the key to put data into a hash - %scorecard_report
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

foreach my $month ( @{$months->{'sorter'}} ) {
    my $month_start = $months->{$month}{'start_datetime'};
    my $month_end   = $months->{$month}{'stop_datetime'};

    $select_scores_sth->execute($month_start, $month_end);
    while ( my $select_scores_rh = $select_scores_sth->fetchrow_hashref ) {

        my $unique_id             = $select_scores_rh->{'unique_id'};
        my $article_id            = $select_scores_rh->{'article_id'};
        my $scorer_id             = $select_scores_rh->{'scorer_id'};
        my $incomplete            = $select_scores_rh->{'incomplete'};
        my $too_thin              = $select_scores_rh->{'too_thin'};
        my $duplicate             = $select_scores_rh->{'duplicate'};
        my $incorrect             = $select_scores_rh->{'incorrect'};
        my $too_wordy             = $select_scores_rh->{'too_wordy'};
        my $marketing_terminology = $select_scores_rh
->{'marketing_terminology'};
        my $generic_examples      = $select_scores_rh->{'generic_examples'};
        my $formatting            = $select_scores_rh->{'formatting'};
        my $grammar_spelling      = $select_scores_rh->{'grammar_spelling'};
        my $numbered_steps        = $select_scores_rh->{'numbered_steps'};
        my $title                 = $select_scores_rh->{'title'};
        my $context               = $select_scores_rh->{'context'};
        my $environment           = $select_scores_rh->{'environment'};
        my $symptoms              = $select_scores_rh->{'symptoms'};
        my $question              = $select_scores_rh->{'question'};
        my $solution              = $select_scores_rh->{'solution'};
        my $mixed_fields          = $select_scores_rh->{'mixed_fields'};
        my $score_value           = $select_scores_rh->{'score_value'};
        my $comments              = $select_scores_rh->{'comments'};
        my $created_ts            = $select_scores_rh->{'created_ts'};

        my %score = ();
        $score{'article_id'}            = $article_id;
        $score{'Incomplete'}            = $incomplete;
        $score{'Too Thin'}              = $too_thin;
        $score{'Duplicate'}             = $duplicate;
        $score{'Incorrect'}             = $incorrect;
        $score{'Too Wordy'}             = $too_wordy;
        $score{'Marketing Terminology'} = $marketing_terminology;
        $score{'Generic Examples'}      = $generic_examples;
        $score{'Formatting'}            = $formatting;
        $score{'Grammar or Spelling'}   = $grammar_spelling;
        $score{'Numbered Steps'}        = $numbered_steps;
        $score{'Title'}                 = $title;
        $score{'Context'}               = $context;
        $score{'Environment'}           = $environment;
        $score{'Symptoms'}              = $symptoms;
        $score{'Question'}              = $question;
        $score{'Solution'}              = $solution;
        $score{'Mixed Fields'}          = $mixed_fields;
        $score{'+Total Average Score (SQL)_'} = $score_value;
        $score{'created'}               = $created_ts;

        # firstly, we query an articles's author from the articles table,
        # then query the real author from the realowner_article table.
        # if records exist in the latter, we overwrite the former

        # set 'chaag' as the default author
        my $author_id = 1;

        my $article_owner_sql = "SELECT owner FROM articles WHERE id=?";
        my $article_owner_sth = $kb_dbh->prepare($article_owner_sql)
            or die $kb_dbh->errstr;

        $article_owner_sth->execute($article_id);
        my $article_owner_rh = $article_owner_sth->fetchrow_hashref;
        if ($article_owner_rh) {
            $author_id = $article_owner_rh->{'owner'};
        }
        $article_owner_sth->finish;

        my $realowner_sql
        = "SELECT user_id FROM realowner_article WHERE article_id=?";
        my $realowner_sth = $kb_dbh->prepare($realowner_sql)
            or die $kb_dbh->errstr;

        $realowner_sth->execute($article_id);
        my $realowner_rh = $realowner_sth->fetchrow_hashref;
        if ($realowner_rh) {
            $author_id = $realowner_rh->{'user_id'};
        }
        $realowner_sth->finish;

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
             AND (role = 'lead' OR role = 'cse' OR role = 'sonata'
                  OR role = 'e4e')
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
            if ($author_email_rh->{'manager'}) {
                $mgr_email = $author_email_rh->{'manager'};
            }
            if ($author_email_rh->{'coach_y'}) {
                $coach_y   = $author_email_rh->{'coach_y'};
            }
        }
        else {
            next;
        }
        $scorecard_report{$author_id}{'mail'} = $author_email;
        $scorecard_report{$author_id}{'mgr_mail'} = $mgr_email;

        # if the author is a coach, then his/her coach is himself/herself.
        if ($coach_y && ($coach_id eq "") ) {
            $coach_id = $author_id;
        }

        # query scorer's email addr based on the scorer id
        my $scorer_email = "";
        my $scorer_email_sql
        = "SELECT primary_email FROM scorecard_users WHERE user_id=?
           AND _del='0'";
        my $scorer_email_sth = $kb_dbh->prepare($scorer_email_sql)
            or die $kb_dbh->errstr;

        $scorer_email_sth->execute($scorer_id);
        my $scorer_email_rh = $scorer_email_sth->fetchrow_hashref;
        if ($scorer_email_rh) {
            if ($scorer_email_rh->{'primary_email'}) {
                $scorer_email = $scorer_email_rh->{'primary_email'};
            }
        }

        # query local coach's email addr based on coach id
        my $coach_email = "cics-kcscouncil\@cisco.com";
        my $coach_email_sql
        = "SELECT primary_email FROM scorecard_users WHERE user_id=?
           AND _del='0'";
        my $coach_email_sth = $kb_dbh->prepare($coach_email_sql)
            or die $kb_dbh->errstr;

        $coach_email_sth->execute($coach_id);
        my $coach_email_rh = $coach_email_sth->fetchrow_hashref;
        if ($coach_email_rh) {
            if ($coach_email_rh->{'primary_email'}) {
                $coach_email = $coach_email_rh->{'primary_email'};
            }
        }
        if (!grep { $_ eq $coach_email }
            @{$scorecard_report{$author_id}{'local coach mail'}} ) {

            push @{$scorecard_report{$author_id}{'local coach mail'}},
            $coach_email;
        }

        # classify a score to 'local coach' or 'other coach' scores
        my $coach = "";
        if ($scorer_id eq $coach_id) {
            # if the author is a coach, then he/she will not get
            # the 'local coach report'.
            next if $coach_y;

            $coach = "local coach";
            $scorecard_report{$author_id}{$coach}{'coaches'} = "0";

            $coach_email =~ s/@.+//g;
            $scorecard_report{$author_id}{$coach}{'category'}
            = "Scoring by $coach_email";
        }
        else {
            $coach = "other coach";
            $scorecard_report{$author_id}{$coach}{'coaches'} = "1";

            # figure out other coaches' names
            $scorer_email =~ s/@.+//g;
            $scorecard_report{$author_id}{$coach}{'category'}
            = "Scoring by Others";

            if (!grep {$_ eq $scorer_email} @{$scorecard_report{$author_id}
{$coach}{'scores'}{$month}{$article_id}{'coaches'}}) {
                push @{$scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'coaches'}}, $scorer_email;
            }
        }
        $scorecard_report{$author_id}{$coach}{'scores'}{$month}{$article_id}
{$unique_id} = \%score;
        push @{$scorecard_report{$author_id}{$coach}{'scores'}{$month}
{$article_id}{'sorter'}}, $unique_id;

        # sorting months
        if( !grep { $_ eq $month }
            @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}}) {

            push @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}},
            $month;
        }

        # sorting article ids
        if ( !grep { $_ eq $article_id } @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{'sorter'}} ) {
            push @{$scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'sorter'}}, $article_id;
        }
    }

}
$select_scores_sth->finish;

#----------------------------------------------------------------------
# re-calculating the values in a hash - %scorecard_report
#----------------------------------------------------------------------
my @article_unusable     = ('Incomplete', 'Too Thin', 'Duplicate',
                            'Incorrect', 'Article Unusable Total'
                           );
my @content_needs_rework = ('Too Wordy', 'Marketing Terminology',
                            'Generic Examples', 'Formatting',
                            'Grammar or Spelling', 'Numbered Steps',
                            'Content Needs Rework Total'
                           );
my @field_needs_rework   = ('Title', 'Context', 'Environment',
                            'Symptoms', 'Question', 'Solution',
                            'Mixed Fields', 'Field Needs Rework Total'
                           );

foreach my $author_id ( keys %scorecard_report ) {
    # local/other coach
    foreach my $coach ( keys %{$scorecard_report{$author_id}} ) {
        next if $coach eq "local coach mail";
        next if $coach eq "mgr_mail";
        next if $coach eq "mail";

        my %month_metrics = (
            'new articles' => 0,
            'edited articles' => 0,
            'total updated articles' => 0,
            'new article scored' => 0,
            'edited article scored' => 0,
            'total times scored' => 0,
            'unique articles scored' => 0,
            'Starting Average Percentage' => 0,
            'Incomplete' => 0,
            'Too Thin' => 0,
            'Duplicate' => 0,
            'Incorrect' => 0,
            'Article Unusable Total' => 0,
            'Too Wordy' => 0,
            'Marketing Terminology' => 0,
            'Generic Examples' => 0,
            'Formatting' => 0,
            'Grammar or Spelling' => 0,
            'Numbered Steps' => 0,
            'Content Needs Rework Total' => 0,
            'Title' => 0,
            'Context' => 0,
            'Environment' => 0,
            'Symptoms' => 0,
            'Question' => 0,
            'Solution' => 0,
            'Mixed Fields' => 0,
            'Field Needs Rework Total' => 0,
            '+Total Average Score (SQL)_' => 0,
        );
        foreach my $month ( @{$scorecard_report{$author_id}{$coach}
{'scores'}{'sorter'}} ) {
                my %article_metrics = (
                    'new article scored' => 0,
                    'edited article scored' => 0,
                    'total times scored' => 0,
                    'Incomplete' => 0,
                    'Too Thin' => 0,
                    'Duplicate' => 0,
                    'Incorrect' => 0,
                    'Article Unusable Total' => 0,
                    'Too Wordy' => 0,
                    'Marketing Terminology' => 0,
                    'Generic Examples' => 0,
                    'Formatting' => 0,
                    'Grammar or Spelling' => 0,
                    'Numbered Steps' => 0,
                    'Content Needs Rework Total' => 0,
                    'Title' => 0,
                    'Context' => 0,
                    'Environment' => 0,
                    'Symptoms' => 0,
                    'Question' => 0,
                    'Solution' => 0,
                    'Mixed Fields' => 0,
                    'Field Needs Rework Total' => 0,
                    '+Total Average Score (SQL)_' => 0,
                );

            my $new_articles           = 0;
            my $edited_articles        = 0;
            my $total_updated_articles = 0;

            my $unique_articles_scored = 0;
            foreach my $article_id ( @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{'sorter'}} ) {
                $unique_articles_scored ++;

                # pull out an article's created datetime from the history table
                my $article_created_ts_sql
                = "SELECT rowmtime FROM history WHERE article_id=? AND
                   status='6'";
                my $article_created_ts_sth
                = $kb_dbh->prepare($article_created_ts_sql)
                  or die $kb_dbh->errstr;

                $article_created_ts_sth->execute($article_id);
                my $article_created_ts_rh
                = $article_created_ts_sth->fetchrow_hashref;
                my $article_created_ts = $article_created_ts_rh->{'rowmtime'};

                # pull out an article's first edited datetime from the history
                # table.
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

                my %score_metrics = (
                    'Incomplete' => 0,
                    'Too Thin' => 0,
                    'Duplicate' => 0,
                    'Incorrect' => 0,
                    'Article Unusable Total' => 0,
                    'Too Wordy' => 0,
                    'Marketing Terminology' => 0,
                    'Generic Examples' => 0,
                    'Formatting' => 0,
                    'Grammar or Spelling' => 0,
                    'Numbered Steps' => 0,
                    'Content Needs Rework Total' => 0,
                    'Title' => 0,
                    'Context' => 0,
                    'Environment' => 0,
                    'Symptoms' => 0,
                    'Question' => 0,
                    'Solution' => 0,
                    'Mixed Fields' => 0,
                    'Field Needs Rework Total' => 0,
                    '+Total Average Score (SQL)_' => 0,
                );

                # set an article neither new article nor edited article
                # as default
                $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'new articles'} = 0;
                $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'edited articles'} = 0;

                my $score_count = scalar @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{$article_id}{'sorter'}};
                my $score_num = 0;

                foreach my $unique_id ( @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{$article_id}{'sorter'}} ) {

                    $score_num ++;
                    # created datetime for a score
                    my $created_ts = $scorecard_report{$author_id}{$coach}
{'scores'}{$month}{$article_id}{$unique_id}{'created'};

                    # 1 - if an article haven't been edited before, then this
                    # score belongs to new article scored.
                    # 2 - if an article have been edited ever:
                    # a, this score's created datetime is former than article's
                    # first edited datetime, then the score belongs to new
                    # article scored.
                    # b, on the contrary, this score belongs to edited article
                    # scored.
                    my $created_flag = 0;
                    my $edited_flag  = 0;
                    if ( defined $article_edited_ts ) {
                        $created_flag
                        = Date_Cmp($article_created_ts, $created_ts);
                        $edited_flag
                        = Date_Cmp($created_ts, $article_edited_ts);
                        if ($created_flag < 0 && $edited_flag < 0) {
                            $new_article_scored ++;
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'new articles'} = "1";
                        }
                        elsif ($edited_flag > 0) {
                            $edited_article_scored ++;
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'edited articles'} = "1";
                        }
                    }
                    else {
                        $created_flag
                        = Date_Cmp($article_created_ts, $created_ts);
                        if ($created_flag < 0) {
                            $new_article_scored ++;
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'new articles'} = "1";
                        }
                    }

                    if ($score_num == 1) {

                        # Article Unusable Total
                        my $article_unusable_total = 0;
                        foreach my $metric ( @article_unusable ) {
                            next if $metric eq "Article Unusable Total";

                            $article_unusable_total
                            += $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id}{$metric};
                        }
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id}{'Article Unusable Total'}
                        = $article_unusable_total;

                        # Content Needs Rework Total
                        my $content_needs_rework_total = 0;
                        foreach my $metric ( @content_needs_rework ) {
                            next if $metric eq "Content Needs Rework Total";

                            $content_needs_rework_total
                            += $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id}{$metric};
                        }
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id}{'Content Needs Rework Total'}
                        = $content_needs_rework_total;

                        # Field Needs Rework Total
                        my $field_needs_rework_total = 0;
                        foreach my $metric ( @field_needs_rework ) {
                            next if $metric eq "Field Needs Rework Total";

                            $field_needs_rework_total
                            += $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id}{$metric};
                        }
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id}{'Field Needs Rework Total'}
                        = $field_needs_rework_total;

                        # calculating the amount for each metric_value for
                        # an article
                        foreach my $metric ( keys %{$scorecard_report
{$author_id}{$coach}{'scores'}{$month}{$article_id}{$unique_id}} ) {
                            if ( exists $score_metrics{$metric} ) {
                                $score_metrics{$metric} += $scorecard_report
{$author_id}{$coach}{'scores'}{$month}{$article_id}{$unique_id}{$metric};
                            }
                        }
                    }
                    delete $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$unique_id};
                }
                delete $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'sorter'};

                $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{$article_id}{'new article scored'}
                = $new_article_scored;
                $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{$article_id}{'edited article scored'}
                = $edited_article_scored;

                $total_times_scored
                = $new_article_scored + $edited_article_scored;
                $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{$article_id}{'total times scored'}
                = $total_times_scored;

                foreach my $metric ( keys %score_metrics ) {
                    if ( $metric eq "+Total Average Score (SQL)_" ) {
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$metric}
                        = $score_metrics{$metric}/$score_count;
                    }
                    else {
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$metric}
                        = $score_metrics{$metric};
                    }
                }

                foreach my $metric ( keys %{$scorecard_report{$author_id}
{$coach}{'scores'}{$month}{$article_id}} ) {
                    if ( $metric eq "coaches" ) {
                        foreach my $coach_name ( @{$scorecard_report{$author_id}
{$coach}{'scores'}{$month}{$article_id}{'coaches'}} ) {
                            if ( !grep { $_ eq $coach_name }
                                @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{'coaches'}} ) {

                                push @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{'coaches'}}, $coach_name;
                            }
                        }
                    }
                    else {
                        if ( exists $article_metrics{$metric} ) {
                            $article_metrics{$metric}
                            += $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{$metric};
                        }
                    }
                }

                if ( $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'new articles'} eq "1" ) {
                    $new_articles ++;
                }
                elsif ( $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id}{'edited articles'} eq "1" ) {
                    $edited_articles ++;
                }

                delete $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$article_id};
            }

            my $article_count = scalar @{$scorecard_report{$author_id}{$coach}
{'scores'}{$month}{'sorter'}};
            delete $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'sorter'};

            foreach my $metric ( keys %article_metrics ) {
                if ( $metric eq "new article scored"
                     || $metric eq "edited article scored"
                     || $metric eq "total times scored" ) {
                    $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                    = $article_metrics{$metric};
                }
                else {
                    $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                    = $article_metrics{$metric}/$article_count;
                }
            }

            # new/edited/total updated articles for per month
            $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'new articles'}
            = $new_articles;
            $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'edited articles'}
            = $edited_articles;

            $total_updated_articles = $new_articles + $edited_articles;
            $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'total updated articles'}
            = $total_updated_articles;

            # unique articles scored for per month
            $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'unique articles scored'}
            = $unique_articles_scored;

            $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{'Starting Average Percentage'}
            = 1;

            foreach my $metric ( keys %month_metrics ) {
                $month_metrics{$metric} += $scorecard_report{$author_id}
{$coach}{'scores'}{$month}{$metric};
            }
        }

        my $month_nums = scalar @{$scorecard_report{$author_id}{$coach}
{'scores'}{'sorter'}};
        foreach my $metric ( keys %month_metrics ) {
            my $total = $month_metrics{$metric};
            $scorecard_report{$author_id}{$coach}{'scores'}{'Total'}{$metric}
             = $total;

            my $average = $month_metrics{$metric}/$month_nums;
            $scorecard_report{$author_id}{$coach}{'scores'}{'Average'}{$metric}
            = $average;
        }
        push @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}},
        "Total";
        push @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}},
        "Average";

        foreach my $month
            ( @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}} ) {

            foreach my $metric ( keys %month_metrics ) {
                my $metric_value = $scorecard_report{$author_id}{$coach}
{'scores'}{$month}{$metric};

                if ( $metric_value == 0 ) {
                    $scorecard_report{$author_id}{$coach}{'scores'}{$month}
{$metric}
                    = "";
                }
                else {
                    if ( $metric eq "+Total Average Score (SQL)_" ) {
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                        = sprintf("%.2f", $metric_value) . "%";
                    }
                    elsif ( $metric eq "Starting Average Percentage" ) {
                        $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                        = $metric_value*100 . "%";
                    }
                    else {
                        if ( grep { $_ eq $metric } @article_unusable ) {
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                            = "-" . sprintf("%.2f", $metric_value*50) . "%";
                        }
                        elsif ( grep { $_ eq $metric } @content_needs_rework ) {
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                            = "-" . sprintf("%.2f", $metric_value*3.85) . "%";
                        }
                        elsif ( grep { $_ eq $metric } @field_needs_rework ) {
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                            = "-" . sprintf("%.2f", $metric_value*3.85) . "%";
                        }
                        else {
                            $scorecard_report{$author_id}{$coach}{'scores'}
{$month}{$metric}
                            = sprintf("%.2f", $metric_value);
                        }

                    }
                }
            }
        }

        # use array to sort the calculating metrics
        @{$scorecard_report{$author_id}{$coach}{'scores'}{'metrics'}}
        = ( 'new articles',
            'edited articles',
            'total updated articles',
            'new article scored',
            'edited article scored',
            'total times scored',
            'unique articles scored',
            'Starting Average Percentage',
            'Incomplete',
            'Too Thin',
            'Duplicate',
            'Incorrect',
            'Article Unusable Total',
            'Too Wordy',
            'Marketing Terminology',
            'Generic Examples',
            'Formatting',
            'Grammar or Spelling',
            'Numbered Steps',
            'Content Needs Rework Total',
            'Title',
            'Context',
            'Environment',
            'Symptoms',
            'Question',
            'Solution',
            'Mixed Fields',
            'Field Needs Rework Total',
            '+Total Average Score (SQL)_'
        );
    }
}
#print Dumper(\%scorecard_report);

#----------------------------------------------------------------------
# output the report email
#----------------------------------------------------------------------
my $tt = Template->new(
    {   INCLUDE_PATH => $tmpl_dir, 
        EVAL_PERL    => 1,
    }
) || die $Template::ERROR, "\n";

foreach my $author_id ( keys %scorecard_report ) {
    my $author_email = $scorecard_report{$author_id}{'mail'};
    my $coach_email  = $scorecard_report{$author_id}{'local coach mail'}[0];
    my $mgr_email    = $scorecard_report{$author_id}{'mgr_mail'};

    my $author_name  = $scorecard_report{$author_id}{'mail'};
    $author_name =~ s/@.+//g;

    foreach my $coach ( keys %{$scorecard_report{$author_id}} ) {
        next if $coach eq "mail";
        next if $coach eq "mgr_mail";
        next if $coach eq "local coach mail";

        # generate the body of report email
        my $output;
        my %input_vars;
        %{$input_vars{'items'}} = %{$scorecard_report{$author_id}{$coach}};
        #print Dumper(\%{$input_vars{'items'}});

        $tt->process($report_tmpl, \%input_vars, \$output);
        #print "$output";
        my $digest = get_email($header_tmpl, $output, $footer_tmpl);

        if ($env =~ /development|test/i) {
            $digest .= "<p>author: $author_email
                        <br>coach: $coach_email
                        <br>manager: $mgr_email</p>";
        }

        my $from = $coach_email;
        my $to;
        if ($env =~ /development|test/i) {
            $to = $email_to;
        }
        elsif ($env =~ /production/i) {
            $to = $author_email;
        }

        my $email_cc = $coach_email ."," . $mgr_email;
        my $cc;
        if ($env =~ /development|test/i) {
            $cc = "";
        }
        elsif ($env =~ /production/i) {
            $cc = $email_cc;
        }

        my $bcc;
        if ($env =~ /development|test/i) {
            $bcc = "";
        }
        elsif ($env =~ /production/i) {
            $bcc = $email_to;
        }

        my $report_tag = "";
        if ( $coach eq "local coach" ) {
            $report_tag = "Local Coach Report";
        }
        elsif ( $coach eq "other coach" ) {
            $report_tag = "Other Coach Report";
        }
        else {
        }

        my $subject
        = "$report_tag - KCS Monthly Scorecard Summary for $author_name "
          . "for $quarter_name";

        # send out the report email
        if ( $author_email ne $coach_email ) {
            email_results($from, $to, $cc, $bcc, $subject, $digest);
        }
    }

    #last;
}

#----------------------------------------------------------------------
# Subroutines...
#----------------------------------------------------------------------
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
    this script used to send out individual quarterly reports to scorecard in iKbase.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/individual_quarterly_report_development.conf
      ../conf/individual_quarterly_report_test.conf
      ../conf/individual_quarterly_report_production.conf

  Examples:
  $0 -r -e development

EOP
}
