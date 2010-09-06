#!/usr/bin/perl -w
use strict;
#======================================================================
# Description: manipulate and sent out the scorecard individual weekly
# report(two versions - local coach report and other coaches report).
# Author: jameyu(jameyu@cisco.com)
# Date: 2010/07/29
#======================================================================
use Getopt::Std;
use Date::Manip;
use Template;
use Data::Dumper;

use lib '../../../../utils/lib';
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
                    '../conf/individual_weekly_report_development.conf',
                'test'        =>
                    '../conf/individual_weekly_report_test.conf',
                'production'  =>
                    '../conf/individual_weekly_report_production.conf',
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

#my $start_datetime = previous_sunday_datetime(time, 1);
#my $stop_datetime  = last_sunday_datetime(time);
my $start_datetime = "2010-07-26 00:00:00";
my $stop_datetime  = "2010-08-21 00:00:00";

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

$select_scores_sth->execute($start_datetime, $stop_datetime);
while ( my $select_scores_rh = $select_scores_sth->fetchrow_hashref ) {

    my $unique_id             = $select_scores_rh->{'unique_id'};
    my $article_id            = $select_scores_rh->{'article_id'};
    my $scorer_id             = $select_scores_rh->{'scorer_id'};
    my $incomplete            = $select_scores_rh->{'incomplete'};
    my $too_thin              = $select_scores_rh->{'too_thin'};
    my $duplicate             = $select_scores_rh->{'duplicate'};
    my $incorrect             = $select_scores_rh->{'incorrect'};
    my $too_wordy             = $select_scores_rh->{'too_wordy'};
    my $marketing_terminology = $select_scores_rh->{'marketing_terminology'};
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
    my $comments              =$select_scores_rh->{'comments'};
    my $created_ts            = $select_scores_rh->{'created_ts'};

    my $article_title = "";

    my $article_title_sql = "SELECT title FROM content WHERE article=?";
    my $article_title_sth = $kb_dbh->prepare($article_title_sql)
        or die $kb_dbh->errstr;

    $article_title_sth->execute($article_id);
    my $article_title_rh = $article_title_sth->fetchrow_hashref;
    if ($article_title_rh) {
        $article_title = $article_title_rh->{'title'};
    }

    my %score = ();
    $score{'article_id'}            = $article_id;
    $score{'article_title'}         = $article_title;
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
    $score{'Total Score'}           = $score_value;
    $score{'created'}               = $created_ts;

    # add a comment's content
    $score{'comments'}{$created_ts}{'content'} = $comments;

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
    my $realowner_sth = $kb_dbh->prepare($realowner_sql) or die $kb_dbh->errstr;

    $realowner_sth->execute($article_id);
    my $realowner_rh = $realowner_sth->fetchrow_hashref;
    if ($realowner_rh) {
        $author_id = $realowner_rh->{'user_id'};
    }
    $realowner_sth->finish;

    # query the email addr for the author, manager and coach id from
    # the scorecard_users table based on the author id
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
            $coach_id = $author_email_rh->{'coach'};
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

    # query scorer's email addr based on scorer id
    my $scorer_email = "";
    my $scorer_email_sql
    = "SELECT primary_email FROM scorecard_users WHERE user_id=? AND _del='0'";
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
    = "SELECT primary_email FROM scorecard_users WHERE user_id=? AND _del='0'";
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
    my $coach       = "";
    if ($scorer_id eq $coach_id) {
        # if the author is a coach, then he/she will not get
        # the 'local coach report'.
        next if $coach_y;

        $coach = "local coach";
        $scorecard_report{$author_id}{$coach}{'coaches'} = "0";
    }
    else {
        $coach = "other coach";

        # figure out other coaches' names
        $scorer_email =~ s/@.+//g;

        $scorecard_report{$author_id}{$coach}{'coaches'} = "1";
        if (!grep {$_ eq $scorer_email} @{$scorecard_report{$author_id}
{$coach}{'scores'}{$article_id}{'coaches'}}) {
            push @{$scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{'coaches'}}, $scorer_email;
        }

        # add a comment's coach name
        $score{'comments'}{$created_ts}{'scorer'} = $scorer_email;
    }
    $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}{$unique_id}
    = \%score;
    push @{$scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{'sorter'}}, $unique_id;

    # put article ids into array
    if (!grep { $_ eq $article_id }
        @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}}) {
        push @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}},
         $article_id;
    }

}
$select_scores_sth->finish;

#----------------------------------------------------------------------
# calculating the values of some metrics and add them into a hash
# - %scorecard_report
#----------------------------------------------------------------------

# classify metrics into arrays, in order to further calculating
my @article_unusable_items     = ('Incomplete', 'Too Thin','Duplicate',
                                  'Incorrect', 'Article Unusable Total');
my @content_needs_rework_items = ('Too Wordy', 'Marketing Terminology',
                                  'Generic Examples', 'Formatting',
                                  'Grammar or Spelling', 'Numbered Steps',
                                  'Content Needs Rework Total');
my @field_needs_rework_items   = ('Title', 'Context', 'Environment', 'Symptoms',
                                  'Question', 'Solution', 'Mixed Fields',
                                  'Field Needs Rework Total');

foreach my $author_id ( keys %scorecard_report ) {

    foreach my $coach ( keys %{$scorecard_report{$author_id}} ) {
        next if $coach eq "mail";
        next if $coach eq "mgr_mail";
        next if $coach eq "local coach mail";

        # 'new article scored', 'edited article scored',
        # 'total number of times scored' calculating
        my $new_article_scored_count    = 0;
        my $edited_article_scored_count = 0;
        my $total_times_scored_count    = 0;

        my $new_article_scored_avg      = "N/A";
        my $edited_article_scored_avg   = "N/A";
        my $total_times_scored_avg      = "N/A";

        foreach my $article_id
            ( keys %{$scorecard_report{$author_id}{$coach}{'scores'}} ) {
            next if $article_id eq "sorter";

            # pull out an article's created datetime from the history table
            my $article_created_ts_sql
            = "SELECT rowmtime FROM history WHERE article_id=? AND status='6'";
            my $article_created_ts_sth
            = $kb_dbh->prepare($article_created_ts_sql) or die $kb_dbh->errstr;

            $article_created_ts_sth->execute($article_id);
            my $article_created_ts_rh
            = $article_created_ts_sth->fetchrow_hashref;
            my $article_created_ts = $article_created_ts_rh->{'rowmtime'};

            # pull out an article's first edited datetime from the history
            # table.
            my $article_edited_ts_sql
            = "SELECT rowmtime FROM history WHERE article_id=? AND status='5'
              ORDER BY rowmtime asc LIMIT 0, 1";
            my $article_edited_ts_sth
            = $kb_dbh->prepare($article_edited_ts_sql) or die $kb_dbh->errstr;

            $article_edited_ts_sth->execute($article_id);
            my $article_edited_ts_rh = $article_edited_ts_sth->fetchrow_hashref;
            my $article_edited_ts = $article_edited_ts_rh->{'rowmtime'};

            # figure out new article scored, edited article scored,
            # total number of times scored for per article
            my $new_article_scored    = 0;
            my $edited_article_scored = 0;
            my $total_times_scored    = 0;
            foreach my $unique_id( keys %{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}} ) {

                next if $unique_id eq "sorter";
                next if $unique_id eq "coaches";
                my $created_ts
                = $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$unique_id}{'created'};
                # 1 - if an article haven't been edited before, then this score
                # belongs to new article scored.
                # 2 - if an article have been edited ever:
                # a, this score's created datetime is former than article's
                # first edited datetime, then the score belongs to new article
                # scored.
                # b, on the contrary, this score belongs to edited article
                # scored.
                my $created_flag = 0;
                my $edited_flag  = 0;
                if ( defined $article_edited_ts ) {
                    $created_flag = Date_Cmp($article_created_ts, $created_ts);
                    $edited_flag  = Date_Cmp($created_ts, $article_edited_ts);
                    if ($created_flag < 0 && $edited_flag < 0) {
                        $new_article_scored ++;
                    }
                    elsif ($edited_flag > 0) {
                        $edited_article_scored ++;
                    }
                }
                else {
                    $created_flag = Date_Cmp($article_created_ts, $created_ts);
                    if ($created_flag < 0) {
                        $new_article_scored ++;
                    }
                }
            }
            $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{'new article scored'}
            = $new_article_scored;
            $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{'edited article scored'}
            = $edited_article_scored;

            $total_times_scored = $new_article_scored + $edited_article_scored;
            $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{'total number of times scored'}
            = $total_times_scored;

            $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{'Starting Percentage'}
            = 1;

            $new_article_scored_count    += $new_article_scored;
            $edited_article_scored_count += $edited_article_scored;
            $total_times_scored_count    += $total_times_scored;
        }
        my $article_count
        = scalar @{$scorecard_report{$author_id}{$coach}{'scores'}{'sorter'}};

        # total and average of starting percentage
        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'Starting Percentage'}
        = $article_count;
        $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Starting Percentage'}
        = 1;

        # total and average of new article scored, edited article scored,
        # total numer of times scored
        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'new article scored'}
        = $new_article_scored_count;
        $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'new article scored'}
        = $new_article_scored_avg;

        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'edited article scored'}
        = $edited_article_scored_count;
        $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'edited article scored'}
        = $edited_article_scored_avg;

        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'total number of times scored'}
        = $total_times_scored_count;
        $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'total number of times scored'}
        = $total_times_scored_avg;

        # leave the latest score only for per article
        foreach my $article_id
            ( keys %{$scorecard_report{$author_id}{$coach}{'scores'}} ) {

            next if $article_id eq "sorter";
            next if $article_id eq "total";
            next if $article_id eq "average";

            # count of scores for an article
            my $scores_count
            = scalar @{$scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{'sorter'}};

            # unique id of the latest score for an article
            my $last_unique_id = $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{'sorter'}[$scores_count - 1];

            # append comments of the other scores to the last score's comments
            # field
            #if ($scores_count > 1) {
                my $i = 0;
                foreach my $unique_id ( @{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{'sorter'}}) {

                    my $created_ts
                    = $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$unique_id}{'created'};

                    push @{$scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$last_unique_id}{'comments'}{'sorter'}}, $created_ts;
                    last if $i== ($scores_count - 1);

                    $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$last_unique_id}{'comments'}{$created_ts}{'content'}
                    = $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$unique_id}{'comments'}{$created_ts}{'content'};

                    $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$last_unique_id}{'comments'}{$created_ts}{'scorer'}
                    = $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$unique_id}{'comments'}{$created_ts}{'scorer'};

                    # remove a score
                    delete $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$unique_id};

                    $i ++;
                }
            #}
        }

        my %items_count = (
            'Incomplete' => 0,
            'Too Thin' => 0,
            'Duplicate' => 0,
            'Incorrect' => 0,
            'Too Wordy' => 0,
            'Marketing Terminology' => 0,
            'Generic Examples' => 0,
            'Formatting' => 0,
            'Grammar or Spelling' => 0,
            'Numbered Steps' => 0,
            'Title' => 0,
            'Context' => 0,
            'Environment' => 0,
            'Symptoms' => 0,
            'Question' => 0,
            'Solution' => 0,
            'Mixed Fields' => 0,
            'Total Score' => 0,
        );

        my $article_unusable_sum     = 0;
        my $content_needs_rework_sum = 0;
        my $field_needs_rework_sum   = 0;
        foreach my $article_id
            ( keys %{$scorecard_report{$author_id}{$coach}{'scores'}} ) {

            next if $article_id eq "sorter";
            next if $article_id eq "total";
            next if $article_id eq "average";
            foreach my $metric ( keys %{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}} ) {

                next if $metric eq "Starting Percentage";
                next if $metric eq "new article scored";
                next if $metric eq "edited article scored";
                next if $metric eq "total number of times scored";
                next if $metric eq "coaches";

                next if $metric eq "sorter";

                delete $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$metric}{'article_id'};
                delete $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{$metric}{'created'};

                my $article_unusable_count     = 0;
                my $content_needs_rework_count = 0;
                my $field_needs_rework_count   = 0;
                foreach my $item ( keys %{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{$metric}} ) {

                    my $item_value = $scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{$metric}{$item};

                    # calculating count of per item in order to calculate
                    # total and average for those items
                    foreach my $item_count ( keys %items_count ) {
                        if ($item eq $item_count) {
                            if ($item_count eq "Total Score") {
                                $items_count{$item_count} += $item_value;
                            }
                            else {
                                if ($item_value == 1) {
                                    $items_count{$item_count} ++;
                                }
                            }
                        }
                    }

                    # calculating article unusable total,
                    # content needs rework total,
                    # field_needs_rework_total for per article
                    if (grep { $_ eq $item } @article_unusable_items) {
                        if ($item_value == 1) {
                            $article_unusable_count ++;
                        }
                    }
                    if (grep { $_ eq $item } @content_needs_rework_items) {
                        if ($item_value == 1) {
                            $content_needs_rework_count ++;
                        }
                    }
                    if (grep { $_ eq $item } @field_needs_rework_items) {
                        if ($item_value == 1) {
                            $field_needs_rework_count ++;
                        }
                    }
                }

                # assign Article Unusable Total, Content Needs Rework Total,
                # Field Needs Rework Total for an article
                $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$metric}{'Article Unusable Total'}
                = $article_unusable_count;
                $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$metric}{'Content Needs Rework Total'}
                = $content_needs_rework_count;
                $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$metric}{'Field Needs Rework Total'}
                = $field_needs_rework_count;

                $article_unusable_sum += $article_unusable_count;
                $content_needs_rework_sum += $content_needs_rework_count;
                $field_needs_rework_sum += $field_needs_rework_count;
            }

            # re-organize the hash - %scorecard_report
            my $unique_counts = scalar @{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{'sorter'}};
            my $last_unique_id = $scorecard_report{$author_id}{$coach}{'scores'}
{$article_id}{'sorter'}[$unique_counts - 1];

            foreach my $item ( keys %{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{$last_unique_id}} ) {
                $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$item}
                = $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$last_unique_id}{$item};
            }
            delete $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$last_unique_id};
            delete $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{'sorter'};

        }

        # total and average of
        # Article Unusable Total, Content Needs Rework Total,
        # Field Needs Rework Total
        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'Article Unusable Total'}
        = $article_unusable_sum;

        if ($article_unusable_sum != 0) {
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Article Unusable Total'}
            = sprintf "%.2f", $article_unusable_sum/$article_count;
        }
        else {
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Article Unusable Total'}
            = 0;
        }

        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'Content Needs Rework Total'}
        = $content_needs_rework_sum;

        if ($content_needs_rework_sum != 0) {
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Content Needs Rework Total'}
            = sprintf "%.2f", $content_needs_rework_sum/$article_count;
        }
        else {
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Content Needs Rework Total'}
            = 0;
        }

        $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{'Field Needs Rework Total'}
        = $field_needs_rework_sum;

        if ($field_needs_rework_sum != 0) {
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Field Needs Rework Total'}
            = sprintf "%.2f", $field_needs_rework_sum/$article_count;
        }
        else {
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{'Field Needs Rework Total'}
            = 0;
        }

        # calculating total and average to per item
        foreach my $item_count ( keys %items_count ) {
            if ($items_count{$item_count} != 0) {
                $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{$item_count}
                = sprintf "%.2f", $items_count{$item_count};
            }
            else {
                $scorecard_report{$author_id}{$coach}{'scores'}{'total'}
{$item_count}
                = 0;
            }

            my $average_value = $items_count{$item_count}/$article_count;
            if ($average_value != 0) {
                $average_value = sprintf "%.2f", $average_value;
            }
            $scorecard_report{$author_id}{$coach}{'scores'}{'average'}
{$item_count}
            = $average_value;
        }

        # format values of the metrics
        foreach my $article_id
            ( keys %{$scorecard_report{$author_id}{$coach}{'scores'}} ) {
            next if $article_id eq "sorter";

            foreach my $metric ( keys %{$scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}} ) {

                if ($scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$metric} eq '0') {
                    $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$metric}
                    = '';
                }
                else {
                    next if $metric eq "new article scored";
                    next if $metric eq "edited article scored";
                    next if $metric eq "total number of times scored";
                    next if $metric eq "comments";
                    next if $metric eq "article_title";
                    next if $metric eq "coaches";

                    my $metric_value = "";
                    if ($metric eq "Starting Percentage") {
                        $metric_value = $scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{$metric}*100 . "%";
                    }
                    elsif ($metric eq "Total Score") {
                        $metric_value = $scorecard_report{$author_id}{$coach}
{'scores'}{$article_id}{$metric} . "%";
                    }

                    if (grep { $_ eq $metric } @article_unusable_items) {
                        $metric_value
                        = "-" . sprintf("%.2f", $scorecard_report{$author_id}
{$coach}{'scores'}{$article_id}{$metric}*50) . "%";
                    }
                    if (grep { $_ eq $metric } @content_needs_rework_items) {
                        $metric_value
                        = "-" . sprintf("%.2f", $scorecard_report{$author_id}
{$coach}{'scores'}{$article_id}{$metric}*3.85) . "%";
                    }
                    if (grep { $_ eq $metric } @field_needs_rework_items) {
                        $metric_value
                         = "-" . sprintf("%.2f", $scorecard_report{$author_id}
{$coach}{'scores'}{$article_id}{$metric}*3.85) . "%";
                    }
                    $scorecard_report{$author_id}{$coach}{'scores'}{$article_id}
{$metric}
                    = $metric_value;
                }
            }
        }

        # use array to sort the calculating metrics
        @{$scorecard_report{$author_id}{$coach}{'scores'}{'metrics'}}
        = ( 'new article scored',
            'edited article scored',
            'total number of times scored',
            'Starting Percentage',
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
            'Total Score'
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

        my $subject = "$report_tag - KCS Article Scorecard Feedback for"
                      ." $author_name, Week Ending $stop_datetime";

        # send out the report email
        if ($author_email ne $coach_email) {
            email_results($from, $to, $cc, $bcc, $subject, $digest);
        }
    }

    #last;
}

#----------------------------------------------------------------------
# Subroutines...
#----------------------------------------------------------------------
#----------------------------------------------------------------------
# last Sunday
#----------------------------------------------------------------------
sub last_sunday_datetime {

    my $time   = shift;

    my @when   = localtime($time);
    my $dow    = $when[6];
    my $offset = 0;
    $offset    = 60*60*24*$dow;

    my @sunday_when = localtime($time - $offset);
    my $year   = $sunday_when[5] + 1900;
    my $mon    = $sunday_when[4] + 1;
    my $mday   = $sunday_when[3];

    my $datetime = sprintf("%04d-%02d-%02d 00:00:00", $year, $mon, $mday);

    return $datetime; 
}

#----------------------------------------------------------------------
# calculate the datetime of Sunday in previous weeks 
#----------------------------------------------------------------------
sub previous_sunday_datetime {

    my ($time, $week)   = @_;

    my @when   = localtime($time);
    my $dow    = $when[6];
    my $offset = 0;
    $offset    = 60*60*24*($dow + 7*$week);

    my @sunday_when = localtime($time - $offset);
    my $year   = $sunday_when[5] + 1900;
    my $mon    = $sunday_when[4] + 1;
    my $mday   = $sunday_when[3];

    my $datetime = sprintf("%04d-%02d-%02d 00:00:00", $year, $mon, $mday);

    return $datetime;
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
    this script used to send out individual weekly reports to scorecard in iKbase.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/individual_weekly_report_development.conf
      ../conf/individual_weekly_report_test.conf
      ../conf/individual_weekly_report_production.conf

  Examples:
  $0 -r -e development

EOP
}
