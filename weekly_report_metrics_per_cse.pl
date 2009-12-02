#!/usr/bin/perl
#----------------------------------------------------------------------
# this script is used to send out per CSE performance report(weekly)
#----------------------------------------------------------------------
use strict;
use Getopt::Std;
use Data::Dumper;
use Template;

use lib '../../../utils/lib';
use MyDB;
use FileUtils;
use SendMail;
use ParseConfig;

#----------------------------------------------------------------------
# check options -r, -e <environment> and read config vars from *.conf
# files
#----------------------------------------------------------------------

# check options
my %opts;
getopts( "re:", \%opts );

if ( (!defined $opts{'r'}) || (!defined $opts{'e'}) ) {
    usage();
    exit;
}
my $environment = $opts{'e'};

# config files path
my %env_opts = (
    'development' => '../conf/weekly_report_metrics_per_cse_development.conf',
    'test'        => '../conf/weekly_report_metrics_per_cse_test.conf',
    'production'  => '../conf/weekly_report_metrics_per_cse_production.conf',
);

my $config_file = $env_opts{$environment};

my ($stat, $err) = ParseConfig::colon($config_file); 
if ( !$stat ) {
    die $err;
}
my $config_vars_rh = $err;

# templates of report email
my $tmpl_dir    = $config_vars_rh->{'template_dir'};
my $report_tmpl = $config_vars_rh->{'report_template'};
my $header_tmpl = $config_vars_rh->{'header_template'};
my $footer_tmpl = $config_vars_rh->{'footer_template'};

die "tmpl_dir not defined"    unless ( defined $tmpl_dir );
die "report_tmpl not defined" unless ( defined $report_tmpl );
die "header_tmpl not defined" unless ( defined $header_tmpl );
die "footer_tmpl not defined" unless ( defined $footer_tmpl );

# sql file path to pull CSEs
my $cses_sql_file = $config_vars_rh->{'cses_sql_path'};
die "cses_sql_path not defined" unless ( defined $cses_sql_file );

# db alias
my $csat_db_alias   = $config_vars_rh->{'csat_db_alias'};
my $report_db_alias = $config_vars_rh->{'report_db_alias'};
my $ikb_db_alias    = $config_vars_rh->{'ikb_db_alias'};
my $rt_db_alias     = $config_vars_rh->{'rt_db_alias'};

die "csat_db_alias not defined"   unless ( defined $csat_db_alias );
die "report_db_alias not defined" unless ( defined $report_db_alias );
die "ikb_db_alias not defined"    unless ( defined $ikb_db_alias );
die "rt_db_alias not defined"     unless ( defined $rt_db_alias );

# email fields 
my $subject_prefix = $config_vars_rh->{'subject_prefix'};
my $email_from     = $config_vars_rh->{'from'};
my $email_to       = $config_vars_rh->{'to'};
my $email_bcc      = $config_vars_rh->{'bcc'};

die "subject_prefix not subject" unless ( defined $subject_prefix );
die "email_from not defined"     unless ( defined $email_from );
die "email_to not defined"       unless ( defined $email_to );
die "email_bcc not defined"      unless ( defined $email_bcc );

#----------------------------------------------------------------------
# create database connections - CSAT, report, iKbase, RT 
#----------------------------------------------------------------------

# CSAT DB
my ( $stat, $err ) = MyDB::getDBH($csat_db_alias);

if ( !$stat ) {
    die $err;
}
my $csat_dbh = $err;

# report DB
my ( $stat, $err ) = MyDB::getDBH($report_db_alias);

if ( !$stat ) {
    die $err;
}
my $report_dbh = $err;

# iKbase DB
my ( $stat, $err ) = MyDB::getDBH($ikb_db_alias);

if ( !$stat ) {
    die $err;
}
my $iKbase_dbh = $err;

# RT DB
my ( $stat, $err ) = MyDB::getDBH($rt_db_alias);

if ( !$stat ) {
    die $err;
}
my $rt_dbh = $err;

#----------------------------------------------------------------------
# pull out KPI and SPI metrics
#----------------------------------------------------------------------

# CSEs include this following roles: 'cse', 'e4e', 'sonata', 'lead'
my ( $stat, $err ) = FileUtils::file2string(
    {   file             => $cses_sql_file,
        comment_flag     => 1,
        blank_lines_flag => 1
    }
);
if ( !$stat ) {
    email_errors($err);
    die $err;
}
my $employee_sql = $err;

# pull out CSEs' array
my $cses_sth = $report_dbh->prepare($employee_sql) or die $report_dbh->errstr;
$cses_sth->execute() or die $cses_sth->errstr;

my @cses = ();
while ( my $cses_rh = $cses_sth->fetchrow_hashref ) {
    my $cse = $cses_rh->{'owner_name'};

    if ( !( grep{$_ eq $cse} @cses ) ) {
        push @cses, $cse;
    }
}


my $last_sunday = last_sunday_datetime(time);
my @week_ends_order = (
                       substr($last_sunday, 0, 10),
                       substr(previous_sunday_datetime(time, 1), 0, 10),
                       substr(previous_sunday_datetime(time, 2), 0, 10),
                       substr(previous_sunday_datetime(time, 3), 0, 10),
                       );

# KPI - New Tickets
my $new_tickets_KPI
  = new_tickets_KPI($last_sunday, $report_dbh);

# KPI - CSAT Avg
my $csat_avg_KPI
  = csat_avg_KPI($last_sunday, $csat_dbh);

# Global CSE CSAT Avg
my $csat_avg_Global
  = csat_avg_Global($last_sunday, $csat_dbh);

# Team CSE CSAT Avg
my $csat_avg_Team
  = csat_avg_Team(
                  $last_sunday,
                  \@week_ends_order,
                  \@cses,
                  $report_dbh,
                  $csat_dbh
                  );

# KPI - Tickets Touched
my $tickets_touched_KPI
  = tickets_touched_KPI($last_sunday, $report_dbh);

# KPI - Tickets Resolved
my $tickets_resolved_KPI
  = tickets_resolved_KPI($last_sunday, $report_dbh);

# KPI - Tickets Reopened
my $tickets_reopened_KPI
  = tickets_reopened_KPI($last_sunday, $report_dbh);

# KPI - New KB articles
my $new_kb_articles_KPI
  = new_kb_articles_KPI($last_sunday, $iKbase_dbh);

# KPI - KB linking %
my $kb_linking_KPI
  = kb_linking_KPI($last_sunday, $report_dbh, $rt_dbh); 

# SPI - Avg Ticket Resolution Time
my $avg_resolution_SPI
  = avg_resolution_SPI($last_sunday, $report_dbh);

# SPI - Avg Interactions Per Ticket
my $avg_interactions_per_ticket_SPI
  = avg_interactions_per_ticket_SPI($last_sunday, $report_dbh);

# SPI - P1/P2 Tickets
my $p1_p2_tickets_SPI
  = p1_p2_tickets_SPI($last_sunday, $report_dbh);

# SPI - Management Escalated Tickets 
my $management_escalated_tickets_SPI
  = management_escalated_tickets_SPI($last_sunday, $rt_dbh);

# SPI - Low CSATs (1-2) on CSE questions 
my $low_cast_questions_SPI
  = low_cast_questions_SPI($last_sunday, $csat_dbh);

# SPI - High CSATs (4-5) on CSE questions
my $high_csat_questions_SPI
  = high_csat_questions_SPI($last_sunday, $csat_dbh);

# All CSATs (1-5) on CSE questions
my $all_csat_questions_SPI
  = all_csat_questions_SPI($last_sunday, $csat_dbh);

# SPI - Total CSAT Surveys
my $total_cast_surveys_SPI
  = total_cast_surveys_SPI($last_sunday, $csat_dbh);

#----------------------------------------------------------------------
# use employees table to filter metric hashes and combine them into a 
# new hash with employees' ownername as the key 
#----------------------------------------------------------------------
my $employee_sth = $report_dbh->prepare($employee_sql)
  or die $report_dbh->errstr;

$employee_sth->execute() or die $employee_sth->errstr;

my %cse_metrics;
while( my $employee_rh = $employee_sth->fetchrow_hashref) {

    my $owner_name     = $employee_rh->{'owner_name'};
    my $employee_email = $employee_rh->{'email'};

    my $manager_email  = $employee_rh->{'manager'};

    # cse's name
    $cse_metrics{$owner_name}{'name'}    = $owner_name;

    # cse's email
    $cse_metrics{$owner_name}{'email'}   = $employee_email;

    # Weekends' order 
    @{$cse_metrics{$owner_name}{'week_ends_order'}}  = @week_ends_order;

    # Extend Metrics' order
    @{$cse_metrics{$owner_name}{'Extend'}{'metrics_order'}} = (
        'Global CSE CSAT Avg',
        'Team CSE CSAT Avg',
    );

    # KPI Metrics' order
    @{$cse_metrics{$owner_name}{'KPI'}{'metrics_order'}} = (
        'New Tickets',
        'Tickets Touched',
        'Tickets Resolved',
        'Tickets Reopened',
        'CSAT Avg',
        'New KB articles',
        'KB linking %',
    );

    # SPI Metrics' order
    @{$cse_metrics{$owner_name}{'SPI'}{'metrics_order'}} = (
        'Avg Ticket Resolution Time',
        'Avg Interactions Per Ticket',
        'P1/P2 Tickets',
        'Management Escalated Tickets',
        'Low CSATs',
        'High CSATs',

        'All CSATs',

        'Total CSAT Surveys',
#        'Average Ticket Backlog',
    );

    # consolidate a hash that contains the cse's metrics data
    foreach my $week_end ( @{$cse_metrics{$owner_name}{'week_ends_order'}} ) {

        # New Tickets
        if ( exists $new_tickets_KPI->{$owner_name}->{'New Tickets'}->
{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'New Tickets'}{$week_end}
              = $new_tickets_KPI->{$owner_name}->{'New Tickets'}->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'New Tickets'}{$week_end}
              = 0;
        }

        # Tickets Touched
        if ( exists $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Touched'}{$week_end}
              = $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Touched'}{$week_end}
              = 0;
        }

        # Tickets Resolved
        if ( exists $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Resolved'}{$week_end}
              = $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Resolved'}{$week_end}
              = 0;
        }

        # Tickets Reopened
        if ( exists $tickets_reopened_KPI->{$owner_name}->{'Tickets Reopened'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Reopened'}{$week_end}
              = $tickets_reopened_KPI->{$owner_name}->{'Tickets Reopened'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Reopened'}{$week_end}
              = 0;
        }

        # CSAT Avg
        if ( exists $csat_avg_KPI->{$owner_name}->{'CSAT Avg'}->{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'CSAT Avg'}{$week_end}
              = $csat_avg_KPI->{$owner_name}->{'CSAT Avg'}->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'CSAT Avg'}{$week_end}
              = 0;
        }

        # Global CSE CSAT Average
        if ( exists $csat_avg_Global->{$week_end} ) {
            $cse_metrics{$owner_name}{'Extend'}{'Global CSE CSAT Avg'}
{$week_end}
              = $csat_avg_Global->{$week_end}
        }

        # Team CSE CSAT Average
        foreach my $manager ( keys %{$csat_avg_Team} ) {
            if ($manager eq $manager_email) {
                $cse_metrics{$owner_name}{'Extend'}{'Team CSE CSAT Avg'}
{$week_end}
                  = $csat_avg_Team->{$manager}->{$week_end};

                last;
            }
        }

        # New KB articles
        if ( exists $new_kb_articles_KPI->{$owner_name}->{'New KB articles'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'New KB articles'}{$week_end}
              = $new_kb_articles_KPI->{$owner_name}->{'New KB articles'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'New KB articles'}{$week_end}
              = 0;
        }

        # KB linking %
        if ( exists $kb_linking_KPI->{$owner_name}->{'KB linking %'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'KPI'}{'KB linking %'}{$week_end}
              = $kb_linking_KPI->{$owner_name}->{'KB linking %'}->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'KB linking %'}{$week_end}
              = 0;
        }

        # Avg Ticket Resolution Time
        if ( exists $avg_resolution_SPI->{$owner_name}->
{'Avg Ticket Resolution Time'}->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
{$week_end}
              = $avg_resolution_SPI->{$owner_name}->
{'Avg Ticket Resolution Time'}->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
{$week_end}
              = 0;
        }

        # Avg Interactions Per Ticket
        if ( exists $avg_interactions_per_ticket_SPI->{$owner_name}->
{'Avg Interactions Per Ticket'}->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Interactions Per Ticket'}
{$week_end}
              = $avg_interactions_per_ticket_SPI->{$owner_name}->
{'Avg Interactions Per Ticket'}->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Interactions Per Ticket'}
{$week_end}
              = 0;
        }

        # P1/P2 Tickets
        if ( exists $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'P1/P2 Tickets'}{$week_end}
              = $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'}->
{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'P1/P2 Tickets'}{$week_end}
              = 0;
        }

        # Management Escalated Tickets
        if ( exists $management_escalated_tickets_SPI->{$owner_name}->
{'Management Escalated Tickets'}->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Management Escalated Tickets'}
{$week_end}
              = $management_escalated_tickets_SPI->{$owner_name}->
{'Management Escalated Tickets'}->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Management Escalated Tickets'}
{$week_end}
              = 0;
        }

        # Low CSATs
        if ( exists $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Low CSATs'}{$week_end}
              = $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Low CSATs'}{$week_end}
              = 0;
        }

        # High CSATs
        if ( exists $high_csat_questions_SPI->{$owner_name}->{'High CSATs'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'High CSATs'}{$week_end}
              = $high_csat_questions_SPI->{$owner_name}->{'High CSATs'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'High CSATs'}{$week_end}
              = 0;
        }

        # All CSATs
        if ( exists $all_csat_questions_SPI->{$owner_name}->{'All CSATs'}
->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'All CSATs'}{$week_end}
              = $all_csat_questions_SPI->{$owner_name}->{'All CSATs'}
->{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'All CSATs'}{$week_end}
              = 0;
        }

        # Total CSAT Surveys
        if ( exists $total_cast_surveys_SPI->{$owner_name}->
{'Total CSAT Surveys'}->{$week_end} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Total CSAT Surveys'}{$week_end}
              = $total_cast_surveys_SPI->{$owner_name}->{'Total CSAT Surveys'}->
{$week_end};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Total CSAT Surveys'}{$week_end}
              = 0;
        }
    }

    # update mail addr from ironport to cisco for 'jsandl'
    if ( $manager_email =~ /jsandl\@ironport.com/ ) {
        $manager_email = "jsandl\@cisco.com";
    }

    # manager's email
    $cse_metrics{$owner_name}{'manager'} = $manager_email;
}

#----------------------------------------------------------------------
# output per CSE's report email
#----------------------------------------------------------------------
my $tt = Template->new(
    {   INCLUDE_PATH => $tmpl_dir, 
        EVAL_PERL    => 1,
    }
) || die $Template::ERROR, "\n";

foreach my $per_cse_metrics ( keys %cse_metrics ) {

    # Calculate KB Linking % for per CSE
    foreach my $week_end ( @{$cse_metrics{$per_cse_metrics}{'week_ends_order'}})
    {
        if ( ( exists $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}
{$week_end} )
            && ( exists $cse_metrics{$per_cse_metrics}{'KPI'}
{'Tickets Resolved'}{$week_end} ) )
        {

            my $kb_linking
              = $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}
{$week_end};

            my $tickets_resolved
              = $cse_metrics{$per_cse_metrics}{'KPI'}{'Tickets Resolved'}
{$week_end};

            my $kb_linking_percent;

            if ( $tickets_resolved != 0 ) {
                $kb_linking_percent =  ($kb_linking / $tickets_resolved) * 100;
            }
            else {
                $kb_linking_percent = 0;
            }

            if ( $kb_linking_percent != 0 ) {
                $kb_linking_percent = sprintf("%.2f", $kb_linking_percent);
            }

            $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}{$week_end}
               = $kb_linking_percent
        }
        else {
            $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}{$week_end}
              = 0;
        }
    }

    # Generate the body of report email 
    my $output;
    my %input_vars;

    %{$input_vars{'items'}} = %{$cse_metrics{$per_cse_metrics}};
    #print Dumper($input_vars{'items'});

    $tt->process($report_tmpl, \%input_vars, \$output);

    my $digest = get_email($header_tmpl, $output, $footer_tmpl);

    # Configure to, cc depends on the environment 
    my $to;
    if ($environment =~ /development|test/i) {
        $to = $email_to;
    }
    elsif ( $environment =~ /production/i ) {
        $to = $cse_metrics{$per_cse_metrics}{'email'};
    }

    my $cc;
    if ( $environment =~ /development|test/i ) {
        $cc = '';
    }
    elsif ( $environment =~ /production/i ) {
        $cc = $cse_metrics{$per_cse_metrics}{'manager'};
    }

    my $bcc;
    if ( $environment =~ /development|test/i ) {
        $bcc = '';
    }
    elsif ( $environment =~ /production/i ) {
        $bcc = $email_bcc;
    }

    my $subject = $subject_prefix . $cse_metrics{$per_cse_metrics}{'name'};

    # output the report email
    email_results($email_from, $to, $cc, $bcc, $subject, $digest);

    #last;
}


#----------------------------------------------------------------------
# subordinates...
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# New Tickets
#----------------------------------------------------------------------
sub new_tickets_KPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', owner) = 0,
                   owner, LEFT(owner, LOCATE('\@', owner) - 1)
                 ) AS owner,
                 week_ending,
                 COUNT(*) AS total
               FROM
                 case_details
               WHERE 1
                 AND created >= ? 
                 AND created < ? 
               GROUP BY owner, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %new_tickets_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner       = $rh->{'owner'};
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $total       = $rh->{'total'};

        $new_tickets_metric{$owner}{'New Tickets'}{$week_ending} = $total;
    }

    return \%new_tickets_metric;
}

#----------------------------------------------------------------------
# Tickets Touched
#----------------------------------------------------------------------
sub tickets_touched_KPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', cse) = 0,
                   cse, LEFT(cse, LOCATE('\@', cse) - 1)
                 ) AS owner,
                 week_ending,
                 cases_touched
               FROM
                 interactions
               WHERE 1
                 AND week_ending > ?
                 AND week_ending <= ?
               GROUP BY cse, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %tickets_touched_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner         = $rh->{'owner'}; 
        my $week_ending   = substr($rh->{'week_ending'}, 0, 10);
        my $cases_touched = $rh->{'cases_touched'};

        $tickets_touched_metric{$owner}{'Tickets Touched'}{$week_ending}
          = $cases_touched;
    }

    return \%tickets_touched_metric;
}

#----------------------------------------------------------------------
# Tickets Resolved
#----------------------------------------------------------------------
sub tickets_resolved_KPI {

    my ($report_stop, $dbh) = @_;

    my $sql
    = "
    SELECT
      IF(LOCATE('\@', owner) = 0,
        owner, LEFT(owner, LOCATE('\@', owner) - 1)
      ) AS owner_name,
      CONCAT(
        LEFT(DATE_ADD(reso_timestamp
            , INTERVAL (8 - DAYOFWEEK(reso_timestamp)) DAY)
        , 10)
      , ' 00:00:00') AS week_end,
      COUNT(case_number) AS tickets_resolved
    FROM
      case_details
    WHERE 1
      AND reso_timestamp >= ?
      AND reso_timestamp < ?
      AND status = 'resolved'
    GROUP BY owner, week_end
    ORDER BY week_end desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %tickets_resolved_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner            = $rh->{'owner_name'};
        my $week_ending      = substr($rh->{'week_end'}, 0, 10);
        my $tickets_resolved = $rh->{'tickets_resolved'};

        $tickets_resolved_metric{$owner}{'Tickets Resolved'}{$week_ending}
          = $tickets_resolved;
    }

    return \%tickets_resolved_metric;
}

#----------------------------------------------------------------------
# Tickets Reopened
#----------------------------------------------------------------------
sub tickets_reopened_KPI {

    my ($report_stop, $dbh) = @_;

    my $sql
    = "
    SELECT
      IF(LOCATE('\@', cse.email) = 0,
        cse.email, LEFT(cse.email, LOCATE('\@', cse.email) - 1)
      ) AS owner,
      CONCAT(
        LEFT(DATE_ADD(Tickets.resolved,
            INTERVAL (8 - DAYOFWEEK(Tickets.resolved)) DAY)
        , 10)
      , ' 00:00:00') AS week_ending,
      SUM(T_reopen.NewValue = 'open') as tickets_reopened
    FROM 
      rt3.Tickets Tickets
         LEFT JOIN report.employees cse ON 
          (Tickets.Owner = cse.id)
         LEFT JOIN rt3.Transactions T_reopen ON
          (Tickets.id = T_reopen.Ticket
          AND T_reopen.OldValue = 'resolved'
          AND T_reopen.NewValue = 'open')
    WHERE 
      Tickets.id = Tickets.effectiveid /* no merged cases */ 
      AND Tickets.Status IN ('resolved') /* no rejected or deleted ticktes */ 
      AND Tickets.Queue IN (1, 25, 26, 38, 24, 8, 30, 21)
      /* CSR=1, SMB=25, ENT=26, ENC=38, WSA=24, BETA=8, CRES=30, PORTAL=21 */ 
      AND Tickets.resolved >= ?
      AND Tickets.resolved <  ?
    GROUP BY owner, week_ending
    ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %tickets_reopened_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner            = $rh->{'owner'};
        my $week_ending      = substr($rh->{'week_ending'}, 0, 10);
        my $tickets_reopened = $rh->{'tickets_reopened'};

        if ( defined $tickets_reopened ) {
            $tickets_reopened_metric{$owner}{'Tickets Reopened'}{$week_ending}
              = $tickets_reopened;
        }
    }

    return \%tickets_reopened_metric;
}

#----------------------------------------------------------------------
# CSAT Avg
#----------------------------------------------------------------------
sub csat_avg_KPI {

    my ($report_stop, $dbh) = @_;

    my $sql
    = "
    SELECT
      e.owner,
      e.week_ending,
      ROUND(
      (SUM(e.csat) / SUM(e.number)), 2) AS csat
    FROM
        (
        SELECT
          owner,
          CONCAT(
            LEFT(DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
                ), 10
            ), ' 00:00:00'
          ) AS week_ending,
          (
            ((
              SUM(q_experience) +
              SUM(q_courteousn) +
              SUM(q_expertise)  +
              SUM(q_responsive) +
              SUM(q_timeliness) +
              SUM(q_completens) 
             ) / (  6 )
            ) / COUNT(*)
          ) AS csat,
          COUNT(*) AS number
        FROM
          survey
        WHERE 1
          AND survey._del!='Y'
          AND qp_ts >= ?
          AND qp_ts < ?
        GROUP BY id) e
    GROUP BY e.owner, e.week_ending
    ORDER BY e.week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute( $report_start, $report_stop ) or die $sth->errstr;

    my %csat_avg_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner       = $rh->{'owner'};
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $csat        = $rh->{'csat'};

        $report_stop = substr($report_stop, 0, 10);
        $csat_avg_metric{$owner}{'CSAT Avg'}{$week_ending} = $csat;
    }

    return \%csat_avg_metric;
}

#----------------------------------------------------------------------
# Global CSE CSAT Avg - divide the sum of all surveys' CSAT results by
# the total number of surveys in one week
#----------------------------------------------------------------------
sub csat_avg_Global {

    my ($report_stop, $dbh) = @_;

    my $sql
    = "
    SELECT
      e.week_ending,
      ROUND(
      (SUM(e.csat) / SUM(e.number)), 2) AS csat
    FROM
        (
        SELECT
          owner,
          CONCAT(
            LEFT(DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
                ), 10
            ), ' 00:00:00'
          ) AS week_ending,
          (
            ((
              SUM(q_experience) +
              SUM(q_courteousn) +
              SUM(q_expertise)  +
              SUM(q_responsive) +
              SUM(q_timeliness) +
              SUM(q_completens) 
             ) / (  6 )
            ) / COUNT(*)
          ) AS csat,
          COUNT(*) AS number
        FROM
          survey
        WHERE 1
          AND survey._del!='Y'
          AND qp_ts >= ?
          AND qp_ts < ?
        GROUP BY id) e
    GROUP BY e.week_ending
    ORDER BY e.week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute( $report_start, $report_stop ) or die $sth->errstr;

    my %csat_avg_Global = ();
    while( my $rh = $sth->fetchrow_hashref ) {
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $csat        = $rh->{'csat'};

        $csat_avg_Global{$week_ending} = $csat;
    }

    return \%csat_avg_Global;
}

#----------------------------------------------------------------------
# Team CSE CSAT Avg
#----------------------------------------------------------------------
sub csat_avg_Team {

    my ($report_stop, $week_ends_order, $cses, $report_dbh, $csat_dbh) = @_;

    # figure out how many teams, and make relationships between
    # CSEs and their teams with the use of 'report.employees' table.
    my $sql = "SELECT
                 manager
               FROM
                 employees
               WHERE
                 SUBSTR(email, 1, LOCATE('\@', email) - 1) = ?";
    my $sth = $report_dbh->prepare($sql) or die $report_dbh->errstr;

    my %cses_info = ();

    my @managers = ();
    foreach my $cse ( @{$cses} ) {

        $sth->execute($cse) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $manager = $rh->{'manager'};

            $cses_info{$cse}{'manager'} = $manager;

            if ( !( grep{ $_ eq $manager } @managers ) ) {
                push @managers, $manager;
            }
        }
    }

    # figure out the sum of all surveys' CSAT results and the total number of
    # surveys for each team in one week, then we can divide the former with the
    # later to get 'Team CSE CSAT Avg.'.
    #we can use a CSE's name to determine an survey belongs to which team.
    my $survey_sql
    = "
    SELECT
      owner,
      LEFT(DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
        ), 10) AS week_ending,
      ROUND(
      (
        ((
          SUM(q_experience) +
          SUM(q_courteousn) +
          SUM(q_expertise)  +
          SUM(q_responsive) +
          SUM(q_timeliness) +
          SUM(q_completens) 
         ) / (  6 )
        ) / COUNT(*)
      ), 2) AS csat
    FROM
      survey
    WHERE 1
      AND survey._del!='Y'
      AND qp_ts >= ?
      AND qp_ts < ?
    GROUP BY id";
    my $survey_sth = $csat_dbh->prepare($survey_sql) or die $csat_dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $survey_sth->execute( $report_start, $report_stop )
    or die $survey_sth->errstr;

    while( my $rh = $survey_sth->fetchrow_hashref ) {
        my $owner       = $rh->{'owner'};
        my $week_ending = $rh->{'week_ending'};
        my $csat        = $rh->{'csat'};

        $cses_info{$owner}{$week_ending}{'value'} += $csat;
        $cses_info{$owner}{$week_ending}{'count'} += 1;
    }

    my %csat_avg_Team = ();
    foreach my $manager ( @managers ) {

        foreach my $week_end ( @{$week_ends_order} ) {

            my $csat_value = 0;
            my $csat_count = 0;
            foreach my $cse ( keys %cses_info ) {

                next if ( !exists $cses_info{$cse}{'manager'} );
                next if ( !exists $cses_info{$cse}{$week_end}{'value'} );
                next if ( !exists $cses_info{$cse}{$week_end}{'count'} );

                next if ( $cses_info{$cse}{'manager'} ne $manager );

                $csat_value += $cses_info{$cse}{$week_end}{'value'};
                $csat_count += $cses_info{$cse}{$week_end}{'count'};
            }

            my $avg_value = 0;
            if ( $csat_count != 0 ) {
                $avg_value = sprintf("%.2f", ($csat_value / $csat_count));
            }

            $csat_avg_Team{$manager}{$week_end} = $avg_value;
        }
    }

    return \%csat_avg_Team;
}

#----------------------------------------------------------------------
# New KB articles
#----------------------------------------------------------------------
sub new_kb_articles_KPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 u.name AS owner,
                 e.week_ending AS week_ending,
                 COUNT(*) AS total
               FROM
                 (  (
                    SELECT
                      articles.id AS article_id,
                      CONCAT(
                        LEFT(
                          DATE_ADD(history.rowmtime,
                            INTERVAL (8 - DAYOFWEEK(history.rowmtime)) DAY
                          ), 10
                        ), ' 00:00:00'
                      ) AS week_ending, 

                      CASE WHEN realowner_article.user_id IS NOT NULL
                        THEN realowner_article.user_id
                      ELSE articles.owner END AS owner,

                      CASE WHEN realowner_article.user_id IS NOT NULL
                        THEN '1'
                      ELSE '0' END AS ext_pub

                    FROM
                      ( articles JOIN history ON articles.id = history.article_id )
                      LEFT JOIN realowner_article
                      ON articles.id = realowner_article.article_id

                    WHERE 1
                      AND articles.status NOT IN ('4')
                      AND history.status   = '6'
                      AND history.rowmtime >= ? 
                      AND history.rowmtime < ? 
                    ORDER BY articles.id
                    ) e
                    JOIN users u ON e.owner = u.id
                 )
                 JOIN articles a ON e.article_id = a.id
               GROUP BY u.name, e.week_ending";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4); 
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %new_kb_articles_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner          = $rh->{'owner'};
        my $week_ending    = substr($rh->{'week_ending'}, 0, 10);
        my $articles_total = $rh->{'total'};

        $new_kb_articles_metric{$owner}{'New KB articles'}{$week_ending}
          = $articles_total;
    }

    return \%new_kb_articles_metric;
}

#----------------------------------------------------------------------
# KB linking %
#----------------------------------------------------------------------
sub kb_linking_KPI {

    my ($report_stop, $report_dbh, $rt_dbh) = @_;

    my $report_start = previous_sunday_datetime(time, 4);

    # pull out the tickets that have the custome field - 'iKbase_ID' from RT
    my $kb_linked_sql = "
        SELECT
            t.Id
        FROM
          Tickets t, TicketCustomFieldValues c, CustomFields cf, Users u
        WHERE 1
          AND t.Id = c.Ticket
          AND t.Id = t.EffectiveId
          AND t.Id = c.Ticket
          AND c.CustomField = cf.Id
          AND cf.Name = 'iKbase_ID'
          AND t.Queue in (1,24,25,26)
          AND t.Created >= ?
          AND t.Created < ?
          AND t.owner = u.id
          AND u.name <> 'Nobody'";
    my $kb_linked_sth = $rt_dbh->prepare($kb_linked_sql)
      or die $rt_dbh->errstr;

    $kb_linked_sth->execute($report_start, $report_stop)
      or die $kb_linked_sth->errstr;

    my @kb_tickets = qw//;
    while ( my $kb_linked_rh = $kb_linked_sth->fetchrow_hashref ) {

        my $ticket_number = $kb_linked_rh->{'Id'};
        push @kb_tickets, $ticket_number;
    }

    # pull out the tickets which their status is 'Resolved' from report database
    my $resolved_sql = "
        SELECT
          IF(LOCATE('\@', owner) = 0,
            owner, LEFT(owner, LOCATE('\@', owner) - 1)
          ) AS owner,
          week_ending,
          case_number
        FROM
          case_details
        WHERE 1
          AND created >= ?
          AND created < ?
          AND status = 'resolved'";
    my $resolved_sth = $report_dbh->prepare($resolved_sql)
      or die $report_dbh->errstr;

    $resolved_sth->execute($report_start, $report_stop)
      or die $resolved_sth->errstr;

    my %kb_linking_metric;
    while ( my $resolved_rh = $resolved_sth->fetchrow_hashref ) {

        my $owner         = $resolved_rh->{'owner'};
        my $week_ending   = substr($resolved_rh->{'week_ending'}, 0, 10);
        my $ticket_number = $resolved_rh->{'case_number'};

        if ( grep {$ticket_number eq $_} @kb_tickets ) {
            $kb_linking_metric{$owner}{'KB linking %'}{$week_ending} += 1;
        }
        else {
            $kb_linking_metric{$owner}{'KB linking %'}{$week_ending} += 0;
        }

    }

    return \%kb_linking_metric;
}

#----------------------------------------------------------------------
# Avg Ticket Resolution Time (seconds) 
#----------------------------------------------------------------------
sub avg_resolution_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', owner) = 0,
                   owner, LEFT(owner, LOCATE('\@', owner) - 1)
                 ) AS owner,
                 week_ending,
                 ROUND(
                   (SUM(resolution_time) / COUNT(case_number)) / (60*60*24)
                 , 2) AS avg_ticket_resolution_time
               FROM
                 case_details
               WHERE 1
                 AND created >= ?
                 AND created < ?
                 AND status = 'resolved'
               GROUP BY owner, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %avg_resolution_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner       = $rh->{'owner'};
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $avg_time    = $rh->{'avg_ticket_resolution_time'};

        $avg_resolution_metric{$owner}{'Avg Ticket Resolution Time'}
{$week_ending} = $avg_time;
    }

    return \%avg_resolution_metric;
}

#----------------------------------------------------------------------
# Avg Interactions Per Ticket
#----------------------------------------------------------------------
sub avg_interactions_per_ticket_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', cse) = 0,
                   cse, LEFT(cse, LOCATE('\@', cse) - 1)
                 ) AS owner,
                 week_ending,
                 ROUND(
                   (interactions / cases_touched)
                 , 2) AS avg_interactions
               FROM
                 interactions
               WHERE 1
                 AND week_ending > ?
                 AND week_ending <= ?
               GROUP BY cse, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %avg_interactions_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner            = $rh->{'owner'};
        my $week_ending      = substr($rh->{'week_ending'}, 0, 10);
        my $avg_interactions = $rh->{'avg_interactions'};

        $avg_interactions_metric{$owner}{'Avg Interactions Per Ticket'}
{$week_ending}
          = $avg_interactions;
    }

    return \%avg_interactions_metric;

}

#----------------------------------------------------------------------
# P1/P2 Tickets
#----------------------------------------------------------------------
sub p1_p2_tickets_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', owner) = 0,
                   owner, LEFT(owner, LOCATE('\@', owner) - 1)
                 ) AS owner,
                 week_ending,
                 COUNT(*) AS p1_p2
               FROM
                 case_details
               WHERE 1
                 AND created >= ?
                 AND created < ?
                 AND (case_details.priority LIKE 'P1%'
                      OR case_details.priority LIKE 'P2%')
               GROUP BY owner, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %p1_p2_tickets_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner       = $rh->{'owner'};
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $p1_p2       = $rh->{'p1_p2'};

        $p1_p2_tickets_metric{$owner}{'P1/P2 Tickets'}{$week_ending}
          = $p1_p2;
    }

    return \%p1_p2_tickets_metric
}

#----------------------------------------------------------------------
# Management Escalated Tickets
#----------------------------------------------------------------------
sub management_escalated_tickets_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', u.Name) = 0,
                    u.Name,
                    LEFT(u.Name, LOCATE('\@', u.Name)-1)
                 ) AS owner,
                 CONCAT(
                   LEFT(
                     DATE_ADD(t.Resolved,
                       INTERVAL (8 - DAYOFWEEK(t.Resolved)) DAY
                     ), 10
                   ), ' 00:00:00'
                 ) AS week_ending,
                 COUNT(t.id) AS management_escalated_tickets
                 FROM
                   Tickets t, TicketCustomFieldValues c, CustomFields cf,
                   Users u
                 WHERE   1
                   AND  t.Resolved >= ?
                   AND  t.Resolved < ?
                   AND  t.Id = t.EffectiveId
                   AND  t.Owner = u.Id
                   AND  t.Status = 'resolved'
                   AND  u.Name <> 'Nobody'
                   AND  t.Id = c.Ticket
                   AND  c.CustomField = cf.Id
                   AND  cf.Name = 'Escalate Ticket'
                 GROUP BY owner, week_ending
                 ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %management_escalated_tickets_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner       = $rh->{'owner'};
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $management_escalated_tickets
          = $rh->{'management_escalated_tickets'};

        $management_escalated_tickets_metric{$owner}
{'Management Escalated Tickets'}{$week_ending}
          = $management_escalated_tickets;
    }

    return \%management_escalated_tickets_metric
}

#----------------------------------------------------------------------
# Low CSATs (1-2) on CSE questions
#----------------------------------------------------------------------
sub low_cast_questions_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 owner,
                 CONCAT(
                  LEFT(
                   DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
                   ), 10
                  ), ' 00:00:00'
                 ) AS week_ending,
                 COUNT(*) AS total_low_csat
                FROM
                (
                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_experience IN (1, 2, 1.0, 2.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_courteousn IN (1, 2, 1.0, 2.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_expertise IN (1, 2, 1.0, 2.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_responsive IN (1, 2, 1.0, 2.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_completens IN (1.0, 2.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_timeliness IN (1, 2, 1.0, 2.0)
                ) e
                WHERE 1
                  AND qp_ts >= ?
                  AND qp_ts < ?
                  AND owner <> ''
                GROUP BY owner, week_ending
                ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %low_cast_questions_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner          = $rh->{'owner'};
        my $week_ending    = substr($rh->{'week_ending'}, 0, 10);
        my $total_low_csat = $rh->{'total_low_csat'};

        $low_cast_questions_metric{$owner}{'Low CSATs'}{$week_ending}
          = $total_low_csat;
    }

    return \%low_cast_questions_metric
}

#----------------------------------------------------------------------
# High CSATs (4-5) on CSE questions
#----------------------------------------------------------------------
sub high_csat_questions_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 owner,
                 CONCAT(
                  LEFT(
                   DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
                   ), 10
                  ), ' 00:00:00'
                 ) AS week_ending,
                 COUNT(*) AS total_high_csat
                FROM
                (
                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_experience IN (4, 5, 4.0, 5.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_courteousn IN (4, 5, 4.0, 5.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_expertise IN (4, 5, 4.0, 5.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_responsive IN (4, 5, 4.0, 5.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_completens IN (4.0, 5.0)

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_timeliness IN (4, 5, 4.0, 5.0)
                ) e
                WHERE 1
                  AND qp_ts >= ?
                  AND qp_ts < ?
                  AND owner <> ''
                GROUP BY owner, week_ending
                ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %high_csat_questions_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner           = $rh->{'owner'};
        my $week_ending     = substr($rh->{'week_ending'}, 0, 10);
        my $total_high_csat = $rh->{'total_high_csat'};

        $high_csat_questions_metric{$owner}{'High CSATs'}{$week_ending}
          = $total_high_csat;
    }

    return \%high_csat_questions_metric
}

#----------------------------------------------------------------------
# All CSATs (1-5) on CSE questions
#----------------------------------------------------------------------
sub all_csat_questions_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 owner,
                 CONCAT(
                  LEFT(
                   DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
                   ), 10
                  ), ' 00:00:00'
                 ) AS week_ending,
                 COUNT(*) AS total_csat
                FROM
                (
                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_experience <> ''

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_courteousn <> ''

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_expertise <>''

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_responsive <> ''

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_completens <> ''

                UNION ALL

                SELECT
                  id, owner, qp_ts
                FROM
                  survey
                WHERE 1
                  AND _del != 'Y'
                  AND q_timeliness <> ''
                ) e
                WHERE 1
                  AND qp_ts >= ?
                  AND qp_ts < ?
                  AND owner <> ''
                GROUP BY owner, week_ending
                ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %all_csat_questions_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner       = $rh->{'owner'};
        my $week_ending = substr($rh->{'week_ending'}, 0, 10);
        my $total_csat  = $rh->{'total_csat'};

        $all_csat_questions_metric{$owner}{'All CSATs'}{$week_ending}
          = $total_csat;
    }

    return \%all_csat_questions_metric
}

#----------------------------------------------------------------------
# Total CSAT Surveys
#----------------------------------------------------------------------
sub total_cast_surveys_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                owner,
                week_ending,
                COUNT(*) AS total_csat_surveys
               FROM
                 (
                 SELECT
                   id,owner,qp_ts,
                   CONCAT(
                     LEFT(
                       DATE_ADD(qp_ts, INTERVAL (8 - DAYOFWEEK(qp_ts)) DAY
                       ), 10
                     ), ' 00:00:00'
                   ) AS week_ending
                 FROM
                   survey
                 WHERE
                   _del != 'Y'
                 ) e
               WHERE
                 qp_ts >= ?
                 AND qp_ts < ?
                 AND owner<>''
               GROUP BY owner, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %total_cast_surveys_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner              = $rh->{'owner'};
        my $week_ending        = substr($rh->{'week_ending'}, 0, 10);
        my $total_csat_surveys = $rh->{'total_csat_surveys'};

        $total_cast_surveys_metric{$owner}{'Total CSAT Surveys'}{$week_ending}
          = $total_csat_surveys;
    }

    return \%total_cast_surveys_metric
}

#----------------------------------------------------------------------
# Average Ticket Backlog
#----------------------------------------------------------------------
sub average_ticket_backlog_SPI {

    my ($report_stop, $dbh) = @_;

    my $sql = "SELECT
                 IF(LOCATE('\@', u.Name) = 0,
                    u.Name,
                    LEFT(u.Name, LOCATE('\@', u.Name)-1)
                 ) AS owner,
                 CONCAT(
                   LEFT(
                     DATE_ADD(t.Created, INTERVAL (8 - DAYOFWEEK(t.Created)) DAY
                     ), 10
                   ), ' 00:00:00'
                 ) AS week_ending,
                 COUNT(t.id) AS average_backlog_per_week,
               FROM
                 Tickets t, Users u
               WHERE 1
                 AND (t.Status   = 'open'
                     OR t.Status = 'stalled'
                     OR t.Status = 'new')
                 AND t.Owner = u.id
                 AND u.Name <> 'Nobody'
                 AND t.Created >= ?
                 AND t.Created < ?
               GROUP BY owner, week_ending
               ORDER BY week_ending desc";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = previous_sunday_datetime(time, 4);
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %average_ticket_backlog_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner                    = $rh->{'owner'};
        my $week_ending              = substr($rh->{'week_ending'}, 0, 10);
        my $average_backlog_per_week = $rh->{'average_backlog_per_week'};

        $average_ticket_backlog_metric{$owner}{'Average Ticket Backlog'}
{$week_ending}
          = $average_backlog_per_week;
    }

    return \%average_ticket_backlog_metric
}

#----------------------------------------------------------------------
# current datetime
#----------------------------------------------------------------------
sub current_datetime {

    my $time = shift;

    my ( $sec, $min, $hour, $mday, $mon, $year ) = localtime($time); 
    $year += 1900;
    $mon  += 1;

    my $datetime
      = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday,
        $hour, $min, $sec);

    return $datetime;
}

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
    my ( $stat, $err ) = FileUtils::file2string(
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
        die "could not send out ikb digest";
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
    this script is used to send metrics report to per CSE weekly.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/weekly_report_metrics_per_cse_development.conf
      ../conf/weekly_report_metrics_per_cse_test.conf
      ../conf/weekly_report_metrics_per_cse_production.conf

  Examples:
  $0 -r -e development

EOP
}
