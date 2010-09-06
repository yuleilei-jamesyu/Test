#!/usr/bin/perl
use strict;
use Getopt::Std;
use Data::Dumper;
use Template;

use Spreadsheet::WriteExcel;

#use Spreadsheet::Read;
use GD::Chart::Radial;

#use Spreadsheet::ParseExcel;

#use lib '../../../utils/lib';
use lib '/home/james/utils/lib';
use MyDB;
use FileUtils;
use SendMail;
use ParseConfig;

use lib '../lib';
use GetMetricsData;

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
my $environment  = $opts{'e'};

# config files path
my %env_opts = (
    'development'
        => '../conf/monthly_report_metrics_team_summary_development.conf',
    'test'
        => '../conf/monthly_report_metrics_team_summary_test.conf',
    'production'
        => '../conf/monthly_report_metrics_team_summary_production.conf',
);

my $config_file = $env_opts{$environment};

my ($stat, $err) = ParseConfig::colon($config_file); 
if ( !$stat ) {
    die $err;
}
my $config_vars_rh = $err;

# the name of month
my $month_name = $config_vars_rh->{'month_name'};
die "month_name not defined" unless ( defined $month_name );

# the range of week ends for a month
my $week_ending_start = $config_vars_rh->{'week_ending_start'};
my $week_ending_stop  = $config_vars_rh->{'week_ending_stop'};

die "week_ending_start not defined" unless ( defined $week_ending_start );
die "week_ending stop not defined"  unless ( defined $week_ending_stop );

$week_ending_start .= " 00:00:00";
$week_ending_stop  .= " 00:00:00";

my $month = {
            "start_weekend" => $week_ending_start,
            "stop_weekend"  => $week_ending_stop,
};

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

my $new_tickets_sql_file
  = $config_vars_rh->{'new_tickets_KPI'};
die "new_tickets_KPI not defined"
  unless ( defined $new_tickets_sql_file );

my $tickets_touched_sql_file
  = $config_vars_rh->{'tickets_touched_KPI'};
die "tickets_touched_KPI not defined"
  unless ( defined $tickets_touched_sql_file );

my $tickets_resolved_sql_file
  = $config_vars_rh->{'tickets_resolved_KPI'};
die "tickets_resolved_KPI not defined"
  unless ( defined $tickets_resolved_sql_file );

my $tickets_reopened_sql_file
  = $config_vars_rh->{'tickets_reopened_KPI'};
die "tickets_reopened_KPI not defined"
  unless ( defined $tickets_reopened_sql_file );

my $csat_avg_sql_file
  = $config_vars_rh->{'csat_avg_KPI'};
die "csat_avg_KPI not defined"
  unless ( defined $csat_avg_sql_file );

my $csat_avg_Global_sql_file
  = $config_vars_rh->{'csat_avg_Global'};
die "csat_avg_Global not defined"
  unless ( defined $csat_avg_Global_sql_file );

my $new_kb_articles_sql_file
  = $config_vars_rh->{'new_kb_articlets_KPI'};
die "new_kb_articlets_KPI not defined"
  unless ( defined $new_kb_articles_sql_file );

my $avg_resolution_sql_file
  = $config_vars_rh->{'avg_resolution_SPI'};
die "avg_resolution_KPI not defined"
  unless ( defined $avg_resolution_sql_file );

my $avg_interactions_per_ticket_sql_file
  = $config_vars_rh->{'avg_interactions_per_ticket_SPI'};
die "avg_interactions_per_ticket_SPI not defined"
  unless ( defined $avg_interactions_per_ticket_sql_file );

my $p1_p2_tickets_sql_file
  = $config_vars_rh->{'p1_p2_tickets_SPI'};
die "p1_p2_tickets_SPI not defined"
  unless ( defined $p1_p2_tickets_sql_file );

my $management_escalated_tickets_sql_file
  = $config_vars_rh->{'management_escalated_tickets_SPI'};
die "management_escalated_tickets_SPI not defined"
  unless ( defined $management_escalated_tickets_sql_file );

my $low_cast_questions_sql_file
  = $config_vars_rh->{'low_cast_questions_SPI'};
die "low_cast_questions_SPI not defined"
  unless ( defined $low_cast_questions_sql_file );

my $high_csat_questions_sql_file
  = $config_vars_rh->{'high_cast_questions_SPI'};
die "high_cast_questions_SPI not defined"
  unless ( defined $high_csat_questions_sql_file );

my $all_csat_questions_sql_file
  = $config_vars_rh->{'all_csat_questions_SPI'};
die "all_csat_questions_SPI not defined"
  unless ( defined $all_csat_questions_sql_file );

my $total_cast_surveys_sql_file
  = $config_vars_rh->{'total_cast_surveys_SPI'};
die "total_cast_surveys_SPI not defined"
  unless ( defined $total_cast_surveys_sql_file );

# data directory
my $data_dir = $config_vars_rh->{'data_dir'};
die "data_dir not defined" unless ( defined $data_dir );

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

die "subject_prefix not defined" unless ( defined $subject_prefix );
die "email_from not defined"     unless ( defined $email_from );
die "email_to not defined"       unless ( defined $email_to );
die "email_bcc not defined"      unless ( defined $email_bcc );

#----------------------------------------------------------------------
# create database connections - CSAT, report, iKbase, RT 
#----------------------------------------------------------------------

# CSAT DB
( $stat, $err ) = MyDB::getDBH($csat_db_alias);

if ( !$stat ) {
    die $err;
}
my $csat_dbh = $err;

# report DB
( $stat, $err ) = MyDB::getDBH($report_db_alias);

if ( !$stat ) {
    die $err;
}
my $report_dbh = $err;

# iKbase DB
( $stat, $err ) = MyDB::getDBH($ikb_db_alias);

if ( !$stat ) {
    die $err;
}
my $iKbase_dbh = $err;

# RT DB
( $stat, $err ) = MyDB::getDBH($rt_db_alias);

if ( !$stat ) {
    die $err;
}
my $rt_dbh = $err;

# SET SQL_BIG_SELECTS=1
set_big_sql($csat_dbh);
set_big_sql($report_dbh);
set_big_sql($iKbase_dbh);
set_big_sql($rt_dbh);

#----------------------------------------------------------------------
# pull out KPI and SPI metrics
#----------------------------------------------------------------------

# CSEs include this following roles: 'cse', 'e4e', 'sonata', 'lead'
( $stat, $err ) = FileUtils::file2string(
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

# KPI - New Tickets
my $new_tickets_KPI
  = GetMetricsData::main(
                        'New Tickets',
                        $new_tickets_sql_file,
                        $report_dbh,
                        $month
                        );

# KPI - Tickets Touched
my $tickets_touched_KPI
  = GetMetricsData::main(
                        'Tickets Touched',
                        $tickets_touched_sql_file,
                        $report_dbh,
                        $month
                        );

# KPI - Tickets Resolved
my $tickets_resolved_KPI
  = GetMetricsData::main(
                        'Tickets Resolved',
                        $tickets_resolved_sql_file,
                        $report_dbh,
                        $month
                        );

# KPI - Tickets Reopened
my $tickets_reopened_KPI
  = GetMetricsData::main(
                        'Tickets Reopened',
                        $tickets_reopened_sql_file,
                        $report_dbh,
                        $month
                        );

# KPI - CSAT Avg
my $csat_avg_KPI
  = GetMetricsData::main(
                        'CSAT Avg',
                        $csat_avg_sql_file,
                        $csat_dbh,
                        $month
                        );

# Global CSE CSAT Avg - divide the sum of all surveys' CSAT results by
# the total number of surveys in one month
my $csat_avg_Global
  = GetMetricsData::main(
                         'Global CSE CSAT Avg',
                         $csat_avg_Global_sql_file,
                         $csat_dbh,
                         $month
                         );

# Team CSE CSAT Avg
my $csat_avg_Team
  = csat_avg_Team(
                  $month,
                  \@cses,
                  $report_dbh,
                  $csat_dbh
                  );

# KPI - New KB articles
my $new_kb_articles_KPI
  = GetMetricsData::main(
                        'New KB articles',
                        $new_kb_articles_sql_file,
                        $iKbase_dbh,
                        $month
                        );

# KPI - KB linking %
my $kb_linking_KPI
  = kb_linking_KPI(
                   $month,
                   $report_dbh,
                   $rt_dbh
                   );

# SPI - Avg Ticket Resolution Time
my $avg_resolution_SPI
  = GetMetricsData::main(
                        'Avg Ticket Resolution Time',
                        $avg_resolution_sql_file,
                        $report_dbh,
                        $month
                        );

# SPI - Avg Interactions Per Ticket
my $avg_interactions_per_ticket_SPI
  = GetMetricsData::main(
                        'Avg Interactions Per Ticket',
                        $avg_interactions_per_ticket_sql_file,
                        $report_dbh,
                        $month
                        );

# SPI - P1/P2 Tickets
my $p1_p2_tickets_SPI
  = GetMetricsData::main(
                        'P1/P2 Tickets',
                        $p1_p2_tickets_sql_file,
                        $report_dbh,
                        $month
                        );

# SPI - Management Escalated Tickets
my $management_escalated_tickets_SPI
  = GetMetricsData::main(
                        'Management Escalated Tickets',
                        $management_escalated_tickets_sql_file,
                        $rt_dbh,
                        $month
                        );

# SPI - Low CSATs (1-2) on CSE questions
my $low_cast_questions_SPI
  = GetMetricsData::main(
                        'Low CSATs',
                        $low_cast_questions_sql_file,
                        $csat_dbh,
                        $month
                        );

# SPI - High CSATs (4-5) on CSE questions
my $high_csat_questions_SPI
  = GetMetricsData::main(
                        'High CSATs',
                        $high_csat_questions_sql_file,
                        $csat_dbh,
                        $month
                        );

# SPI - All CSATs (1-5) on CSE questions
my $all_csat_questions_SPI
  = GetMetricsData::main(
                        'All CSATs',
                        $all_csat_questions_sql_file,
                        $csat_dbh,
                        $month
                        );

# SPI - Total CSAT Surveys
my $total_cast_surveys_SPI
  = GetMetricsData::main(
                        'Total CSAT Surveys',
                        $total_cast_surveys_sql_file,
                        $csat_dbh,
                        $month
                        );

#----------------------------------------------------------------------
# use employees table to filter metric hashes and combine them into a 
# new hash with employees' ownername as the key 
#----------------------------------------------------------------------
my $employee_sth = $report_dbh->prepare($employee_sql)
  or die $report_dbh->errstr;
$employee_sth->execute() or die $employee_sth->errstr;

my %cse_metrics;
while( my $employee_rh = $employee_sth->fetchrow_hashref) {

    my $first_name     = $employee_rh->{'first_name'};
    my $last_name      = $employee_rh->{'last_name'};

    my $region         = $employee_rh->{'region'};
    my $role           = $employee_rh->{'role'};

    my $owner_name     = $employee_rh->{'owner_name'};
    my $employee_email = $employee_rh->{'email'};

    # pull out a manager's cisco email address to replace his/her
    # ironport email address
    my $manager_email = "";
    my $ironport_email = $employee_rh->{'manager'};

    # @cisco.com
    if ( $ironport_email =~ /.+\@cisco\.com/ ) {
        $manager_email = $ironport_email;
    }
    else {
        # @ironport.com
        my $cisco_email_sql
        = "
        SELECT
          primary_email
        FROM
          employees
        WHERE
          email = ?";
        my $cisco_email_sth = $report_dbh->prepare($cisco_email_sql)
        or die $report_dbh->errstr;

        $cisco_email_sth->execute($ironport_email)
          or die $cisco_email_sth->errstr;
        my $cisco_email_rh = $cisco_email_sth->fetchrow_hashref;

        if( $cisco_email_rh ) {
            $manager_email = $cisco_email_rh->{'primary_email'};
        }
    }

    # a manager's alias
    my $manager = "";
    if ( $manager_email =~ /(.+)\@(?:ironport|cisco)\.com/ ) {
        $manager = $1;
    }
    else {
        last;
    }

    # cse's full name
    my $full_name = $first_name . " " . $last_name;
    $cse_metrics{$manager}{$owner_name}{'full_name'}
      = $full_name;

    # cse's alias
    $cse_metrics{$manager}{$owner_name}{'name'}
      = $owner_name;

    # cse's region
    $cse_metrics{$manager}{$owner_name}{'region'}
      = $region;

    # cse's role
    $cse_metrics{$manager}{$owner_name}{'role'}
      = $role;

    # cse's manager
    $cse_metrics{$manager}{$owner_name}{'manager'}
      = $manager;

    # cse's email
    $cse_metrics{$manager}{$owner_name}{'email'}
      = $employee_email;

    # manager's name
    $cse_metrics{$manager}{'name'}
      = $manager;

    # manager's email
    $cse_metrics{$manager}{'email'}
      = $manager_email;

    # month's name
    $cse_metrics{$manager}{'month'} = $month_name;

    # Extend Metrics' order
    @{$cse_metrics{$manager}{'Extend_metrics_order'}} = (
        'Global CSE CSAT Avg',
        'Team CSE CSAT Avg',
    );

    # KPI Metrics' order
    @{$cse_metrics{$manager}{'KPI_metrics_order'}} = (
        'New Tickets',
        'Tickets Touched',
        'Tickets Resolved',
        'Tickets Reopened',
        'CSAT Avg',
        'New KB articles',
        'KB linking %',
    );

    # SPI Metrics' order
    @{$cse_metrics{$manager}{'SPI_metrics_order'}} = (
        'Avg Ticket Resolution Time',
        'Avg Interactions Per Ticket',
        'P1/P2 Tickets',
        'Management Escalated Tickets',
        'Low CSATs',
        'High CSATs',

        'All CSATs',

        'Total CSAT Surveys',
    );

    # consolidate a hash that contains the cse's metrics data

    # New Tickets
    if ( exists $new_tickets_KPI->{$owner_name}->{'New Tickets'}) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New Tickets'}
          = $new_tickets_KPI->{$owner_name}->{'New Tickets'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New Tickets'}
          = 0;
    }

    # Tickets Touched
    if ( exists $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Touched'}
          = $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Touched'}
          = 0;
    }

    # Tickets Resolved
    if ( exists $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Resolved'}
          = $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Resolved'}
          = 0;
    }

    # Tickets Reopened
    if ( exists $tickets_reopened_KPI->{$owner_name}->{'Tickets Reopened'} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Reopened'}
          = $tickets_reopened_KPI->{$owner_name}->{'Tickets Reopened'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Reopened'}
          = 0;
    }

    # CSAT Average
    if ( exists $csat_avg_KPI->{$owner_name}->{'CSAT Avg'} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'CSAT Avg'}
          = $csat_avg_KPI->{$owner_name}->{'CSAT Avg'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'CSAT Avg'}
          = 0;
    }

    # Global CSE CSAT Average
    if ( exists $csat_avg_Global->{'value'} ) {
        $cse_metrics{$manager}{'Extend'}{'Global CSE CSAT Avg'}
          = $csat_avg_Global->{'value'};
    }

    # Team CSE CSAT Average
    foreach my $cse_manager ( keys %{$csat_avg_Team} ) {

        if ($cse_manager eq $manager_email) {

            $cse_metrics{$manager}{'Extend'}{'Team CSE CSAT Avg'}
              = $csat_avg_Team->{$cse_manager};

            last;
        }
    }

    # New KB Articles
    if ( exists $new_kb_articles_KPI->{$owner_name}->{'New KB articles'} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New KB articles'}
          = $new_kb_articles_KPI->{$owner_name}->{'New KB articles'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New KB articles'}
          = 0;
    }

    # divide the count of 'KB linking' tickets by the count of resolved
    # tickets to figure out 'KB Linking %' for per CSE
    if (
        ( exists $kb_linking_KPI->{$owner_name}->{'KB linking %'} )
        &&
        ( exists $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'} )
        ) {

        my $kb_linking
          = $kb_linking_KPI->{$owner_name}->{'KB linking %'};

        my $tickets_resolved
          = $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'};

        my $kb_linking_percent;
        if ( $tickets_resolved != 0 ) {
              $kb_linking_percent = ($kb_linking / $tickets_resolved) * 100;
        }
        else {
              $kb_linking_percent = 0;
        }

        if ( $kb_linking_percent != 0 ) {
            $kb_linking_percent = sprintf("%.2f", $kb_linking_percent);
        }

        $cse_metrics{$manager}{$owner_name}{'KPI'}{'KB linking %'}
          = $kb_linking_percent;
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'KB linking %'}
          = 0;
    }

    # Avg Ticket Resolution Time
    if ( exists $avg_resolution_SPI->{$owner_name}
->{'Avg Ticket Resolution Time'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
          = $avg_resolution_SPI->{$owner_name}->{'Avg Ticket Resolution Time'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
          = 0;
    }

    #Avg Interactions Per Ticket
    if ( exists $avg_interactions_per_ticket_SPI->{$owner_name}
->{'Avg Interactions Per Ticket'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Avg Interactions Per Ticket'}
          = $avg_interactions_per_ticket_SPI->{$owner_name}
->{'Avg Interactions Per Ticket'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Avg Interactions Per Ticket'}
          = 0;
    }

    # P1/P2 Tickets
    if ( exists $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'P1/P2 Tickets'}
          = $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'P1/P2 Tickets'}
          = 0;
    }

    # Management Escalated Tickets
    if ( exists $management_escalated_tickets_SPI->{$owner_name}
->{'Management Escalated Tickets'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Management Escalated Tickets'}
          = $management_escalated_tickets_SPI->{$owner_name}
->{'Management Escalated Tickets'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Management Escalated Tickets'}
          = 0;
    }

    # Low CSATs (1-2) on CSE questions
    if ( exists $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Low CSATs'}
          = $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Low CSATs'}
          = 0;
    }

    # High CSATs (4-5) on CSE questions
    if ( exists $high_csat_questions_SPI->{$owner_name}->{'High CSATs'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'High CSATs'}
          = $high_csat_questions_SPI->{$owner_name}->{'High CSATs'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'High CSATs'}
          = 0;
    }

    # All CSATs (1-5) on CSE questions
    if ( exists $all_csat_questions_SPI->{$owner_name}->{'All CSATs'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'All CSATs'}
          = $all_csat_questions_SPI->{$owner_name}->{'All CSATs'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'All CSATs'}
          = 0;
    }

    # Total CSAT Surveys
    if ( exists $total_cast_surveys_SPI->{$owner_name}
->{'Total CSAT Surveys'} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Total CSAT Surveys'}
          = $total_cast_surveys_SPI->{$owner_name}->{'Total CSAT Surveys'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Total CSAT Surveys'}
          = 0;
    }

    # a manager's manager
    $cse_metrics{$manager}{'manager'} = 'tomdavi@cisco.com';

}

#----------------------------------------------------------------------
# output Team summary report email
#----------------------------------------------------------------------
my $tt = Template->new(
    {   INCLUDE_PATH => $tmpl_dir, 
        EVAL_PERL    => 1,
    }
) || die $Template::ERROR, "\n";

foreach my $cse_manager ( keys %cse_metrics ) {

    # Generate the body of report email 
    my $output;
    my %input_vars;

    %{$input_vars{'items'}} = %{$cse_metrics{$cse_manager}};
    #print Dumper($input_vars{'items'});

    $tt->process($report_tmpl, \%input_vars, \$output);

    my $digest = get_email($header_tmpl, $output, $footer_tmpl);

    # remove the metric - 'All CSATs' that is not involved in the statistics
    splice @{$cse_metrics{$cse_manager}{'SPI_metrics_order'}}, 6, 1;

    foreach my $cse ( keys %{$cse_metrics{$cse_manager}} ) {
        #clean up those keys which is unuseful for the excel
        next
        if (
            ( $cse eq "Extend_metrics_order" )
            || ( $cse eq "KPI_metrics_order" )
            || ( $cse eq "SPI_metrics_order" )
            || ( $cse eq "Extend" )
            || ( $cse eq "email" )
            || ( $cse eq "manager" )
            || ( $cse eq "name" )
            || ( $cse eq "month" ));

        delete($cse_metrics{$cse_manager}{$cse}{'SPI'}{'All CSATs'});
    }

    # create the excel file and add in the data and radial chart
    my $cse_team_month = \%{$cse_metrics{$cse_manager}};

    #To get the first row for whole format
    my @first_row = get_options_per_cse($cse_team_month);

    my @array_set;
    my $i = 0;
    foreach my $hash_key ( keys %{$cse_team_month} ) {

      #clean up those keys which is unuseful for the excel
      next
      if (
          ( $hash_key eq "Extend_metrics_order" )
          || ( $hash_key eq "KPI_metrics_order" )
          || ( $hash_key eq "SPI_metrics_order" )
          || ( $hash_key eq "Extend" )
          || ( $hash_key eq "email" )
          || ( $hash_key eq "manager" )
          || ( $hash_key eq "name" )
          || ( $hash_key eq "month" )
          );

      #To get the row one by one and store it into array format
      #call sub get one row of excel
      next if !exists($cse_team_month->{$hash_key}->{'KPI'});
      next if !exists($cse_team_month->{$hash_key}->{'SPI'});

      my @one_row
        = get_row_per_cse( $cse_team_month->{$hash_key}, \@first_row );
      $array_set[$i] = [@one_row];

      $i++;
      @one_row = ();
    }

    my @final_metrics = ( \@first_row, \@array_set );

    #calculate the result into hash format
    my @chart_data_set
      =calculate_radial_charts_data(\@final_metrics );

    # make a directory for per team
    my $charts_dir = $data_dir . $cse_manager . "/";
    mkdir "$charts_dir", 0777 unless -d "$charts_dir";

    #create the radial chart for each cse
    my @radail_chart_dir_set
      = plot_radial_charts($charts_dir, \@chart_data_set);

    sleep(5);

    #export metrics data to excel
    my $file_path = make_excel_for_each_cse(
                                            $charts_dir,
                                            $cse_team_month,
                                            \@final_metrics,
                                            \@radail_chart_dir_set
                                            );

    # Configure to, cc depends on the environment 
    my $to;
    if ($environment =~ /development|test/i) {
        $to = $email_to;
    }
    elsif ( $environment =~ /production/i ) {
        my $manager_email = $cse_metrics{$cse_manager}{'email'};

        $to = get_report_to($manager_email, $report_dbh);
    }

    my $cc;
    if ( $environment =~ /development|test/i ) {
        $cc = '';
    }
    elsif ( $environment =~ /production/i ) {
        $cc = $cse_metrics{$cse_manager}{'manager'};
    }

    my $bcc;
    if ( $environment =~ /development|test/i ) {
        $bcc = '';
    }
    elsif ( $environment =~ /production/i ) {
        $bcc = $email_bcc;
    }
    #print $to . "|" . $cc . "|" . $bcc . "\n";

    my $subject
      = $month_name
        . " " . $subject_prefix
        . $cse_metrics{$cse_manager}{'name'}
        ;

    # output report email 
    email_results($email_from, $to, $cc, $subject, $digest, $file_path);

    #last;
}


#----------------------------------------------------------------------
# subordinates...
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Team CSAT Avg
#----------------------------------------------------------------------
sub csat_avg_Team {

    my ($month, $cses, $report_dbh, $csat_dbh) = @_;

    # figure out how many teams, and make relationships between
    # CSEs and their teams with the use of 'report.employees' table.
    my $sql = "SELECT
                 manager
               FROM
                 employees
               WHERE
                 SUBSTR(email, 1, LOCATE('\@', email) - 1) = ?";
    my $sth = $report_dbh->prepare($sql) or die $report_dbh->errstr;

    my $primary_email_sql = "SELECT
                               primary_email
                             FROM
                               employees
                             WHERE email = ?";
    my $primary_email_sth = $report_dbh->prepare($primary_email_sql)
      or die $report_dbh->errstr;

    my %cses_info = ();

    my @managers = ();
    foreach my $cse ( @{$cses} ) {

        $sth->execute($cse) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $manager = $rh->{'manager'};

            if ( $manager !~ /.+\@cisco\.com/ ) {
                $primary_email_sth->execute($manager)
                  or die $primary_email_sth->errstr;

                my $primary_email_rh = $primary_email_sth->fetchrow_hashref;
                if ($primary_email_rh) {
                    $manager = $primary_email_rh->{'primary_email'};
                }
            }

            $cses_info{$cse}{'manager'} = $manager;

            if ( !( grep{ $_ eq $manager } @managers ) ) {
                push @managers, $manager;
            }
        }
    }

    # figure out the sum of all surveys' CSAT results and the total number of
    # surveys for each team, then we can divide the former with the
    # later to get 'Team CSE CSAT Avg.'.
    #we can use a CSE's name to determine an survey belongs to which team.
    my $survey_sql
    = "
    SELECT
      owner,
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

    my $month_start = $month->{'start_weekend'};;
    my $month_end   = $month->{'stop_weekend'};

    $survey_sth->execute( $month_start, $month_end )
    or die $survey_sth->errstr;

    while( my $rh = $survey_sth->fetchrow_hashref ) {
        my $owner = $rh->{'owner'};
        my $csat  = $rh->{'csat'};

        $cses_info{$owner}{'value'} += $csat;
        $cses_info{$owner}{'count'} += 1;
    }

    my %csat_avg_Team = ();
    foreach my $manager ( @managers ) {

        my $csat_value = 0;
        my $csat_count = 0;
        foreach my $cse ( keys %cses_info ) {

            next if ( !exists $cses_info{$cse}{'manager'} );
            next if ( !exists $cses_info{$cse}{'value'} );
            next if ( !exists $cses_info{$cse}{'count'} );

            next if ( $cses_info{$cse}{'manager'} ne $manager );

            $csat_value += $cses_info{$cse}{'value'};
            $csat_count += $cses_info{$cse}{'count'};
        }

        my $avg_value = 0;
        if ( $csat_count != 0 ) {
            $avg_value = sprintf("%.2f", ($csat_value / $csat_count));
        }

        $csat_avg_Team{$manager} = $avg_value;
    }
    return \%csat_avg_Team;
}

#----------------------------------------------------------------------
# KB linking %
#----------------------------------------------------------------------
sub kb_linking_KPI {

    my $month      = shift;
    my $report_dbh = shift;
    my $rt_dbh     = shift;

    my $month_start = $month->{'start_weekend'};
    my $month_end   = $month->{'stop_weekend'};

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

    $kb_linked_sth->execute($month_start, $month_end)
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
          ) AS employee_name,
          case_number
        FROM
          case_details
        WHERE 1
          AND created >= ?
          AND created < ?
          AND status = 'resolved'";
    my $resolved_sth = $report_dbh->prepare($resolved_sql)
      or die $report_dbh->errstr;

    $resolved_sth->execute($month_start, $month_end)
      or die $resolved_sth->errstr;

    my %kb_linking_metric;
    while ( my $resolved_rh = $resolved_sth->fetchrow_hashref ) {

        my $owner         = $resolved_rh->{'employee_name'};
        my $ticket_number = $resolved_rh->{'case_number'};

        if ( grep {$ticket_number eq $_} @kb_tickets ) {
            $kb_linking_metric{$owner}{'KB linking %'} += 1;
        }
        else {
            $kb_linking_metric{$owner}{'KB linking %'} += 0;
        }
    }
    return \%kb_linking_metric;

}

#----------------------------------------------------------------------
# SET SQL_BIG_SELECTS=1
#----------------------------------------------------------------------
sub set_big_sql {

    my $dbh = shift;

    my $set_big_sql = "SET SQL_BIG_SELECTS=1";
    my $set_big_sth = $dbh->prepare($set_big_sql) or die $dbh->errstr;

    $set_big_sth->execute() or die $set_big_sth->errstr;

    $set_big_sth->finish();

}

#----------------------------------------------------------------------
# figure out which team report should be sent to which manager
# as well as his/her assistants
#----------------------------------------------------------------------
sub get_report_to {

    my $email = shift;
    my $dbh   = shift;

    my $reportto = $email;

    # figure out a manager's pk depend on his/her email addr
    my $pk_sql = "SELECT id, pk FROM employees WHERE primary_email = ?";
    my $pk_sth = $dbh->prepare($pk_sql) or die $dbh->errstr;

    $pk_sth->execute($email) or die $pk_sth->errstr;
    my $pk_rh = $pk_sth->fetchrow_hashref;

    if( $pk_rh ) {
        my $pk = $pk_rh->{'pk'};

        # figure out a manager's assistants' pks
        my $metrics_sql
        = "
        SELECT
          metrics_secondary
        FROM
          metrics_reportto
        WHERE
          del <> '1'
          AND metrics_primary = ?";
        my $metrics_sth = $dbh->prepare($metrics_sql) or die $dbh->errstr;

        $metrics_sth->execute($pk) or die $metrics_sth->errstr;
        while( my $metrics_rh = $metrics_sth->fetchrow_hashref ) {
            my $metrics_secondary = $metrics_rh->{'metrics_secondary'};

            # figure out a manager's assistants' email addrs depend on their
            # pks
            my $reportto_sql
              = "SELECT primary_email FROM employees WHERE pk = ?";
            my $reportto_sth = $dbh->prepare($reportto_sql)
              or die $dbh->errstr;

            $reportto_sth->execute($metrics_secondary)
              or die $$reportto_sth->errstr;
            my $reportto_rh = $reportto_sth->fetchrow_hashref;

            if( $reportto_rh ) {
                my $email = $reportto_rh->{'primary_email'};

                $reportto .= ", " . $email;
            }
        }
    }

    return $reportto;
}

#----------------------------------------------------------------------
# get header of excel
#----------------------------------------------------------------------
sub get_options_per_cse {

 #contains whole format keys
 my $cse_team_month = shift;

 my @cse_kpi          = @{ $cse_team_month->{'KPI_metrics_order'} };
 my @cse_spi          = @{ $cse_team_month->{'SPI_metrics_order'} };
 my @cse_format_order = ( "CSE", "CSE alias", "Region", "Role", "Manager" );
 my @first_row        = ( @cse_format_order, @cse_kpi, @cse_spi );

 return @first_row;
}

#----------------------------------------------------------------------
# get one row of excel
#----------------------------------------------------------------------
sub get_row_per_cse {

 my $cse_month   = shift;
 my $first_row     = shift;

 my %kpi_spi_value = ( %{ $cse_month->{"KPI"} }, %{ $cse_month->{"SPI"} } );

 #delete $kpi_spi_value{"metrics_order"};
 my @return_array = (
  $cse_month->{"full_name"},
  $cse_month->{"name"},
  $cse_month->{"region"},
  $cse_month->{"role"},
  $cse_month->{"manager"}
 );

 my @first_row = @{$first_row};

 #making an order to cse kpi and spi
 for my $i ( 5 .. $#first_row ) {

  if ( !exists $kpi_spi_value{ $first_row[$i] } ) {
   $kpi_spi_value{ $first_row[$i] } = 0 ;
  }
  foreach my $status ( keys %kpi_spi_value ) {
   if ( $first_row[$i] eq $status ) {
    $kpi_spi_value{$status} = 0 if ( $kpi_spi_value{$status} eq "" );
    push @return_array, $kpi_spi_value{$status};
   }
  }

 }
 return @return_array;

}

#----------------------------------------------------------------------
# calculate the data in hash
#----------------------------------------------------------------------
sub calculate_radial_charts_data {
 my @record_set  = shift;
 my $offset      = 5;

 #print Dumper(@record_set);
 my @first_row   =
   @{ $record_set[0][0] }[ 5 .. scalar( @{ $record_set[0][0] } ) - 1 ];

 my @sub_record_set = @{ $record_set[0][1] };
 
 my @one_record_total = @{ $record_set[0][1][0] };

 my @total_array;
 for my $x ( $offset .. $#one_record_total ) {
  my $total = 0;
  for my $y ( 0 .. $#sub_record_set ) {
   $total = $total + $sub_record_set[$y][$x];
  }
  push @total_array, $total / ( $#sub_record_set + 1 );
 }

 my @per_cse_for_radial_chart;
 my @return_array;
 for my $i ( 0 .. $#sub_record_set ) {
  my @per_cse_average;
  my $cse_name;
  for my $j ( $offset .. $#one_record_total ) {
   my $singal_average;

   if ( $sub_record_set[$i][$j] == 0 ) {
    $singal_average = 0;
   }
   else {
    $singal_average
     = sprintf("%.2f",$sub_record_set[$i][$j] / $total_array[ $j - $offset ]);
   }
   #print $singal_average

   $cse_name = $sub_record_set[$i][0];
   push @per_cse_average, $singal_average;
  }

  $per_cse_for_radial_chart[0] = $cse_name;
  $per_cse_for_radial_chart[1] = [@first_row];
  $per_cse_for_radial_chart[2] = [@per_cse_average];

  # if values of somebody's metrics are all zero, then ignore this guy's radial
  # chart.
  my $sum = 0;
  foreach my $t ( @per_cse_average ) {
    $sum += $t;
  }
  if ( $sum != 0 ) {
    push @return_array, [@per_cse_for_radial_chart];
  }

 }
 return @return_array;

}

#----------------------------------------------------------------------
# create a radial charts according to input array set
#----------------------------------------------------------------------
sub plot_radial_charts {
 my $charts_dir = shift;
 my @chart_data = shift;

 my @real_chart_set = @{$chart_data[0]};
 my @dir_set;
 #print Dumper(@real_chart_set);
 for my $z ( 0 .. $#real_chart_set ) {
  my $width  = 50;
  my $height = 50;
  my $chart  = GD::Chart::Radial->new( $width, $height );
  $chart->set( 
   title => $real_chart_set[$z][0], 
   style         => 'Polygon',
   colours       => [qw/white black red/]
  );
  shift(@{$real_chart_set[$z]});
  #print Dumper($real_chart_set[$z]);
  $chart->plot($real_chart_set[$z]);
  
  my $chart_dir = $charts_dir . $z.".png";
  open IMG, ">".$chart_dir or die $!;
  binmode IMG;
  print IMG $chart->png;
  close IMG;
  push @dir_set, $chart_dir;
 }
  return @dir_set;
}

#----------------------------------------------------------------------
# make excel attachment for per cse monthly report
#----------------------------------------------------------------------

sub make_excel_for_each_cse {

 my $charts_dir           = shift;
 my $cse_month          = shift;
 my @record_set           = shift;
 my $radail_chart_dir_set = shift;
 #print Dumper(@radail_chart_dir_set);
 #print $#radail_chart_dir_set;

 my @radail_chart_dir_set = @{$radail_chart_dir_set};
 my $file_name = $charts_dir . 'team_monthly_report.xls';

 my $per_cse_monthly_report = Spreadsheet::WriteExcel->new($file_name);
 my $monthly_report         =
   $per_cse_monthly_report->add_worksheet( $cse_month->{"month"} );

 my $format = $per_cse_monthly_report->add_format();
 $format->set_bold();

 #print Dumper($record_set[0][1]);
 $monthly_report->write_row( 0, 0, $record_set[0][0], $format);
 my @sub_record_set = @{ $record_set[0][1] };

 #write the real data to the excel
 for my $i ( 0 .. $#sub_record_set ) {
  $monthly_report->write_row( $i + 1, 0, $sub_record_set[$i] );
 }

 #Write the formula to get the averager number
 my $average_position_row = $#sub_record_set + 2;
 my $average_position_col = 4;
 $monthly_report->write( $average_position_row, $average_position_col,
  "Average:", $format);

#print '=SUM('.number_to_char($average_position_col+$i+2).'2:'
#.number_to_char($average_position_col+$i+2)
#.($#sub_record_set+1).')/'.($#sub_record_set+1);
 for my $i ( 0 .. 13 ) {
  $monthly_report->write_formula(
   $average_position_row,
   $average_position_col + $i + 1,
   '=SUM('
     . number_to_char( $average_position_col + $i + 2 ) . '2:'
     . number_to_char( $average_position_col + $i + 2 )
     . ( $#sub_record_set + 2 ) . ')/'
     . ( $#sub_record_set + 1 ),
    $format
  );
 }

 $monthly_report->write( $average_position_row + 3, 0, "Radials", $format);
 $monthly_report->write(
  $average_position_row + 3,
  1,
"All stats are for individual CSEs compared to the average for their team"
. " within that region. 1 is equal to the average."
 );
 $monthly_report->write(
                          $average_position_row + 4,
                          1,
                          [ @{ $record_set[0][0] }[ 5 .. scalar( @{ $record_set[
0][0] } ) - 1 ] ],
                          $format
                          );

 my $formula_col = 6;
 for my $i ( 0 .. $#sub_record_set ) {
  $monthly_report->write( $average_position_row + 5 + $i,
   0, $sub_record_set[$i][0] );
  for my $j ( 0 .. 13 ) {
   $monthly_report->write_formula(
    $average_position_row + 5 + $i,
    $j + 1,
    "=IF(".( number_to_char( $formula_col + $j ) ) . ($#sub_record_set + 3)
        . "=0,0,"
      . ( number_to_char( $formula_col + $j ) )
      . ( $i + 2 ) . "/"
      . ( number_to_char( $formula_col + $j ) )
      . ($#sub_record_set + 3) . ")"
   );
  }
 }

 my $start_radial_chart_row = ($#sub_record_set*2)+9;
 my $start_radial_chart_col = 0;

 for my $t(0..$#radail_chart_dir_set) {

#  if ($t==1) {
#    $start_radial_chart_col = $start_radial_chart_col + 10;
#  }
  if ($t%2==0 && $t!=0) {
    $start_radial_chart_row = $start_radial_chart_row + 16;
    $start_radial_chart_col = $start_radial_chart_col - 10;
  }
  if ($t%2!=0 && $t!=0) {
    $start_radial_chart_col = $start_radial_chart_col + 10;
  }
#  print "Row is eq".$start_radial_chart_row."\n";
#  print "col is eq".$start_radial_chart_col."\n";
  #print $radail_chart_dir_set[$t];
  $monthly_report -> insert_image(
                                    $start_radial_chart_row,
                                    $start_radial_chart_col,
                                    $radail_chart_dir_set[$t]
                                    );
 }
 
 $per_cse_monthly_report->close();
 
 #sleep(5);

#--------------Try to use read lib to parse it---------------------
# my $ref = ReadData ($file_name);
#
# print Dumper($ref->[1]{C14});
#--------------Try to use read lib to parse it---------------------
#
#------------------Try to use spreadsheet parse to parser it--------------------
#my $team_monthly_report = Spreadsheet::ParseExcel->new();
#my $workbook = $team_monthly_report -> Parse($file_name);
#
#for my $worksheet ( $workbook->worksheets() ) {
##    print Dumper($worksheet);
 #    my ( $row_min, $row_max ) = $worksheet->row_range();
 #    my ( $col_min, $col_max ) = $worksheet->col_range();
 #
 #    for my $row ( $row_max-$#sub_record_set .. $row_max ) {
 #        for my $col ( $col_min .. $col_max-4 ) {
 #            my $cell = $worksheet->get_cell( $row, $col );
 #            next unless $cell;
 #
 #            print "Row, Col    = ($row, $col)\n";
 #            print "Value       = ", $cell->value(),       "\n";
 #            print "Unformatted = ", $cell->unformatted(), "\n";
 #            print "\n";
 #
 #        }
 #    }
 #
 #    print $row_max."\n";
 #    print $col_max."\n";

 #}

 #print Dumper($workbook);

 #print Dumper($monthly_report -> print_area(14,0,20,16));

return $file_name;
}

#----------------------------------------------------------------------
# To change the input number to alphabet
#----------------------------------------------------------------------
sub number_to_char {
 my $number = shift;

 my $number_to_char = {
  "1"  => "A",
  "2"  => "B",
  "3"  => "C",
  "4"  => "D",
  "5"  => "E",
  "6"  => "F",
  "7"  => "G",
  "8"  => "H",
  "9"  => "I",
  "10" => "J",
  "11" => "K",
  "12" => "L",
  "13" => "M",
  "14" => "N",
  "15" => "O",
  "16" => "P",
  "17" => "Q",
  "18" => "R",
  "19" => "S",
  "20" => "T",
  "21" => "U",
  "22" => "V",
  "23" => "W",
  "24" => "X",
  "25" => "Y",
  "26" => "Z"
 };

 foreach my $hash_k ( keys %{$number_to_char} ) {
  if ( $hash_k eq $number ) {
   return $number_to_char->{$hash_k};
  }
 }
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

    my ($from, $to, $cc, $subject, $html, $file_path) = @_;

    my %mail_config = (
        'reply_to'   => $from,
        'from'       => $from,
        'to'         => $to, 
        'cc'         => $cc,
        'bcc'        => '',
        'subject'    => $subject, 
        'text'       => '',
        'html'       => $html,
        'attachment' => $file_path,
    );

    my ( $stat, $err ) = SendMail::multi_mail_attachment( \%mail_config );
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
# calculate start weekend and stop weekend of the report
#----------------------------------------------------------------------
sub report_start_end_weekend {

    my ( $weeks, $time ) = @_;

    my $start_weekend;
    my $stop_weekend;

    $stop_weekend  = last_sunday_datetime($time);
    $start_weekend = previous_sunday_datetime($time, $weeks); 

    my %weekend_hash = ( 
        'start_weekend' => $start_weekend,
        'stop_weekend'  => $stop_weekend,
    );


    return \%weekend_hash; 
}

#----------------------------------------------------------------------
# usage
#----------------------------------------------------------------------
sub usage {

    print << "EOP";

  USAGE:
    $0 -r -e < environment >

  DESCRIPTION:
    this script is used to send metrics report for team summary monthly.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/monthly_report_metrics_team_summary_development.conf
      ../conf/monthly_report_metrics_team_summary_test.conf
      ../conf/monthly_report_metrics_team_summary_production.conf

  Examples:
  $0 -r -e development

EOP
}