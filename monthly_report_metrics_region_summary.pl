#!/usr/bin/perl
use strict;
use Getopt::Std;
use Data::Dumper;
use Template;
use Spreadsheet::WriteExcel;

use lib '../../../../utils/lib';
use MyDB;
use FileUtils;
use SendMail;
use ParseConfig;

use lib '../lib';
use GetMetricsData_Region;

#----------------------------------------------------------------------
# check options -r, -e <environment> and read  config vars from *.conf
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
        => '../conf/monthly_report_metrics_region_summary_development.conf',
    'test'
        => '../conf/monthly_report_metrics_region_summary_test.conf',
    'production'
        => '../conf/monthly_report_metrics_region_summary_production.conf',
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

# data directory
my $data_dir = $config_vars_rh->{'data_dir'};
die "data_dir not defined" unless ( defined $data_dir );

# sql file path to pull CSEs and regions
my $cses_sql_file    = $config_vars_rh->{'cses_sql_path'};
my $regions_sql_file = $config_vars_rh->{'regions_sql_path'};

die "cses_sql_path not defined"    unless ( defined $cses_sql_file );
die "regions_sql_path not defined" unless ( defined $regions_sql_file );

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
my $email_cc       = $config_vars_rh->{'cc'};

die "subject_prefix not subject" unless ( defined $subject_prefix );
die "from not defined"           unless ( defined $email_from );
die "to not defined"             unless ( defined $email_to );
die "cc not defined"             unless ( defined $email_cc );

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

# KPI - New Tickets
my $new_tickets_KPI
  = GetMetricsData_Region::main(
                                'New Tickets',
                                $new_tickets_sql_file,
                                $report_dbh,
                                $month
                                );

# KPI - Tickets Touched
my $tickets_touched_KPI
  = GetMetricsData_Region::main(
                                'Tickets Touched',
                                $tickets_touched_sql_file,
                                $report_dbh,
                                $month
                                );

# KPI - Tickets Resolved
my $tickets_resolved_KPI
  = GetMetricsData_Region::main(
                                'Tickets Resolved',
                                $tickets_resolved_sql_file,
                                $report_dbh,
                                $month
                                );

# KPI - CSAT Avg
my $csat_avg_KPI
  = GetMetricsData_Region::main(
                                'CSAT Avg',
                                $csat_avg_sql_file,
                                $csat_dbh,
                                $month
                                );

# KPI - New KB articles
my $new_kb_articles_KPI
  = GetMetricsData_Region::main(
                                'New KB articles',
                                $new_kb_articles_sql_file,
                                $iKbase_dbh,
                                $month
                                );

# KPI - KB linking %
my $kb_linking_KPI
  = kb_linking_KPI($rt_dbh, $report_dbh, $month);

# SPI - Avg Ticket Resolution Time
my $avg_resolution_SPI
  = GetMetricsData_Region::main(
                                'Avg Ticket Resolution Time',
                                $avg_resolution_sql_file,
                                $report_dbh,
                                $month
                                );

# SPI - Avg Interactions Per Ticket
my $avg_interactions_per_ticket_SPI
  = GetMetricsData_Region::main(
                                'Avg Interactions Per Ticket',
                                $avg_interactions_per_ticket_sql_file,
                                $report_dbh,
                                $month
                                );

# SPI - P1/P2 Tickets
my $p1_p2_tickets_SPI
  = GetMetricsData_Region::main(
                                'P1/P2 Tickets',
                                $p1_p2_tickets_sql_file,
                                $report_dbh,
                                $month
                                );

# SPI - Management Escalated Tickets
my $management_escalated_tickets_SPI
  = GetMetricsData_Region::main(
                                'Management Escalated Tickets',
                                $management_escalated_tickets_sql_file,
                                $rt_dbh,
                                $month
                                );

# SPI - Low CSATs (1-2) on CSE questions
my $low_cast_questions_SPI
  = GetMetricsData_Region::main(
                                'Low CSATs',
                                $low_cast_questions_sql_file,
                                $csat_dbh,
                                $month
                                );

# SPI - High CSATs (4-5) on CSE questions
my $high_csat_questions_SPI
  = GetMetricsData_Region::main(
                                'High CSATs',
                                $high_csat_questions_sql_file,
                                $csat_dbh,
                                $month
                                );

# SPI - All CSATs (1-5) on CSE questions
my $all_csat_questions_SPI
  = GetMetricsData_Region::main(
                                'All CSATs',
                                $all_csat_questions_sql_file,
                                $csat_dbh,
                                $month
                                );

# SPI - Total CSAT Surveys
my $total_cast_surveys_SPI
  = GetMetricsData_Region::main(
                                'Total CSAT Surveys',
                                $total_cast_surveys_sql_file,
                                $csat_dbh,
                                $month
                                );

#----------------------------------------------------------------------
# use employees table to filter metric hashes and combine them into a 
# new hash with employees' ownername as the key
#----------------------------------------------------------------------
my @teams = (
    'ESA SMB',
    'ESA ENT',
    'WSA',
    'IEA',
    'CRES',
    'Beta',
    'CSR',
              );

my @regions = (
    'APAC',
    'EMEA',
    'NA-EAST',
    'NA-WEST'
               );

my @KPI_metrics = (
        'New Tickets',
        'Tickets Touched',
        'Tickets Resolved',
        'CSAT Avg',
        'New KB articles',
        'KB linking %',
                         );

my @SPI_metrics = (
        'Avg Ticket Resolution Time',
        'Avg Interactions Per Ticket',
        'P1/P2 Tickets',
        'Management Escalated Tickets',
        'Low CSATs',
        'High CSATs',

        'All CSATs',

        'Total CSAT Surveys',
#        'Current Ticket Backlog',
#        'Average Ticket Backlog',
                         );

my @metrics = ( @KPI_metrics, @SPI_metrics );

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

my $employee_sth = $report_dbh->prepare($employee_sql)
  or die $report_dbh->errstr;

$employee_sth->execute() or die $employee_sth->errstr;

# the following loop consolidates the metrics' hashes that pulled out from
# various databases
my %cse_metrics;

while(my $employee_rh = $employee_sth->fetchrow_hashref) {

    my $employee = $employee_rh->{'owner'};

    foreach my $team ( @teams ) {

        if( exists $new_tickets_KPI->{$employee}->{'New Tickets'}->{$team} ) {
            $cse_metrics{$employee}{'New Tickets'}{$team}
              = $new_tickets_KPI->{$employee}->{'New Tickets'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'New Tickets'}{$team} = 0;
        }

        if ( exists $tickets_touched_KPI->{$employee}->{'Tickets Touched'}
->{$team} ) {
            $cse_metrics{$employee}{'Tickets Touched'}{$team}
              =  $tickets_touched_KPI->{$employee}->{'Tickets Touched'}
->{$team}
        }
        else {
            $cse_metrics{$employee}{'Tickets Touched'}{$team} = 0;
        }

        if ( exists $tickets_resolved_KPI->{$employee}->{'Tickets Resolved'}
->{$team} ) {
            $cse_metrics{$employee}{'Tickets Resolved'}{$team}
              = $tickets_resolved_KPI->{$employee}->{'Tickets Resolved'}
->{$team};
        }
        else {
            $cse_metrics{$employee}{'Tickets Resolved'}{$team} = 0;
        }

        if ( exists $csat_avg_KPI->{$employee}->{'CSAT Avg'}->{$team} ) {
            $cse_metrics{$employee}{'CSAT Avg'}{$team}
              = $csat_avg_KPI->{$employee}->{'CSAT Avg'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'CSAT Avg'}{$team} = 0;
        }

        if ( exists $new_kb_articles_KPI->{$employee}->{'New KB articles'}
->{$team} ) {
            $cse_metrics{$employee}{'New KB articles'}{$team}
              = $new_kb_articles_KPI->{$employee}->{'New KB articles'}
->{$team};
        }
        else {
            $cse_metrics{$employee}{'New KB articles'}{$team} = 0;
        }

        if ( exists $kb_linking_KPI->{$employee}->{'KB linking %'}->{$team} )
        {
            $cse_metrics{$employee}{'KB linking %'}{$team}
              = $kb_linking_KPI->{$employee}->{'KB linking %'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'KB linking %'}{$team} = 0;
        }

        if ( exists $avg_resolution_SPI->{$employee}
->{'Avg Ticket Resolution Time'}->{$team} ) {
            $cse_metrics{$employee}{'Avg Ticket Resolution Time'}{$team}
              = $avg_resolution_SPI->{$employee}->{'Avg Ticket Resolution Time'}
->{$team};
        }
        else {
            $cse_metrics{$employee}{'Avg Ticket Resolution Time'}{$team} = 0;
        }

        if ( exists $avg_interactions_per_ticket_SPI->{$employee}
->{'Avg Interactions Per Ticket'}->{$team} ) {
            $cse_metrics{$employee}{'Avg Interactions Per Ticket'}{$team}
              = $avg_interactions_per_ticket_SPI->{$employee}
->{'Avg Interactions Per Ticket'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'Avg Interactions Per Ticket'}{$team} = 0;
        }

        if ( exists $p1_p2_tickets_SPI->{$employee}->{'P1/P2 Tickets'}
->{$team} ) {
            $cse_metrics{$employee}{'P1/P2 Tickets'}{$team}
              = $p1_p2_tickets_SPI->{$employee}->{'P1/P2 Tickets'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'P1/P2 Tickets'}{$team} = 0;
        }

        if ( exists $management_escalated_tickets_SPI->{$employee}
->{'Management Escalated Tickets'}->{$team} ) {
            $cse_metrics{$employee}{'Management Escalated Tickets'}{$team}
              = $management_escalated_tickets_SPI->{$employee}
->{'Management Escalated Tickets'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'Management Escalated Tickets'}{$team} = 0;
        }

        if ( exists $low_cast_questions_SPI->{$employee}->{'Low CSATs'}
->{$team} ) {
            $cse_metrics{$employee}{'Low CSATs'}{$team}
              = $low_cast_questions_SPI->{$employee}->{'Low CSATs'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'Low CSATs'}{$team} = 0;
        }

        if ( exists $high_csat_questions_SPI->{$employee}->{'High CSATs'}
->{$team} ) {
            $cse_metrics{$employee}{'High CSATs'}{$team}
              = $high_csat_questions_SPI->{$employee}->{'High CSATs'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'High CSATs'}{$team} = 0;
        }

        if ( exists $all_csat_questions_SPI->{$employee}->{'All CSATs'}
->{$team} ) {
            $cse_metrics{$employee}{'All CSATs'}{$team}
              = $all_csat_questions_SPI->{$employee}->{'All CSATs'}->{$team};
        }
        else {
            $cse_metrics{$employee}{'All CSATs'}{$team} = 0;
        }

        if ( exists $total_cast_surveys_SPI->{$employee}->{'Total CSAT Surveys'}
->{$team} ) {
            $cse_metrics{$employee}{'Total CSAT Surveys'}{$team}
              = $total_cast_surveys_SPI->{$employee}->{'Total CSAT Surveys'}
->{$team};
        }
        else {
            $cse_metrics{$employee}{'Total CSAT Surveys'}{$team} = 0;
        }

    }
}


# the following loops adjust the struct of the hash to this style:
# 'WSA' => 'jahasan' => 'New Tickets'
my %new_cse_metrics;

foreach my $cse ( keys %cse_metrics ) {

    foreach my $metric ( @metrics ) {

        foreach my $team ( @teams ) {

            if ( exists $cse_metrics{$cse}{$metric}{$team} ) {
                $new_cse_metrics{$team}{$cse}{$metric}
                  = $cse_metrics{$cse}{$metric}{$team};
            }
        }
    }
}
#print Dumper(%new_cse_metrics);

# add region into the hash, and adjust the struct of the hash to this style:
# 'WSA' => 'APAC' => 'jahasan' => 'New Tickets'

my ( $stat, $err ) = FileUtils::file2string(
    {   file             => $regions_sql_file,
        comment_flag     => 1,
        blank_lines_flag => 1
    }
);
if ( !$stat ) {
    email_errors($err);
    die $err;
}
my $cse_region_sql = $err;

my $cse_region_sth = $report_dbh->prepare($cse_region_sql)
  or die $report_dbh->errstr;

$cse_region_sth->execute() or die $cse_region_sth->errstr;

my %final_cse_metrics;

while ( my $cse_region_rh = $cse_region_sth->fetchrow_hashref ) {

    my $owner  = $cse_region_rh->{'owner'};
    my $region = $cse_region_rh->{'region'};

    foreach my $team ( @teams) {

        if ( exists $new_cse_metrics{$team}{$owner} ) {
            #$final_cse_metrics{$team}{$region}{$owner}
            #  = $new_cse_metrics{$team}{$owner};

            foreach my $metric ( @metrics ) {

                if ( exists $new_cse_metrics{$team}{$owner}{$metric} ) {
                    $final_cse_metrics{$team}{$region}{$metric}
                      += $new_cse_metrics{$team}{$owner}{$metric};
                }
                else {
                    $final_cse_metrics{$team}{$region}{$metric} = 0;
                }
            }
        }
    }
}


# calculate the value of KB Linking %, and output the final hash
foreach my $team ( keys %final_cse_metrics ) {

    foreach my $region ( @regions ) {

        if ( (exists $final_cse_metrics{$team}{$region}{'KB linking %'})
               && (exists $final_cse_metrics{$team}{$region}
{'Tickets Resolved'}) ) {

            my $kb_linking = $final_cse_metrics{$team}{$region}{'KB linking %'};
            my $tickets_resolved
              = $final_cse_metrics{$team}{$region}{'Tickets Resolved'};

            my $kb_linking_percent = 0;
            if ( $tickets_resolved != 0 ) {
                $kb_linking_percent = $kb_linking / $tickets_resolved;
            }

            if ( $kb_linking_percent != 0 ) {
                $kb_linking_percent = sprintf("%.2f", $kb_linking_percent);
            }

            $final_cse_metrics{$team}{$region}{'KB linking %'}
              = $kb_linking_percent;
        }
        else {
            $final_cse_metrics{$team}{$region}{'KB linking %'} = 0;
        }

    }

    @{$final_cse_metrics{$team}{'KPI_metrics_order'}} = @KPI_metrics;
    @{$final_cse_metrics{$team}{'SPI_metrics_order'}} = @SPI_metrics;
}

$final_cse_metrics{'month'} = $month_name;
@{$final_cse_metrics{'teams_order'}} = @teams;

#print Dumper(%final_cse_metrics);

#----------------------------------------------------------------------
# output Region summary report email
#----------------------------------------------------------------------
my $tt = Template->new(
    {   INCLUDE_PATH => $tmpl_dir, 
        EVAL_PERL    => 1,
    }
) || die $Template::ERROR, "\n";

# Generate the body of report email 
my $output;
my %input_vars;

%{$input_vars{'items'}} = %final_cse_metrics;
#print Dumper($input_vars{'items'});
$tt->process($report_tmpl, \%input_vars, \$output);

my $digest    = get_email($header_tmpl, $output, $footer_tmpl);

# attachment
my $file_path = get_excel_attachment($month_name, \%input_vars);

# Configure to, cc depends on the environment 
my $to;
if ($environment =~ /development|test/i) {
    $to = $email_cc;
}
elsif ( $environment =~ /production/i ) {
    $to = $email_to;
}
my $cc = '';

my $subject = $month_name . " " . $subject_prefix;

# output report email 
email_results($email_from, $to, $cc, $subject, $digest, $file_path);


#----------------------------------------------------------------------
# subordinates...
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# KB linking %
#----------------------------------------------------------------------
sub kb_linking_KPI {

    my $rt_dbh     = shift;
    my $report_dbh = shift;
    my $month      = shift;

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
          ) AS owner,
          (
          CASE queue
            WHEN 'ESA SMB Support' THEN 'ESA SMB'
            WHEN 'ESA ENT Support' THEN 'ESA ENT'
            WHEN 'WSA Support' THEN 'WSA'
            WHEN 'Encryption' THEN 'IEA'
            WHEN 'CRES Support' THEN 'CRES'
            WHEN 'IronPort Beta' THEN 'Beta'
            WHEN 'CSR Support' THEN 'CSR'
          ELSE 'NULL'
          END
          ) AS team,
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

        my $owner         = $resolved_rh->{'owner'};
        my $ticket_number = $resolved_rh->{'case_number'};

        my $team          = $resolved_rh->{'team'};
        next if $team eq 'NULL';

        if ( grep {$ticket_number eq $_} @kb_tickets ) {
            $kb_linking_metric{$owner}{'KB linking %'}{$team} += 1;
        }
        else {
            $kb_linking_metric{$owner}{'KB linking %'}{$team} += 0;
        }

    }

    return \%kb_linking_metric;
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
# get *.xls file as attachment for the report
#----------------------------------------------------------------------
sub get_excel_attachment {

    my ($month_name, $metrics_data) = @_;

    mkdir "$data_dir", 0777 unless -d "$data_dir";

    my $file_path = $data_dir . $month_name
                    . " Region Performance Report.xls"
                    ;

    my $workbook = Spreadsheet::WriteExcel->new($file_path);
    my $worksheet = $workbook->add_worksheet();

    # the font format of team, such as "WSA"
    my $format_team = $workbook->add_format();
    $format_team->set_bold();
    $format_team->set_font('Times New Roman');
    $format_team->set_size(20);

    # the font format of category, such as "Key Performance Indicators (KPI's)"
    my $format_cate = $workbook->add_format();
    $format_cate->set_bold();
    $format_team->set_font('Times New Roman');
    $format_cate->set_size(15);

    # the font format of metric, such as "New Tickets"
    my $format_metrics = $workbook->add_format();
    $format_metrics->set_bold();
    $format_team->set_font('Times New Roman');
    $format_metrics->set_size(10);

    my $row = 0;

    foreach my $team ( @{$metrics_data->{'items'}->{'teams_order'}} ) {

        my $col = 0;

        $worksheet->write($row, $col, $team, $format_team);

        ## write the metrics belong to Key Performance Indicators (KPI's) into
        # this *.xls
        $row++;
        $worksheet->write($row,
                          $col,
                          "Key Performance Indicators (KPI's)",
                          $format_cate
                          );

        # metric title
        $row++;
        $worksheet->write($row, $col, "Region", $format_metrics);

        my $kpi_title_col = $col + 1;
        foreach my $metric ( @{$metrics_data->{'items'}->{$team}
->{'KPI_metrics_order'}} ) {
            $worksheet->write($row, $kpi_title_col, $metric, $format_metrics);
            $kpi_title_col++;
        }

        # metric value
        $row++;
        foreach my $region ( @regions ) {

            $worksheet->write($row, $col, $region);

            my $kpi_data_col = $col + 1;
            foreach my $metric ( @{$metrics_data->{'items'}->{$team}
->{'KPI_metrics_order'}} ) {
                $worksheet->write($row,
                                  $kpi_data_col,
                                  $metrics_data->{'items'}->{$team}->{$region}
->{$metric}
                                  );
                $kpi_data_col++;
            }
            $row++;
        }

        ## write the metrics belong to Supplemental Performance Indicators
        # (SPI's) into this *.xls
        $row++;
        $worksheet->write($row,
                          $col,
                          "Supplemental Performance Indicators (SPI's)",
                          $format_cate
                          );

        # metric title
        $row++;
        $worksheet->write($row, $col, "Region", $format_metrics);

        my $spi_title_col = $col + 1;
        foreach my $metric ( @{$metrics_data->{'items'}->{$team}
->{'SPI_metrics_order'}} ) {
            $worksheet->write($row, $spi_title_col, $metric, $format_metrics);
            $spi_title_col++;
        }

        # metric value
        $row++;
        foreach my $region ( @regions ) {

            $worksheet->write($row, $col, $region);

            my $spi_data_col = $col + 1;
            foreach my $metric ( @{$metrics_data->{'items'}->{$team}
->{'SPI_metrics_order'}} ) {
                $worksheet->write($row,
                                  $spi_data_col,
                                  $metrics_data->{'items'}->{$team}->{$region}
->{$metric}
                                  );
                $spi_data_col++;
            }
            $row++;
        }

        $row += 2;

    }

    return $file_path;
}

#----------------------------------------------------------------------
# email out results
#----------------------------------------------------------------------
sub email_results {

    my ($from, $to, $cc, $subject, $html, $file_path) = @_;

    my %mail_config = (
        'reply_to' => $from,
        'from'     => $from,
        'to'       => $to, 
        'cc'       => $cc,
        'bcc'      => '',
        'subject'  => $subject, 
        'text'     => '',
        'html'     => $html,
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
    my $to       = $email_cc;
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
    this script is used to send metrics report for region summary monthly.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/monthly_report_metrics_region_summary_development.conf
      ../conf/monthly_report_metrics_region_summary_test.conf
      ../conf/monthly_report_metrics_region_summary_production.conf

  Examples:
  $0 -r -e development

EOP
}