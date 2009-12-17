#!/usr/bin/perl
use strict;
use Getopt::Std;
use Data::Dumper;
use Template;

use Spreadsheet::WriteExcel;

#use Spreadsheet::Read;
use GD::Chart::Radial;

#use Spreadsheet::ParseExcel;

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

# Cisco month1
my $month1_name  = $config_vars_rh->{'month1_name'};
my $month1_start = $config_vars_rh->{'month1_start'};
my $month1_end   = $config_vars_rh->{'month1_end'};

die "month1_name not defined"  unless ( defined $month1_name );
die "month1_start not defined" unless ( defined $month1_start );
die "month1_end not defined"   unless ( defined $month1_end );

# Cisco month2
my $month2_name  = $config_vars_rh->{'month2_name'};
my $month2_start = $config_vars_rh->{'month2_start'};
my $month2_end   = $config_vars_rh->{'month2_end'};

die "month2_name not defined"  unless ( defined $month2_name );
die "month2_start not defined" unless ( defined $month2_start );
die "month2_end not defined"   unless ( defined $month2_end );

# Cisco month3
my $month3_name  = $config_vars_rh->{'month3_name'};
my $month3_start = $config_vars_rh->{'month3_start'};
my $month3_end   = $config_vars_rh->{'month3_end'};

die "month3_name not defined"  unless ( defined $month3_name );
die "month3_start not defined" unless ( defined $month3_start );
die "month3_end not defined"   unless ( defined $month3_end );

# Cisco month4
my $month4_name  = $config_vars_rh->{'month4_name'};
my $month4_start = $config_vars_rh->{'month4_start'};
my $month4_end   = $config_vars_rh->{'month4_end'};

die "month4_name not defined"  unless ( defined $month4_name );
die "month4_start not defined" unless ( defined $month4_start );
die "month4_end not defined"   unless ( defined $month4_end );

my $cisco_months = {
                    $month1_name => [$month1_start, $month1_end],
                    $month2_name => [$month2_start, $month2_end],
                    $month3_name => [$month3_start, $month3_end],
                    $month4_name => [$month4_start, $month4_end],
                   };
$cisco_months->{'months_order'} = [
                                   $month1_name,
                                   $month2_name,
                                   $month3_name,
                                   $month4_name
                                   ];

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
my $email_cc       = $config_vars_rh->{'cc'};
my $email_bcc      = $config_vars_rh->{'bcc'};


die "subject_prefix not defined" unless ( defined $subject_prefix );
die "email_from not defined"     unless ( defined $email_from );
die "email_to not defined"       unless ( defined $email_to );
die "email_cc not defined"       unless ( defined $email_cc );
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
  = new_tickets_KPI($cisco_months, $report_dbh);

# KPI - Tickets Touched
my $tickets_touched_KPI
  = tickets_touched_KPI($cisco_months, $report_dbh);

# KPI - Tickets Resolved
my $tickets_resolved_KPI
  = tickets_resolved_KPI($cisco_months, $report_dbh);

# KPI - Tickets Reopened
my $tickets_reopened_KPI
  = tickets_reopened_KPI($cisco_months, $report_dbh);

# KPI - CSAT Avg
my $csat_avg_KPI
  = csat_avg_KPI($cisco_months, $csat_dbh);

# KPI - New KB articles
my $new_kb_articles_KPI
  = new_kb_articles_KPI($cisco_months, $iKbase_dbh);

# KPI - KB linking %
my $kb_linking_KPI
  = kb_linking_KPI($report_weekend, $rt_dbh, $report_dbh);

# SPI - Avg Ticket Resolution Time
my $avg_resolution_SPI
  = avg_resolution_SPI($report_weekend, $report_dbh);

# SPI - Avg Interactions Per Ticket
my $avg_interactions_per_ticket_SPI
  = avg_interactions_per_ticket_SPI($report_weekend, $report_dbh);

# SPI - P1/P2 Tickets
my $p1_p2_tickets_SPI
  = p1_p2_tickets_SPI($report_weekend, $report_dbh);

# SPI - Management Escalated Tickets
my $management_escalated_tickets_SPI
  = management_escalated_tickets_SPI($report_weekend, $rt_dbh);

# SPI - Low CSATs (1-2) on CSE questions
my $low_cast_questions_SPI
  = low_cast_questions_SPI($report_weekend, $csat_dbh);

# SPI - High CSATs (4-5) on CSE questions
my $high_csat_questions_SPI
  = high_csat_questions_SPI($report_weekend, $csat_dbh);

# SPI - All CSATs (1-5) on CSE questions
my $all_csat_questions_SPI
  = all_csat_questions_SPI($report_weekend, $csat_dbh);

# SPI - Total CSAT Surveys
my $total_cast_surveys_SPI
  = total_cast_surveys_SPI($report_weekend, $csat_dbh);

# SPI - Current Ticket Backlog
#my $current_ticket_backlog_SPI
#  = current_ticket_backlog_SPI($report_weekend, $report_dbh);

# SPI - Average Ticket Backlog
#my $average_ticket_backlog_SPI
#  = average_ticket_backlog_SPI($report_weekend, $weeks, $rt_dbh);

#----------------------------------------------------------------------
# use employees table to filter metric hashes and combine them into a 
# new hash with employees' ownername as the key 
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
    my $manager        = $employee_rh->{'manager_name'};
    my $manager_email  = $employee_rh->{'manager'};

    # cse's full name
    my $owner_full_name = $first_name . " " . $last_name;
    $cse_metrics{$manager}{$owner_name}{'full_name'}    = $owner_full_name;

    # cse's name
    $cse_metrics{$manager}{$owner_name}{'name'}    = $owner_name;

    # cse's region
    $cse_metrics{$manager}{$owner_name}{'region'}    = $region;

    # cse's role
    $cse_metrics{$manager}{$owner_name}{'role'}    = $role;

    # cse's manager
    $cse_metrics{$manager}{$owner_name}{'manager'}    = $manager;

    # cse's email
    $cse_metrics{$manager}{$owner_name}{'email'}   = $employee_email;

    # manager's manager
    $cse_metrics{$manager}{'manager'} = 'tdavis@ironport.com';

    # manager's name
    $cse_metrics{$manager}{'name'} = $manager;

    # manager's email
    $cse_metrics{$manager}{'email'} = $manager_email;

    # quarter's name
    $cse_metrics{$manager}{'quarter'} = $quarter_name;

    # KPI Metrics' order
    @{$cse_metrics{$manager}{'KPI_metrics_order'}} = (
        'New Tickets',
        'Tickets Touched',
        'Tickets Resolved',
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
#        'Current Ticket Backlog',
#        'Average Ticket Backlog',
    );

    if ( exists $new_tickets_KPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New Tickets'}
          = $new_tickets_KPI->{$owner_name}->{'New Tickets'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New Tickets'} = 0;
    }

    if ( exists $tickets_touched_KPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Touched'}
          = $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Touched'} = 0;
    }

    if ( exists $tickets_resolved_KPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Resolved'}
          = $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'Tickets Resolved'} = 0;
    }

    if ( exists $csat_avg_KPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'CSAT Avg'}
          = $csat_avg_KPI->{$owner_name}->{'CSAT Avg'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'CSAT Avg'} = 0;
    }

    if ( exists $new_kb_articles_KPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New KB articles'}
          = $new_kb_articles_KPI->{$owner_name}->{'New KB articles'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'New KB articles'} = 0;
    }

    # Calculate KB Linking % for per CSE
    if ( ( exists $kb_linking_KPI->{$owner_name} )
           && ( exists $tickets_resolved_KPI->{$owner_name} ) ) {
        my $kb_linking
          = $kb_linking_KPI->{$owner_name}{'KB linking %'};
    
        my $tickets_resolved
          = $tickets_resolved_KPI->{$owner_name}{'Tickets Resolved'};

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

        $cse_metrics{$manager}{$owner_name}{'KPI'}{'KB linking %'}
          = $kb_linking_percent;
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'KPI'}{'KB linking %'} = 0;
    }

    if ( exists $avg_resolution_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
          = $avg_resolution_SPI->{$owner_name}->{'Avg Ticket Resolution Time'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
          = 0;
    }

    if ( exists $avg_interactions_per_ticket_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Avg Interactions Per Ticket'}
          = $avg_interactions_per_ticket_SPI->{$owner_name}->
{'Avg Interactions Per Ticket'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Avg Interactions Per Ticket'} = 0;
    }

    if ( exists $p1_p2_tickets_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'P1/P2 Tickets'}
          = $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'P1/P2 Tickets'} = 0;
    }

    if ( exists $management_escalated_tickets_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Management Escalated Tickets'}
          = $management_escalated_tickets_SPI->{$owner_name}->
{'Management Escalated Tickets'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}
{'Management Escalated Tickets'} = 0;
    }

    if ( exists $low_cast_questions_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Low CSATs'}
          = $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Low CSATs'} = 0;
    }

    if ( exists $high_csat_questions_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'High CSATs'}
          = $high_csat_questions_SPI->{$owner_name}->{'High CSATs'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'High CSATs'} = 0;
    }

    if ( exists $all_csat_questions_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'All CSATs'}
          = $all_csat_questions_SPI->{$owner_name}->{'All CSATs'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'All CSATs'} = 0;
    }

    if ( exists $total_cast_surveys_SPI->{$owner_name} ) {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Total CSAT Surveys'}
          = $total_cast_surveys_SPI->{$owner_name}->{'Total CSAT Surveys'};
    }
    else {
        $cse_metrics{$manager}{$owner_name}{'SPI'}{'Total CSAT Surveys'} = 0;
    }

#    if ( exists $current_ticket_backlog_SPI->{$owner_name} ) {
#        $cse_metrics{$manager}{$owner_name}{'SPI'}
#{'Current Ticket Backlog'}
#          = $current_ticket_backlog_SPI->{$owner_name}
#{'Current Ticket Backlog'};
#    }
#    else {
#        $cse_metrics{$manager}{$owner_name}{'SPI'}
#{'Current Ticket Backlog'} = 0;
#    }

#    if ( exists $average_ticket_backlog_SPI->{$owner_name} ) {
#        $cse_metrics{$manager}{$owner_name}{'SPI'}
#{'Average Ticket Backlog'}
#          = $average_ticket_backlog_SPI->{$owner_name}
#{'Average Ticket Backlog'};
#    }
#    else {
#        $cse_metrics{$manager}{$owner_name}{'SPI'}
#{'Average Ticket Backlog'} = 0;
#    }
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
        if (    ($cse eq "KPI_metrics_order" )
            || ( $cse eq "SPI_metrics_order" )
            || ( $cse eq "email" )
            || ( $cse eq "manager" )
            || ( $cse eq "name" )
            || ( $cse eq "quarter" ));

        delete($cse_metrics{$cse_manager}{$cse}{'SPI'}{'All CSATs'});
    }
    #print Dumper($cse_metrics{$cse_manager});

    # create the excel file and add in the data and radial chart
    my $cse_team_quarter = \%{$cse_metrics{$cse_manager}};

    #To get the first row for whole format
    my @first_row = get_options_per_cse($cse_team_quarter);

    my @array_set;
    my $i = 0;
    foreach my $hash_key ( keys %{$cse_team_quarter} ) {

      #clean up those keys which is unuseful for the excel
      next
      if (
          ($hash_key eq "KPI_metrics_order" )
          || ( $hash_key eq "SPI_metrics_order" )
          || ( $hash_key eq "email" )
          || ( $hash_key eq "manager" )
          || ( $hash_key eq "name" )
          || ( $hash_key eq "quarter" )
          );

      #To get the row one by one and store it into array format
      #call sub get one row of excel
      next if !exists($cse_team_quarter->{$hash_key}->{'KPI'});
      next if !exists($cse_team_quarter->{$hash_key}->{'SPI'});

      my @one_row
        = get_row_per_cse( $cse_team_quarter->{$hash_key}, \@first_row );
      $array_set[$i] = [@one_row];

      $i++;
      @one_row = ();
    }

    my @final_metrics = ( \@first_row, \@array_set );
    #print Dumper(@final_metrics);

    #calculate the result into hash format
    my @chart_data_set
      =calculate_radial_charts_data(\@final_metrics );
    #print Dumper(@chart_data_set);

    # make a directory for per team
    my $charts_dir = $data_dir . $cse_manager . "/";
    mkdir "$charts_dir", 0777 unless -d "$charts_dir";

    #create the radial chart for each cse
    #return an array that contains all the file
    my @radail_chart_dir_set
      = plot_radial_charts($charts_dir, \@chart_data_set);

    sleep(5);

    #export metrics data to excel
    my $file_path = make_excel_for_each_cse(
                                            $charts_dir,
                                            $cse_team_quarter,
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

    my $subject
      = $quarter_name
        . " " . $subject_prefix
        . $cse_metrics{$cse_manager}{'name'}
        ;

    # output report email 
    email_results($email_from, $to, $cc, $subject, $digest, $file_path);

    last;
}


#----------------------------------------------------------------------
# subordinates...
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# New Tickets
#----------------------------------------------------------------------
sub new_tickets_KPI {

    my $months = shift;
    my $dbh    = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', owner) = 0,
                   owner, LEFT(owner, LOCATE('\@', owner) - 1)
                 ) AS owner,
                 COUNT(*) AS total
               FROM
                 case_details
               WHERE 1
                 AND created >= ? 
                 AND created < ? 
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my %new_tickets_metric = ();
    foreach my $month ( keys %{$months} ) {

        next if ( $month eq 'months_order' );

        my $month_start = $months->{$month}[0] . " 00:00:00";
        my $month_end   = $months->{$month}[1] . " 00:00:00";

        $sth->execute($month_start, $month_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner       = $rh->{'owner'};
            my $total       = $rh->{'total'};

            $new_tickets_metric{$owner}{'New Tickets'}{$month} = $total;
        }

    }

    return \%new_tickets_metric;
}

#----------------------------------------------------------------------
# Tickets Touched
#----------------------------------------------------------------------
sub tickets_touched_KPI {

    my $months = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', cse.email) = 0,
                   cse.email, LEFT(cse.email, LOCATE('\@', cse.email) - 1)
                 ) AS owner,
                 COUNT(DISTINCT(Transactions.Ticket)) AS cases_touched 
               FROM 
                 employees cse
                   LEFT JOIN rt3.Transactions Transactions ON 
                     cse.id = Transactions.Creator 
               WHERE 
                 Transactions.Type IN ('Create', 'Correspond', 'Comment') 
                 AND Transactions.Created >= ?
                 AND Transactions.Created < ?
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my %tickets_touched_metric = ();
    foreach my $month ( keys %{$months} ) {

        next if ( $month eq 'months_order' );

        my $month_start = $months->{$month}[0] . " 00:00:00";
        my $month_end   = $months->{$month}[1] . " 00:00:00";

        $sth->execute($month_start, $month_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner         = $rh->{'owner'}; 
            my $cases_touched = $rh->{'cases_touched'};

            $tickets_touched_metric{$owner}{'Tickets Touched'}{$month}
              = $cases_touched;
        }

    }

    return \%tickets_touched_metric;
}

#----------------------------------------------------------------------
# Tickets Resolved
#----------------------------------------------------------------------
sub tickets_resolved_KPI {

    my $months = shift;
    my $dbh      = shift;

    my $sql
    = "
    SELECT
      IF(LOCATE('\@', owner) = 0,
        owner, LEFT(owner, LOCATE('\@', owner) - 1)
      ) AS owner,
      COUNT(case_number) AS tickets_resolved
    FROM
      case_details
    WHERE 1
      AND reso_timestamp >= ?
      AND reso_timestamp < ?
      AND status = 'resolved'
    GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my %tickets_resolved_metric;
    foreach my $month ( keys %{$months} ) {

        next if ( $month eq 'months_order' );

        my $month_start = $months->{$month}[0] . " 00:00:00";
        my $month_end   = $months->{$month}[1] . " 00:00:00";

        $sth->execute($month_start, $month_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner            = $rh->{'owner'};
            my $tickets_resolved = $rh->{'tickets_resolved'};

            $tickets_resolved_metric{$owner}{'Tickets Resolved'}{$month}
              = $tickets_resolved;
        }

    }

    return \%tickets_resolved_metric;
}

#----------------------------------------------------------------------
# Tickets Reopened
#----------------------------------------------------------------------
sub tickets_reopened_KPI {

    my $months = shift;
    my $dbh    = shift;

    my $sql
    = "
    SELECT
      IF(LOCATE('\@', cse.email) = 0,
        cse.email, LEFT(cse.email, LOCATE('\@', cse.email) - 1)
      ) AS owner,
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
      AND T_reopen.Created >= ?
      AND T_reopen.Created <  ?
    GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my %tickets_reopened_metric;
    foreach my $month ( keys %{$months}) {

        next if ( $month eq 'months_order' );

        my $month_start = $months->{$month}[0] . " 00:00:00";
        my $month_end   = $months->{$month}[1] . " 00:00:00";

        $sth->execute($month_start, $month_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner            = $rh->{'owner'};
            my $tickets_reopened = $rh->{'tickets_reopened'};

            if ( defined $tickets_reopened ) {
                $tickets_reopened_metric{$owner}{'Tickets Reopened'}{$month}
                  = $tickets_reopened;
            }
        }

    }

    return \%tickets_reopened_metric;
}

#----------------------------------------------------------------------
# CSAT Avg  
#----------------------------------------------------------------------
sub csat_avg_KPI {

    my $months = shift;
    my $dbh    = shift; 

    my $sql
    = "
    SELECT
      e.owner,
      ROUND(
      (SUM(e.csat) / SUM(e.number)), 2) AS csat
    FROM
        (
        SELECT
          owner,
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
    GROUP BY e.owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my %csat_avg_metric;
    foreach my $month ( keys %{$months}) {

        next if ( $month eq 'months_order' );

        my $month_start = $months->{$month}[0] . " 00:00:00";
        my $month_end   = $months->{$month}[1] . " 00:00:00";

        $sth->execute($month_start, $month_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner = $rh->{'owner'};
            my $csat  = $rh->{'csat'};

            $csat_avg_metric{$owner}{'CSAT Avg'}{$month} = $csat;
        }

    }

    return \%csat_avg_metric;
}

#----------------------------------------------------------------------
# New KB articles
#----------------------------------------------------------------------
sub new_kb_articles_KPI {

    my $months = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 u.name AS owner,
                 COUNT(*) AS total
               FROM
                 (  (
                    SELECT
                      articles.id AS article_id, 

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
               GROUP BY u.name";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    foreach my $month ( keys %{$months}) {

        next if ( $month eq 'months_order' );

        my $month_start = $months->{$month}[0] . " 00:00:00";
        my $month_end   = $months->{$month}[1] . " 00:00:00";

        $sth->execute($month_start, $month_end) or die $sth->errstr;

    }

    my %new_kb_articles_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner          = $rh->{'owner'};
        my $articles_total = $rh->{'total'};

        $new_kb_articles_metric{$owner}{'New KB articles'} = $articles_total;
    }

    return \%new_kb_articles_metric;
}

#----------------------------------------------------------------------
# KB linking %
#----------------------------------------------------------------------
sub kb_linking_KPI {

    my $weekends   = shift;
    my $rt_dbh     = shift;
    my $report_dbh = shift;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

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
# Avg Ticket Resolution Time (seconds) 
#----------------------------------------------------------------------
sub avg_resolution_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', owner) = 0,
                   owner, LEFT(owner, LOCATE('\@', owner) - 1)
                 ) AS owner,
                 ROUND(
                   (SUM(resolution_time) / COUNT(case_number)) / (60*60*24)
                 , 2) AS avg_ticket_resolution_time
               FROM
                 case_details
               WHERE 1
                 AND created >= ?
                 AND created < ?
                 AND status = 'resolved'
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %avg_resolution_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner       = $rh->{'owner'};
        my $avg_time    = $rh->{'avg_ticket_resolution_time'};

        $avg_resolution_metric{$owner}{'Avg Ticket Resolution Time'}
          = $avg_time;
    }

    return \%avg_resolution_metric;
}

#----------------------------------------------------------------------
# Avg Interactions Per Ticket
#----------------------------------------------------------------------
sub avg_interactions_per_ticket_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', cse) = 0,
                   cse, LEFT(cse, LOCATE('\@', cse) - 1)
                 ) AS owner,
                 ROUND(
                   SUM(interactions) / SUM(cases_touched)
                 , 2) AS avg_interactions
               FROM
                 interactions
               WHERE 1
                 AND week_ending > ?
                 AND week_ending <= ?
               GROUP BY cse";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};
    
    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %avg_interactions_metric;
    while( my $rh = $sth->fetchrow_hashref ) {
        my $owner            = $rh->{'owner'};
        my $avg_interactions = $rh->{'avg_interactions'};

        $avg_interactions_metric{$owner}{'Avg Interactions Per Ticket'}
          = $avg_interactions;
    }

    return \%avg_interactions_metric;

}

#----------------------------------------------------------------------
# P1/P2 Tickets
#----------------------------------------------------------------------
sub p1_p2_tickets_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', owner) = 0,
                   owner, LEFT(owner, LOCATE('\@', owner) - 1)
                 ) AS owner,
                 COUNT(*) AS p1_p2
               FROM
                 case_details
               WHERE 1
                 AND created >= ?
                 AND created < ?
                 AND (case_details.priority LIKE 'P1%'
                      OR case_details.priority LIKE 'P2%')
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %p1_p2_tickets_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner       = $rh->{'owner'};
        my $p1_p2       = $rh->{'p1_p2'};

        $p1_p2_tickets_metric{$owner}{'P1/P2 Tickets'} = $p1_p2;
    }

    return \%p1_p2_tickets_metric
}

#----------------------------------------------------------------------
# Management Escalated Tickets
#----------------------------------------------------------------------
sub management_escalated_tickets_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', u.Name) = 0,
                    u.Name,
                    LEFT(u.Name, LOCATE('\@', u.Name)-1)
                 ) AS owner,
                 COUNT(*) AS management_escalated_tickets
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
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %management_escalated_tickets_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner = $rh->{'owner'};
        my $management_escalated_tickets
          = $rh->{'management_escalated_tickets'};

        $management_escalated_tickets_metric{$owner}
{'Management Escalated Tickets'}
          = $management_escalated_tickets;
    }

    return \%management_escalated_tickets_metric
}

#----------------------------------------------------------------------
# Low CSATs (1-2) on CSE questions
#----------------------------------------------------------------------
sub low_cast_questions_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 owner,
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
                GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %low_cast_questions_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner          = $rh->{'owner'};
        my $total_low_csat = $rh->{'total_low_csat'};

        $low_cast_questions_metric{$owner}{'Low CSATs'} = $total_low_csat;
    }

    return \%low_cast_questions_metric
}

#----------------------------------------------------------------------
# High CSATs (4-5) on CSE questions
#----------------------------------------------------------------------
sub high_csat_questions_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 owner,
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
                GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %high_csat_questions_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner           = $rh->{'owner'};
        my $total_high_csat = $rh->{'total_high_csat'};

        $high_csat_questions_metric{$owner}{'High CSATs'} = $total_high_csat;
    }

    return \%high_csat_questions_metric
}

#----------------------------------------------------------------------
# All CSATs (1-5) on CSE questions
#----------------------------------------------------------------------
sub all_csat_questions_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 owner,
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
                GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %all_csat_questions_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner      = $rh->{'owner'};
        my $total_csat = $rh->{'total_csat'};

        $all_csat_questions_metric{$owner}{'All CSATs'} = $total_csat;
    }

    return \%all_csat_questions_metric
}

#----------------------------------------------------------------------
# Total CSAT Surveys
#----------------------------------------------------------------------
sub total_cast_surveys_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                owner,
                COUNT(*) AS total_csat_surveys
               FROM
                 survey
               WHERE
                 qp_ts >= ?
                 AND qp_ts < ?
                 AND owner<>''
                 AND _del != 'Y'
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %total_cast_surveys_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner              = $rh->{'owner'};
        my $total_csat_surveys = $rh->{'total_csat_surveys'};

        $total_cast_surveys_metric{$owner}{'Total CSAT Surveys'}
          = $total_csat_surveys;
    }

    return \%total_cast_surveys_metric
}

#----------------------------------------------------------------------
# Current Ticket Backlog
#----------------------------------------------------------------------
sub current_ticket_backlog_SPI {

    my $weekends = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 SUBSTR(owner, 1, LOCATE('\@', owner) - 1) AS owner,
                 COUNT(case_number) As current_backlog
               FROM
                 case_details
               WHERE
                 (status = 'open' OR status = 'stalled' OR status = 'new')
                 AND created >= ?
                 AND created < ?
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %current_ticket_backlog_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner           = $rh->{'owner'};
        my $current_backlog = $rh->{'current_backlog'};

        $current_ticket_backlog_metric{$owner}{'Current Ticket Backlog'}
          = $current_backlog;
    }

    return \%current_ticket_backlog_metric
}

#----------------------------------------------------------------------
# Average Ticket Backlog
#----------------------------------------------------------------------
sub average_ticket_backlog_SPI {

    my $weekends = shift;
    my $weeks    = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', u.Name) = 0,
                    u.Name,
                    LEFT(u.Name, LOCATE('\@', u.Name)-1)
                 ) AS owner,
                 ROUND(
                   (COUNT(t.id) / $weeks)
                 , 2) AS average_backlog_per_week
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
               GROUP BY owner";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my $report_start = $weekends->{'start_weekend'};
    my $report_stop  = $weekends->{'stop_weekend'};

    $sth->execute($report_start, $report_stop) or die $sth->errstr;

    my %average_ticket_backlog_metric;
    while(my $rh = $sth -> fetchrow_hashref) {
        my $owner                    = $rh->{'owner'};
        my $average_backlog_per_week = $rh->{'average_backlog_per_week'};

        $average_ticket_backlog_metric{$owner}{'Average Ticket Backlog'}
          = $average_backlog_per_week;
    }

    return \%average_ticket_backlog_metric
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
    my $pk_sql = "SELECT id, pk FROM employees WHERE email = ?";
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
            my $reportto_sql = "SELECT email FROM employees WHERE pk = ?";
            my $reportto_sth = $dbh->prepare($reportto_sql)
              or die $dbh->errstr;

            $reportto_sth->execute($metrics_secondary)
              or die $$reportto_sth->errstr;
            my $reportto_rh = $reportto_sth->fetchrow_hashref;

            if( $reportto_rh ) {
                my $email = $reportto_rh->{'email'};

                $reportto .= ", " . $email;
            }
        }
    }

    return $reportto;
}


#To import the radail chart to the excel document
#insert_the_radial_chart(\@radail_chart_dir_set,$file_path);
#----------------------------------------------------------------------
# get header of excel
#----------------------------------------------------------------------
sub get_options_per_cse {

 #contains whole format keys
 my $cse_team_quarter = shift;

 my @cse_kpi          = @{ $cse_team_quarter->{'KPI_metrics_order'} };
 my @cse_spi          = @{ $cse_team_quarter->{'SPI_metrics_order'} };
 my @cse_format_order = ( "CSE", "CSE alias", "Region", "Role", "Manager" );
 my @first_row        = ( @cse_format_order, @cse_kpi, @cse_spi );

 return @first_row;
}

#----------------------------------------------------------------------
# get one row of excel
#----------------------------------------------------------------------
sub get_row_per_cse {

 my $cse_quarter   = shift;
 my $first_row     = shift;

 my %kpi_spi_value = ( %{ $cse_quarter->{"KPI"} }, %{ $cse_quarter->{"SPI"} } );

 #delete $kpi_spi_value{"metrics_order"};
 my @return_array = (
  $cse_quarter->{"full_name"},
  $cse_quarter->{"name"},
  $cse_quarter->{"region"},
  $cse_quarter->{"role"},
  $cse_quarter->{"manager"}
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
# make excel attachment for per cse quarterly report
#----------------------------------------------------------------------

sub make_excel_for_each_cse {

 my $charts_dir           = shift;
 my $cse_quarter          = shift;
 my @record_set           = shift;
 my $radail_chart_dir_set = shift;
 #print Dumper(@radail_chart_dir_set);
 #print $#radail_chart_dir_set;

 my @radail_chart_dir_set = @{$radail_chart_dir_set};
 my $file_name = $charts_dir . 'team_quarterly_report.xls';

 my $per_cse_quarterly_report = Spreadsheet::WriteExcel->new($file_name);
 my $quarterly_report         =
   $per_cse_quarterly_report->add_worksheet( $cse_quarter->{"quarter"} );

 my $format = $per_cse_quarterly_report->add_format();
 $format->set_bold();

 #print Dumper($record_set[0][1]);
 $quarterly_report->write_row( 0, 0, $record_set[0][0], $format);
 my @sub_record_set = @{ $record_set[0][1] };

 #write the real data to the excel
 for my $i ( 0 .. $#sub_record_set ) {
  $quarterly_report->write_row( $i + 1, 0, $sub_record_set[$i] );
 }

 #Write the formula to get the averager number
 my $average_position_row = $#sub_record_set + 2;
 my $average_position_col = 4;
 $quarterly_report->write( $average_position_row, $average_position_col,
  "Average:", $format);

#print '=SUM('.number_to_char($average_position_col+$i+2).'2:'
#.number_to_char($average_position_col+$i+2)
#.($#sub_record_set+1).')/'.($#sub_record_set+1);
 for my $i ( 0 .. 12 ) {
  $quarterly_report->write_formula(
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

 $quarterly_report->write( $average_position_row + 3, 0, "Radials", $format);
 $quarterly_report->write(
  $average_position_row + 3,
  1,
"All stats are for individual CSEs compared to the average for their team"
. " within that region. 1 is equal to the average."
 );
 $quarterly_report->write(
                          $average_position_row + 4,
                          1,
                          [ @{ $record_set[0][0] }[ 5 .. scalar( @{ $record_set[
0][0] } ) - 1 ] ],
                          $format
                          );

 my $formula_col = 6;
 for my $i ( 0 .. $#sub_record_set ) {
  $quarterly_report->write( $average_position_row + 5 + $i,
   0, $sub_record_set[$i][0] );
  for my $j ( 0 .. 12 ) {
   $quarterly_report->write_formula(
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
  $quarterly_report -> insert_image(
                                    $start_radial_chart_row,
                                    $start_radial_chart_col,
                                    $radail_chart_dir_set[$t]
                                    );
 }
 
 $per_cse_quarterly_report->close();
 
 #sleep(5);

#--------------Try to use read lib to parse it---------------------
# my $ref = ReadData ($file_name);
#
# print Dumper($ref->[1]{C14});
#--------------Try to use read lib to parse it---------------------
#
#------------------Try to use spreadsheet parse to parser it--------------------
#my $team_quarterly_report = Spreadsheet::ParseExcel->new();
#my $workbook = $team_quarterly_report -> Parse($file_name);
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

 #print Dumper($quarterly_report -> print_area(14,0,20,16));

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
    this script is used to send metrics report for team summary quarterly.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/quarterly_report_metrics_team_summary_development.conf
      ../conf/quarterly_report_metrics_team_summary_test.conf
      ../conf/quarterly_report_metrics_team_summary_production.conf

  Examples:
  $0 -r -e development

EOP
}