#!/usr/bin/perl
use strict;
use Getopt::Std;
use Data::Dumper;
use Template;
use Spreadsheet::WriteExcel;

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
        =>'../conf/quarterly_report_metrics_per_cse_development.conf',
    'test'
        => '../conf/quarterly_report_metrics_per_cse_test.conf',
    'production'
        => '../conf/quarterly_report_metrics_per_cse_production.conf',
);

my $config_file = $env_opts{$environment};

my ($stat, $err) = ParseConfig::colon($config_file); 
if ( !$stat ) {
    die $err;
}
my $config_vars_rh = $err;

# Cisco quarter1
my $quarter1_name  = $config_vars_rh->{'quarter1_name'};
my $quarter1_start = $config_vars_rh->{'quarter1_start'};
my $quarter1_end   = $config_vars_rh->{'quarter1_end'};

die "quarter1_name not defined"  unless ( defined $quarter1_name );
die "quarter1_start not defined" unless ( defined $quarter1_start );
die "quarter1_end not defined"   unless ( defined $quarter1_end );

# Cisco quarter2
my $quarter2_name  = $config_vars_rh->{'quarter2_name'};
my $quarter2_start = $config_vars_rh->{'quarter2_start'};
my $quarter2_end   = $config_vars_rh->{'quarter2_end'};

die "quarter2_name not defined"  unless ( defined $quarter2_name );
die "quarter2_start not defined" unless ( defined $quarter2_start );
die "quarter2_end not defined"   unless ( defined $quarter2_end );

# Cisco quarter3
my $quarter3_name  = $config_vars_rh->{'quarter3_name'};
my $quarter3_start = $config_vars_rh->{'quarter3_start'};
my $quarter3_end   = $config_vars_rh->{'quarter3_end'};

die "quarter3_name not defined"  unless ( defined $quarter3_name );
die "quarter3_start not defined" unless ( defined $quarter3_start );
die "quarter3_end not defined"   unless ( defined $quarter3_end );

# Cisco quarter4
my $quarter4_name  = $config_vars_rh->{'quarter4_name'};
my $quarter4_start = $config_vars_rh->{'quarter4_start'};
my $quarter4_end   = $config_vars_rh->{'quarter4_end'};

die "quarter4_name not defined"  unless ( defined $quarter4_name );
die "quarter4_start not defined" unless ( defined $quarter4_start );
die "quarter4_end not defined"   unless ( defined $quarter4_end );

my $cisco_quarters = {
                    $quarter1_name => [$quarter1_start, $quarter1_end],
                    $quarter2_name => [$quarter2_start, $quarter2_end],
                    $quarter3_name => [$quarter3_start, $quarter3_end],
                    $quarter4_name => [$quarter4_start, $quarter4_end],
                   };
$cisco_quarters->{'quarters_order'} = [
                                   $quarter1_name,
                                   $quarter2_name,
                                   $quarter3_name,
                                   $quarter4_name
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

# sql file paths to pull CSEs
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
my $email_bcc      = $config_vars_rh->{'bcc'};


die "subject_prefix not subject" unless ( defined $subject_prefix );
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
  = new_tickets_KPI($cisco_quarters, $report_dbh);

# KPI - Tickets Touched
my $tickets_touched_KPI
  = tickets_touched_KPI($cisco_quarters, $report_dbh);

# KPI - Tickets Resolved
my $tickets_resolved_KPI
  = tickets_resolved_KPI($cisco_quarters, $report_dbh);

# KPI - CSAT Avg
my $csat_avg_KPI
  = csat_avg_KPI($cisco_quarters, $csat_dbh);

# Global CSAT Avg
my $csat_avg_Global
  = csat_avg_Global($cisco_quarters, $csat_dbh);

# Team CSAT Avg
my $csat_avg_Team
  = csat_avg_Team($cisco_quarters, \@cses, $report_dbh, $csat_dbh);

# KPI - New KB articles
my $new_kb_articles_KPI
  = new_kb_articles_KPI($cisco_quarters, $iKbase_dbh);

# KPI - KB linking %
my $kb_linking_KPI
  = kb_linking_KPI($cisco_quarters, $rt_dbh, $report_dbh);

# SPI - Avg Ticket Resolution Time
my $avg_resolution_SPI
  = avg_resolution_SPI($cisco_quarters, $report_dbh);

# SPI - Avg Interactions Per Ticket
my $avg_interactions_per_ticket_SPI
  = avg_interactions_per_ticket_SPI($cisco_quarters, $report_dbh);

# SPI - P1/P2 Tickets
my $p1_p2_tickets_SPI
  = p1_p2_tickets_SPI($cisco_quarters, $report_dbh);

# SPI - Management Escalated Tickets
my $management_escalated_tickets_SPI
  = management_escalated_tickets_SPI($cisco_quarters, $rt_dbh);

# SPI - Low CSATs (1-2) on CSE questions
my $low_cast_questions_SPI
  = low_cast_questions_SPI($cisco_quarters, $csat_dbh);

# SPI - High CSATs (4-5) on CSE questions
my $high_csat_questions_SPI
  = high_csat_questions_SPI($cisco_quarters, $csat_dbh);

# All CSATs (1-5) on CSE questions
my $all_csat_questions_SPI
  = all_csat_questions_SPI($cisco_quarters, $csat_dbh);

# SPI - Total CSAT Surveys
my $total_cast_surveys_SPI
  = total_cast_surveys_SPI($cisco_quarters, $csat_dbh);

# SPI - Current Ticket Backlog
#my $current_ticket_backlog_SPI
#  = current_ticket_backlog_SPI($cisco_quarters, $report_dbh);

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
    my $employee_email = $employee_rh->{'primary_email'};

    # pull out a manager's cisco email address to replace his/her
    # ironport email address
    my $ironport_email  = $employee_rh->{'manager'};

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

    $cisco_email_sth->execute($ironport_email) or die $cisco_email_sth->errstr;
    my $cisco_email_rh = $cisco_email_sth->fetchrow_hashref;

    my $manager_email = '';
    if( $cisco_email_rh ) {
        $manager_email = $cisco_email_rh->{'primary_email'};
    }

    # cse's name
    $cse_metrics{$owner_name}{'name'}    = $owner_name;

    # cse's email
    $cse_metrics{$owner_name}{'email'}   = $employee_email;

    # quarter's name
    @{$cse_metrics{$owner_name}{'quarters_order'}}
      = @{$cisco_quarters->{'quarters_order'}};

    # Extend Metrics' order
    @{$cse_metrics{$owner_name}{'Extend'}{'metrics_order'}} = (
        'Global CSAT Avg',
        'Team CSAT Avg',
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
#        'Current Ticket Backlog',
#        'Average Ticket Backlog',
    );

    # consolidate a hash that contains the cse's metrics data
    foreach my $quarter ( @{$cisco_quarters->{'quarters_order'}} ) {

        # New Tickets
        if ( exists $new_tickets_KPI->{$owner_name}->{'New Tickets'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'New Tickets'}{$quarter}
              = $new_tickets_KPI->{$owner_name}->{'New Tickets'}->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'New Tickets'}{$quarter}
              = 0;
        }

        # Tickets Touched
        if ( exists $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Touched'}{$quarter}
              = $tickets_touched_KPI->{$owner_name}->{'Tickets Touched'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Touched'}{$quarter}
              = 0;
        }

        # Tickets Resolved
        if ( exists $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Resolved'}{$quarter}
              = $tickets_resolved_KPI->{$owner_name}->{'Tickets Resolved'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Resolved'}{$quarter}
              = 0;
        }

        # Tickets Reopened
        if ( exists $csat_avg_KPI->{$owner_name}->{'Tickets Reopened'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'Tickets Reopened'}{$quarter}
              = $csat_avg_KPI->{$owner_name}->{'Tickets Reopened'}->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'CSAT Avg'}{$quarter}
              = 0;
        }

        # CSAT Average
        if ( exists $csat_avg_KPI->{$owner_name}->{'CSAT Avg'}->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'CSAT Avg'}{$quarter}
              = $csat_avg_KPI->{$owner_name}->{'CSAT Avg'}->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'CSAT Avg'}{$quarter}
              = 0;
        }

        # Global CSAT Average
        if ( exists $csat_avg_Global->{$quarter} ) {
            $cse_metrics{$owner_name}{'Extend'}{'Global CSAT Avg'}{$quarter}
              = $csat_avg_Global->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'Extend'}{'Global CSAT Avg'}{$quarter}
              = 0;
        }

        # Team CSAT Average
        foreach my $manager ( keys %{$csat_avg_Team} ) {
            if ($manager eq $ironport_email) {
                $cse_metrics{$owner_name}{'Extend'}{'Team CSAT Avg'}{$quarter}
                  = $csat_avg_Team->{$manager}->{$quarter};

                last;
            }
        }

        # New KB Articles
        if ( exists $new_kb_articles_KPI->{$owner_name}->{'New KB articles'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'New KB articles'}{$quarter}
              = $new_kb_articles_KPI->{$owner_name}->{'New KB articles'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'New KB articles'}{$quarter}
              = 0;
        }

        # KB linking %
        if ( exists $kb_linking_KPI->{$owner_name}->{'KB linking %'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'KPI'}{'KB linking %'}{$quarter}
              = $kb_linking_KPI->{$owner_name}->{'KB linking %'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'KPI'}{'KB linking %'}{$quarter}
              = 0;
        }

        # Avg Ticket Resolution Time
        if ( exists $avg_resolution_SPI->{$owner_name}
->{'Avg Ticket Resolution Time'}->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
{$quarter}
              = $avg_resolution_SPI->{$owner_name}
->{'Avg Ticket Resolution Time'}->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Ticket Resolution Time'}
{$quarter}
              = 0;
        }

        # Avg Interactions Per Ticket
        if ( exists $avg_interactions_per_ticket_SPI->{$owner_name}
->{'Avg Interactions Per Ticket'}->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Interactions Per Ticket'}
{$quarter}
              = $avg_interactions_per_ticket_SPI->{$owner_name}
->{'Avg Interactions Per Ticket'}->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Avg Interactions Per Ticket'}
{$quarter}
              = 0;
        }

        # P1/P2 Tickets
        if ( exists $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'P1/P2 Tickets'}{$quarter}
              = $p1_p2_tickets_SPI->{$owner_name}->{'P1/P2 Tickets'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'P1/P2 Tickets'}{$quarter}
              = 0;
        }

        # Management Escalated Tickets
        if ( exists $management_escalated_tickets_SPI->{$owner_name}
->{'Management Escalated Tickets'}->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Management Escalated Tickets'}
{$quarter}
              = $management_escalated_tickets_SPI->{$owner_name}
->{'Management Escalated Tickets'}->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Management Escalated Tickets'}
{$quarter}
              = 0;
        }

        # Low CSATs (1-2) on CSE questions
        if ( exists $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Low CSATs'}{$quarter}
              = $low_cast_questions_SPI->{$owner_name}->{'Low CSATs'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Low CSATs'}{$quarter}
              = 0;
        }

        # High CSATs (4-5) on CSE questions
        if ( exists $high_csat_questions_SPI->{$owner_name}->{'High CSATs'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'High CSATs'}{$quarter}
              = $high_csat_questions_SPI->{$owner_name}->{'High CSATs'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'High CSATs'}{$quarter}
              = 0;
        }

        # All CSATs (1-5) on CSE questions
        if ( exists $all_csat_questions_SPI->{$owner_name}->{'All CSATs'}
->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'All CSATs'}{$quarter}
              = $all_csat_questions_SPI->{$owner_name}->{'All CSATs'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'All CSATs'}{$quarter}
              = 0;
        }

        # Total CSAT Surveys
        if ( exists $total_cast_surveys_SPI->{$owner_name}
->{'Total CSAT Surveys'}->{$quarter} ) {
            $cse_metrics{$owner_name}{'SPI'}{'Total CSAT Surveys'}{$quarter}
              = $total_cast_surveys_SPI->{$owner_name}->{'Total CSAT Surveys'}
->{$quarter};
        }
        else {
            $cse_metrics{$owner_name}{'SPI'}{'Total CSAT Surveys'}{$quarter}
              = 0;
        }

#    if ( exists $current_ticket_backlog_SPI->{$owner_name}
#->{'Current Ticket Backlog'}->{$quarter} ) {
#        $cse_metrics{$owner_name}{'SPI'}{'Current Ticket Backlog'}{$quarter}
#          = $current_ticket_backlog_SPI->{$owner_name}->
#{'Current Ticket Backlog'}->{$quarter};
#    }
#    else {
#        $cse_metrics{$owner_name}{'SPI'}{'Current Ticket Backlog'}{$quarter}
#           = 0;
#    }
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

    # divide the count of 'KB linking' tickets by the count of resolved tickets
    # to figure out 'KB Linking %' for per CSE
    foreach my $quarter ( @{$cse_metrics{$per_cse_metrics}{'quarters_order'}} )
    {
        if ( ( exists $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}
{$quarter} )
               && ( exists $cse_metrics{$per_cse_metrics}{'KPI'}
{'Tickets Resolved'}{$quarter} ) ) {

            my $kb_linking
              = $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}{$quarter};

            my $tickets_resolved
              = $cse_metrics{$per_cse_metrics}{'KPI'}{'Tickets Resolved'}
{$quarter};

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

            $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}{$quarter}
              = $kb_linking_percent;
        }
        else {
            $cse_metrics{$per_cse_metrics}{'KPI'}{'KB linking %'}{$quarter}
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

    # Generate *.xls attachment
    my @first_row = get_options_per_cse($cse_metrics{$per_cse_metrics});
    my $per_cse_row = get_row_per_cse($cse_metrics{$per_cse_metrics});
    #print Dumper($per_cse_row);

    my $attachment_file
        = make_excel_for_each_cse(
                                  \@first_row,
                                  $per_cse_row,
                                  $data_dir,
                                  );

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
        my $manager_email = $cse_metrics{$per_cse_metrics}{'manager'};

        $cc = get_report_CClist($to, $manager_email, $report_dbh);
    }

    my $bcc;
    if ( $environment =~ /development|test/i ) {
        $bcc = '';
    }
    elsif ( $environment =~ /production/i ) {
        $bcc = $email_bcc;
    }

    my $subject = $subject_prefix . $cse_metrics{$per_cse_metrics}{'name'};

    # output report email 
    email_results(
                  $email_from,
                  $to, $cc, $bcc, $subject, $digest, $attachment_file);
    unlink($attachment_file);

    #last;
}


#----------------------------------------------------------------------
# subordinates...
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# New Tickets
#----------------------------------------------------------------------
sub new_tickets_KPI {

    my $quarters = shift;
    my $dbh      = shift;

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

    my %new_tickets_metric;
    foreach my $quarter (keys %{$quarters}) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner       = $rh->{'owner'};
            my $total       = $rh->{'total'};

            $new_tickets_metric{$owner}{'New Tickets'}{$quarter} = $total;
        }

    }

    return \%new_tickets_metric;
}

#----------------------------------------------------------------------
# Tickets Touched
#----------------------------------------------------------------------
sub tickets_touched_KPI {

    my $quarters = shift;
    my $dbh      = shift;

    my $set_big_sql = "SET SQL_BIG_SELECTS=1";
    my $set_big_sth = $dbh->prepare($set_big_sql) or die $dbh->errstr;
    $set_big_sth->execute() or die $set_big_sth->errstr;

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

    my %tickets_touched_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner         = $rh->{'owner'}; 
            my $cases_touched = $rh->{'cases_touched'};

            $tickets_touched_metric{$owner}{'Tickets Touched'}{$quarter}
              = $cases_touched;
        }

    }

    return \%tickets_touched_metric;
}

#----------------------------------------------------------------------
# Tickets Resolved
#----------------------------------------------------------------------
sub tickets_resolved_KPI {

    my $quarters = shift;
    my $dbh      = shift;

    my $sql
    = "
    SELECT
      IF(LOCATE('\@', owner) = 0,
        owner, LEFT(owner, LOCATE('\@', owner) - 1)
      ) AS owner_name,
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
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner            = $rh->{'owner_name'};
            my $tickets_resolved = $rh->{'tickets_resolved'};

            $tickets_resolved_metric{$owner}{'Tickets Resolved'}{$quarter}
              = $tickets_resolved;
        }

    }

    return \%tickets_resolved_metric;
}

#----------------------------------------------------------------------
# Tickets Reopened
#----------------------------------------------------------------------
sub tickets_reopened_KPI {

    my $quarters = shift;
    my $dbh    = shift;

    my $set_big_sql = "SET SQL_BIG_SELECTS=1";
    my $set_big_sth = $dbh->prepare($set_big_sql) or die $dbh->errstr;
    $set_big_sth->execute() or die $set_big_sth->errstr;

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
    foreach my $quarter ( keys %{$quarters}) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner            = $rh->{'owner'};
            my $tickets_reopened = $rh->{'tickets_reopened'};

            if ( defined $tickets_reopened ) {
                $tickets_reopened_metric{$owner}{'Tickets Reopened'}{$quarter}
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

    my $quarters = shift;
    my $dbh      = shift; 

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
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute( $quarter_start, $quarter_end ) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner = $rh->{'owner'};
            my $csat  = $rh->{'csat'};

            $csat_avg_metric{$owner}{'CSAT Avg'}{$quarter} = $csat;
        }

    }

    return \%csat_avg_metric;
}

#----------------------------------------------------------------------
# Global CSAT Avg- divide the sum of all surveys' CSAT results by
# the total number of surveys in one quarter
#----------------------------------------------------------------------
sub csat_avg_Global {

    my $quarters = shift;
    my $dbh      = shift;

    my $sql
    ="
    SELECT
      ROUND((SUM(e.csat) / SUM(e.number)), 2) AS csat
    FROM
      (
      SELECT
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
      GROUP BY id) e";
    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

    my %csat_avg_Global = ();
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute( $quarter_start, $quarter_end ) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {

            my $csat;
            if ( defined $rh->{'csat'} ) {
                $csat = $rh->{'csat'};
            }
            else {
                $csat = 0;
            }
            $csat_avg_Global{$quarter} = $csat;
        }
    }

    return \%csat_avg_Global;
}

#----------------------------------------------------------------------
# Team CSAT Avg
#----------------------------------------------------------------------
sub csat_avg_Team {

    my ($quarters, $cses, $report_dbh, $csat_dbh) = @_;

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

    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $survey_sth->execute( $quarter_start, $quarter_end )
        or die $survey_sth->errstr;

        while( my $rh = $survey_sth->fetchrow_hashref ) {
            my $owner = $rh->{'owner'};
            my $csat  = $rh->{'csat'};

            $cses_info{$owner}{$quarter}{'value'} += $csat;
            $cses_info{$owner}{$quarter}{'count'} += 1;
        }
    }

    my %csat_avg_Team = ();
    foreach my $manager ( @managers ) {

        foreach my $quarter ( keys %{$quarters} ) {

            next if ( $quarter eq 'quarters_order' );

            my $csat_value = 0;
            my $csat_count = 0;
            foreach my $cse ( keys %cses_info ) {

                next if ( !exists $cses_info{$cse}{'manager'} );
                next if ( !exists $cses_info{$cse}{$quarter}{'value'} );
                next if ( !exists $cses_info{$cse}{$quarter}{'count'} );

                next if ( $cses_info{$cse}{'manager'} ne $manager );

                $csat_value += $cses_info{$cse}{$quarter}{'value'};
                $csat_count += $cses_info{$cse}{$quarter}{'count'};
            }

            my $avg_value = 0;
            if ( $csat_count != 0 ) {
                $avg_value = sprintf("%.2f", ($csat_value / $csat_count));
            }

            $csat_avg_Team{$manager}{$quarter} = $avg_value;
        }
    }

    return \%csat_avg_Team;
}

#----------------------------------------------------------------------
# New KB articles
#----------------------------------------------------------------------
sub new_kb_articles_KPI {

    my $quarters = shift;
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

    my %new_kb_articles_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner          = $rh->{'owner'};
            my $articles_total = $rh->{'total'};

            $new_kb_articles_metric{$owner}{'New KB articles'}{$quarter}
              = $articles_total;
        }
    }

    return \%new_kb_articles_metric;
}

#----------------------------------------------------------------------
# KB linking %
#----------------------------------------------------------------------
sub kb_linking_KPI {

    my ($quarters, $rt_dbh, $report_dbh) = @_;

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

    my %kb_linking_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $kb_linked_sth->execute($quarter_start, $quarter_end)
          or die $kb_linked_sth->errstr;

        my @kb_tickets = qw//;
        while ( my $kb_linked_rh = $kb_linked_sth->fetchrow_hashref ) {

            my $ticket_number = $kb_linked_rh->{'Id'};
            push @kb_tickets, $ticket_number;
        }

        $resolved_sth->execute($quarter_start, $quarter_end)
          or die $resolved_sth->errstr;

        while ( my $resolved_rh = $resolved_sth->fetchrow_hashref ) {

            my $owner         = $resolved_rh->{'owner'};
            my $ticket_number = $resolved_rh->{'case_number'};

            if ( grep {$ticket_number eq $_} @kb_tickets ) {
                $kb_linking_metric{$owner}{'KB linking %'}{$quarter} += 1;
            }
            else {
                $kb_linking_metric{$owner}{'KB linking %'}{$quarter} += 0;
            }
        }

    }

    return \%kb_linking_metric;
}

#----------------------------------------------------------------------
# Avg Ticket Resolution Time (seconds) 
#----------------------------------------------------------------------
sub avg_resolution_SPI {

    my $quarters = shift;
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

    my %avg_resolution_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner    = $rh->{'owner'};
            my $avg_time = $rh->{'avg_ticket_resolution_time'};

            $avg_resolution_metric{$owner}{'Avg Ticket Resolution Time'}
{$quarter}
              = $avg_time;
        }

    }

    return \%avg_resolution_metric;
}

#----------------------------------------------------------------------
# Avg Interactions Per Ticket
#----------------------------------------------------------------------
sub avg_interactions_per_ticket_SPI {

    my $quarters = shift;
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

    my %avg_interactions_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while( my $rh = $sth->fetchrow_hashref ) {
            my $owner            = $rh->{'owner'};
            my $avg_interactions = $rh->{'avg_interactions'};

            $avg_interactions_metric{$owner}{'Avg Interactions Per Ticket'}
{$quarter}
              = $avg_interactions;
        }

    }

    return \%avg_interactions_metric;

}

#----------------------------------------------------------------------
# P1/P2 Tickets
#----------------------------------------------------------------------
sub p1_p2_tickets_SPI {

    my $quarters = shift;
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

    my %p1_p2_tickets_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while(my $rh = $sth -> fetchrow_hashref) {
            my $owner = $rh->{'owner'};
            my $p1_p2 = $rh->{'p1_p2'};

            $p1_p2_tickets_metric{$owner}{'P1/P2 Tickets'}{$quarter} = $p1_p2;
        }

    }

    return \%p1_p2_tickets_metric
}

#----------------------------------------------------------------------
# Management Escalated Tickets
#----------------------------------------------------------------------
sub management_escalated_tickets_SPI {

    my $quarters = shift;
    my $dbh      = shift;

    my $sql = "SELECT
                 IF(LOCATE('\@', u.Name) = 0,
                    u.Name,
                    LEFT(u.Name, LOCATE('\@', u.Name)-1)
                 ) AS owner,
                 COUNT(*) AS management_escalated_tickets
               FROM
                 Tickets t,
                 TicketCustomFieldValues c,
                 CustomFields cf,
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

    my %management_escalated_tickets_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while(my $rh = $sth -> fetchrow_hashref) {
            my $owner = $rh->{'owner'};
            my $management_escalated_tickets
              = $rh->{'management_escalated_tickets'};

            $management_escalated_tickets_metric{$owner}
{'Management Escalated Tickets'}{$quarter}
              = $management_escalated_tickets;
        }
    }

    return \%management_escalated_tickets_metric
}

#----------------------------------------------------------------------
# Low CSATs (1-2) on CSE questions
#----------------------------------------------------------------------
sub low_cast_questions_SPI {

    my $quarters = shift;
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

    my %low_cast_questions_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while(my $rh = $sth -> fetchrow_hashref) {
            my $owner          = $rh->{'owner'};
            my $total_low_csat = $rh->{'total_low_csat'};

            $low_cast_questions_metric{$owner}{'Low CSATs'}{$quarter}
              = $total_low_csat;
        }
    }

    return \%low_cast_questions_metric
}

#----------------------------------------------------------------------
# High CSATs (4-5) on CSE questions
#----------------------------------------------------------------------
sub high_csat_questions_SPI {

    my $quarters = shift;
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

    my %high_csat_questions_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while(my $rh = $sth -> fetchrow_hashref) {
            my $owner           = $rh->{'owner'};
            my $total_high_csat = $rh->{'total_high_csat'};

            $high_csat_questions_metric{$owner}{'High CSATs'}{$quarter}
              = $total_high_csat;
        }

    }

    return \%high_csat_questions_metric
}


#----------------------------------------------------------------------
# All CSATs (1-5) on CSE questions
#----------------------------------------------------------------------
sub all_csat_questions_SPI {

    my $quarters = shift;
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

    my %all_csat_questions_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while(my $rh = $sth -> fetchrow_hashref) {
            my $owner      = $rh->{'owner'};
            my $total_csat = $rh->{'total_csat'};

            $all_csat_questions_metric{$owner}{'All CSATs'}{$quarter}
              = $total_csat;
        }

    }

    return \%all_csat_questions_metric
}


#----------------------------------------------------------------------
# Total CSAT Surveys
#----------------------------------------------------------------------
sub total_cast_surveys_SPI {

    my $quarters = shift;
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


    my %total_cast_surveys_metric;
    foreach my $quarter ( keys %{$quarters} ) {

        next if ( $quarter eq 'quarters_order' );

        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

        while(my $rh = $sth -> fetchrow_hashref) {
            my $owner              = $rh->{'owner'};
            my $total_csat_surveys = $rh->{'total_csat_surveys'};

            $total_cast_surveys_metric{$owner}{'Total CSAT Surveys'}{$quarter}
              = $total_csat_surveys;
        }
    }

    return \%total_cast_surveys_metric
}

#----------------------------------------------------------------------
# Current Ticket Backlog
#----------------------------------------------------------------------
#sub current_ticket_backlog_SPI {

#    my $quarters = shift;
#    my $dbh      = shift;

#    my $sql = "SELECT
#                 SUBSTR(owner, 1, LOCATE('\@', owner) - 1) AS owner,
#                 COUNT(case_number) AS current_backlog,
#               FROM
#                 case_details
#               WHERE
#                 (status = 'open' OR status = 'stalled' OR status = 'new')
#                 AND created >= ?
#                 AND created < ?
#               GROUP BY owner";
#    my $sth = $dbh->prepare($sql) or die $dbh->errstr;

#    my %current_ticket_backlog_metric;
#    foreach my $quarter ( keys %{$quarters} ) {

#        next if ( $quarter eq 'quarters_order' );

#        my $quarter_start = $quarters->{$quarter}[0] . " 00:00:00";
#        my $quarter_end   = $quarters->{$quarter}[1] . " 00:00:00";

#        $sth->execute($quarter_start, $quarter_end) or die $sth->errstr;

#        while(my $rh = $sth -> fetchrow_hashref) {
#            my $owner           = $rh->{'owner'};
#            my $current_backlog = $rh->{'current_backlog'};

#            $current_ticket_backlog_metric{$owner}{'Current Ticket Backlog'}
#{$quarter}
#              = $current_backlog;
#        }
#    }

#    return \%current_ticket_backlog_metric
#}


#----------------------------------------------------------------------
# figure out the email list that the metrics report should be CC'd to
#----------------------------------------------------------------------
sub get_report_CClist {

    my $email         = shift;
    my $manager_email = shift;
    my $dbh           = shift;

    # the CC'd list
    my $reportCC = $manager_email;

    # figure out the CSE's role from the employees table
    my $employee_role = '';

    my $role_sql = "SELECT role FROM employees WHERE primary_email = ?";
    my $role_sth = $dbh->prepare($role_sql) or die $dbh->errstr;

    $role_sth->execute($email) or die $role_sth->errstr;
    my $role_rh = $role_sth->fetchrow_hashref;

    if( $role_rh ) {
        $employee_role = $role_rh->{'role'};
    }

    # figure out the manager's pk from the employees table
    my $pk_sql = "SELECT pk FROM employees WHERE primary_email = ?";
    my $pk_sth = $dbh->prepare($pk_sql) or die $dbh->errstr;

    $pk_sth->execute($manager_email) or die $pk_sth->errstr;
    my $pk_rh = $pk_sth->fetchrow_hashref;

    if( $pk_rh ) {
        my $pk = $pk_rh->{'pk'};

        # figure out a manager's assistants' pks from the metrics_reportto
        # table
        my $metrics_sql
        = "
        SELECT
          metrics_secondary
        FROM
          metrics_reportto
        WHERE
          del <> '1'
          AND metrics_primary = ?
          AND role = ?";
        my $metrics_sth = $dbh->prepare($metrics_sql) or die $dbh->errstr;

        $metrics_sth->execute($pk, $employee_role) or die $metrics_sth->errstr;
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
                if( defined $reportto_rh->{'primary_email'} ) {
                    my $cc_email = $reportto_rh->{'primary_email'};

                    if( $cc_email ne $email ) {
                        $reportCC .= ", " . $cc_email;
                    }
                }
            }
        }

    }

    return $reportCC;
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
# get header of excel
#----------------------------------------------------------------------

sub get_options_per_cse{

    my $cse_quarter = shift;

    my @cse_extend = @{$cse_quarter->{'Extend'}->{'metrics_order'}};
    my @cse_kpi    = @{$cse_quarter->{'KPI'}->{'metrics_order'}};
    my @cse_spi    = @{$cse_quarter->{'SPI'}->{'metrics_order'}};

    my @first_row;
    @first_row = ("CSE alias", @cse_extend, @cse_kpi, @cse_spi);

    return @first_row;

}

#----------------------------------------------------------------------
# get one row of excel
#----------------------------------------------------------------------
sub get_row_per_cse{

    my $cse_quarter = shift;

    my @first_row = get_options_per_cse($cse_quarter);

    my %kpi_spi_value = (
                         %{$cse_quarter->{"Extend"}},
                         %{$cse_quarter->{"KPI"}},
                         %{$cse_quarter->{"SPI"}}
                         );

    delete $kpi_spi_value{"metrics_order"};

    my %return_hash = ('name' => $cse_quarter->{"name"});

    # making an order to the values for cse extend, kpi and spi
    foreach my $quarter ( @{$cse_quarter->{'quarters_order'}} ) {

        my @return_array = ();
        for my $i(1..$#first_row) {

            if (!exists $kpi_spi_value{$first_row[$i]}{$quarter}) {
                $kpi_spi_value{$first_row[$i]}{$quarter} = 0;
            }

            foreach my $status(keys %kpi_spi_value){

                if ($first_row[$i] eq $status) {
                    if ($kpi_spi_value{$status}{$quarter} eq "") {
                        $kpi_spi_value{$status}{$quarter} = 0;
                    }

                    push @return_array, $kpi_spi_value{$status}{$quarter};
                }
            }
        }

        @{$return_hash{$quarter}} = @return_array;
    }

return \%return_hash;

}

#----------------------------------------------------------------------
# make excel attachment for per cse quarterly report
#----------------------------------------------------------------------
#TODO:To make it for recordset
sub make_excel_for_each_cse{

    my $first_row   = shift;
    my $per_cse_row = shift;
    my $data_dir    = shift;

    mkdir "$data_dir", 0777 unless -d "$data_dir";

    my $file_path = $data_dir
                    . $per_cse_row->{"name"}
                    . '_quarterly_report.xls';

    my $per_cse_quarterly_report
        = Spreadsheet::WriteExcel->new($file_path);

    my $format = $per_cse_quarterly_report->add_format();
    $format->set_bold();

    foreach my $quarter ( keys %{$per_cse_row} ) {

        next if ($quarter eq "name");

        my @second_row = ($per_cse_row->{"name"}, @{$per_cse_row->{$quarter}});

        my $quarterly_report
            = $per_cse_quarterly_report->add_worksheet($quarter);

        $quarterly_report -> write_row(0, 0, $first_row, $format);
        $quarterly_report -> write_row(1, 0, \@second_row);
    }

    return $file_path;
}

#----------------------------------------------------------------------
# email out results
#----------------------------------------------------------------------
sub email_results {

    my ($from, $to, $cc, $bcc, $subject, $html, $attachment) = @_;

    my %mail_config = (
        'reply_to'   => $from,
        'from'       => $from,
        'to'         => $to, 
        'cc'         => $cc,
        'bcc'        => $bcc,
        'subject'    => $subject, 
        'text'       => '',
        'html'       => $html,
        'attachment' => $attachment,
    );

    my ( $stat, $err ) = SendMail::multi_mail_attachment( \%mail_config );
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
    this script is used to send metrics report for per CSE quarterly.

  OPTIONS:
    -r .. Run
    -e .. Set Environment [ development | test | production ]

    Each environment has its own databases and set of configuration parameters.

    Configuration files found here:
      ../conf/quarterly_report_metrics_per_cse_development.conf
      ../conf/quarterly_report_metrics_per_cse_test.conf
      ../conf/quarterly_report_metrics_per_cse_production.conf

  Examples:
  $0 -r -e development

EOP
}
