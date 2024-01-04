#!/usr/bin/perl

use strict;
use warnings;
use Spreadsheet::XLSX;
 
my $LOG_DIR=$ARGV[0];
my $excel = Spreadsheet::XLSX -> new ("$LOG_DIR/commodity.xlsx");
my $line;
my $asof;
my $commodity_sql = "$LOG_DIR/commodity.sql";
open(SQL_FILE,'>',$commodity_sql) or die $!;

foreach my $sheet (@{$excel -> {Worksheet}}) {
    if (  $sheet->{Name} eq "Monthly Indices" ){
          exit;
    }
    #printf("Sheet: %s\n", $sheet->{Name});
    $sheet -> {MaxRow} ||= $sheet -> {MinRow};
    my @commodity = ();
    my @commodity_desc = ();
    my @commodity_unit = ();

    foreach my $row ($sheet -> {MinRow} .. $sheet -> {MaxRow}) {
	if ( $row lt 4 ){
		next;
	}
	elsif ( $row eq 4 ) {
                $sheet -> {MaxCol} ||= $sheet -> {MinCol};
                foreach my $col ($sheet -> {MinCol} ..  $sheet -> {MaxCol}) {
                        my $cell = $sheet -> {Cells} [$row] [$col];
			push(@commodity_desc, $cell -> {Val});
		}
	}
	elsif ( $row eq 5 ) {
                $sheet -> {MaxCol} ||= $sheet -> {MinCol};
                foreach my $col ($sheet -> {MinCol} ..  $sheet -> {MaxCol}) {
                        my $cell = $sheet -> {Cells} [$row] [$col];
			push(@commodity_unit, $cell -> {Val});
		}
	}
	elsif ( $row eq 6 ) {
                $sheet -> {MaxCol} ||= $sheet -> {MinCol};
                foreach my $col ($sheet -> {MinCol} ..  $sheet -> {MaxCol}) {
                        my $cell = $sheet -> {Cells} [$row] [$col];
			push(@commodity, $cell -> {Val});
		}
	        
	}
        else {
                $sheet -> {MaxCol} ||= $sheet -> {MinCol};
                foreach my $col ($sheet -> {MinCol} ..  $sheet -> {MaxCol}) {
                    my $cell = $sheet -> {Cells} [$row] [$col];
                    if ($cell) {
                        if ( ($cell -> {Val}) =~ m/M01|M02|M03|M04|M05|M06|M07|M08|M09|M10|M11|M12/ ) {
                             $asof = "LAST_DAY(TO_DATE('".($cell -> {Val})."', 'YYYY\"M\"MM'))";
			     #print "$asof\n";
        		} 
        		else {
			     
                             $line .= "INSERT INTO FIN_COMMODITY_MONTHLY_HISTORY VALUES ('".$commodity[$col]."',".$asof.",".$cell -> {Val}.");\n";
			     print SQL_FILE "$line";
			     $line = '';
        	        }
		}
            }
        }
    }
    for my $i (1 .. $#commodity){
        print SQL_FILE "INSERT INTO FIN_COMMODITY_LOOKUP(COMMODITY_CODE, COMMODITY_NAME, COMMODITY_UNIT) VALUES('".$commodity[$i]."','".$commodity_desc[$i]."','".$commodity_unit[$i]."');\n"
    }
}
