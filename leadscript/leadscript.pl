#!/usr/bin/perl

use strict;
use warnings;

use Parse::AccessLog;
use Data::Dumper;

my $num_args = scalar @ARGV;
if ( $num_args != 1 ) {
    print "\nUso: leadscript.pl access.log\n";
    exit;
}

my $p    = Parse::AccessLog->new;
my @recs = $p->parse( $ARGV[0] );

for my $rec (@recs) {
    my %hash = %{$rec};
    my $time = $hash{"time_local"};
    my $ip   = $hash{"remote_addr"};
    print "$time | $ip\n";
}
