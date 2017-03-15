#!/usr/bin/perl

use strict;
use warnings;

use Parse::AccessLog;
use Data::Dumper;
use MongoDB;

my %hash_ip;

my $num_args = scalar @ARGV;
if ( $num_args != 1 ) {
    print "\nUso: leadscript.pl access.log\n";
    exit;
}

my $p    = Parse::AccessLog->new;
my @recs = $p->parse( $ARGV[0] );

for my $rec (@recs) {
    my %hash = %{$rec};
    my $time = substr( $hash{"time_local"}, 12, 8 );
    my $ip   = $hash{"remote_addr"};
    $hash_ip{$time} = $ip;

    #    print "$time | $ip\n";
}

my $client = MongoDB->connect('mongodb://localhost');
my $collection = $client->ns('sails.lead');    # database foo, collection bar

#my $result     = $collection->insert_one({ some => 'data' });
my $leads = $collection->find;

while ( my $doc = $leads->next ) {
    my $time = substr( $doc->{'createdAt'}, 11, 8 );
    if ( exists $hash_ip{$time} ) {
        my $ip = $hash_ip{$time};
        print $time. " $ip \n";
    }
    else {
        print " Não existe na hash \n";
    }
}
