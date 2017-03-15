#!/usr/bin/perl

use strict;
use warnings;

use Parse::AccessLog;
use Data::Dumper;
use MongoDB;
use Encode qw(decode encode);

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
    my $day = substr($hash{"time_local"}, 0, 2);
    my $ip   = $hash{"remote_addr"};
    $hash_ip{($time, $day)} = $ip;
}

my $client = MongoDB->connect('mongodb://localhost');
my $collection = $client->ns('sails.lead');    # database foo, collection bar
my $leads = $collection->find;

my %result;

while ( my $doc = $leads->next ) {
    my $time = substr( $doc->{'createdAt'}, 11, 8 );
    my $day = substr($doc->{'createdAt'},8,2);
    my $name = encode('utf8', $doc->{'nome'});
    if ( exists $hash_ip{($time, $day)} ) {
        my $ip = $hash_ip{($time, $day)};

        if (! exists $result{$name}) {
          my %lead = (day => $day,
          time => $time,
          ip => $ip,
          name => $name,
          email => $doc->{'email'});

          $result{$name} = \%lead;
        } else
        {
          print "Ignorando Lead repetido $name\n";
        }
    }
    else {
        #print " Não existe na hash de IPs o lead $name !!\n";
    }

}

my @filtered;

for my $line (values %result) {
  if (index(lc($line->{email}), "test") == -1 && $line->{name} ne "" && index(lc($line->{name}), "test") == -1) {
    push @filtered, $line;
  } else {
    print "Ignorando Teste: $line->{email} $line->{name}\n";
  }
}


my @sorted = sort { $a->{day}.$a->{time} cmp $b->{day}.$b->{time} } @filtered;

print "\n\n";

for my $line (values @sorted) {
    #my $line = "Dia $line->{day} $line->{time} IP: $line->{ip}, $line->{email}, $line->{name}";
    my $line = "$line->{time} $line->{email} - $line->{name}";
    print $line."\n";
}

print "Total $#filtered leads válidos\n";
