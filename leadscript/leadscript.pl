
#!/usr/bin/perl

use strict;
use warnings;

use Parse::AccessLog;
use Data::Dumper;
use MongoDB;
use Encode qw(decode encode);
use String::Util qw(trim);
use String::CamelCase qw(camelize);
use DateTime::Format::DateParse;

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
    my $dt = DateTime::Format::DateParse->parse_datetime( $hash{"time_local"} );
    my $formatted_date_time = $dt->ymd . " " . $dt->hms;
    print $formatted_date_time . "\n";
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
    my $email = $doc->{'email'};
    my $type = $doc->{'tipo'};

    if ($type eq 'b2b')  {
      print "B2B: $name $email \n";
    }

    if ( exists $hash_ip{($time, $day)} ) {
        my $ip = $hash_ip{($time, $day)};

        if (! exists $result{$name}) {
          my %lead = (day => $day,
          time => $time,
          ip => $ip,
          name => $name,
          email => $email,
          type => $type);

          $result{$name} = \%lead;
        } else
        {
          print "Ignorando Lead repetido $name\n";
        }
    }
    else {
        print " Não existe na hash de IPs o lead $name !!\n";
    }

}

my $x = scalar (values %result);
print "Total: $x \n";
my @filtered;

for my $line (values %result) {
  if (index(lc($line->{email}), "test") == -1 && $line->{name} ne "" && index(lc($line->{name}), "test") == -1) {
    $line->{email} = trim lc $line->{email};
    $line->{type} = uc $line->{type};
    $line->{name} = trim camelize lc $line->{name};

    push @filtered, $line;
  } else {
    print "Ignorando Teste: $line->{email} $line->{name}\n";
  }
}

my @sorted = sort { $a->{day}.$a->{time} cmp $b->{day}.$b->{time} } @filtered;

print "\n\n";

print "email,nome,ip,tipo,data_hora\n";
for my $line (values @sorted) {
    print "$line->{email},$line->{name},$line->{ip},$line->{type},$line->{day} $line->{time}\n";
    #my $line = "$line->{time} $line->{email} - $line->{name}";
}

print "Total $#filtered leads válidos\n";
