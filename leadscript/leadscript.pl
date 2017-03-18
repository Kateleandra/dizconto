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
use Time::Zone;

sub data_utc_brz {
    my $dt = shift;
    $dt = $dt->add( hours => -3 );

    return $dt;
}

sub get_hash_ip {
    my $file = shift;
    my %hash_ip;

    my $p    = Parse::AccessLog->new;
    my @logs = $p->parse($file);

    for my $log (@logs) {
        my $time = substr( $log->{"time_local"}, 12, 8 );
        my $day  = substr( $log->{"time_local"}, 0,  2 );

        my $dt =
          DateTime::Format::DateParse->parse_datetime( $log->{"time_local"} );
        $dt = data_utc_brz $dt;

        my %pair = (
            ip                  => $log->{"remote_addr"},
            formatted_date_time => $dt->ymd . " " . $dt->hms
        );
        $hash_ip{ ( $time, $day ) } = \%pair;
    }

    return \%hash_ip;
}

sub get_leads {
    my $client = MongoDB->connect('mongodb://localhost');
    my $collection = $client->ns('sails.lead');   # database foo, collection bar
    my $leads      = $collection->find;

    return $leads;
}

sub filter_lead {
    my $hash_name_lead = shift;
    my @filtered_leads;
    for my $line ( values %{$hash_name_lead} ) {
        if (   index( lc( $line->{email} ), "test" ) == -1
            && $line->{name} ne ""
            && index( lc( $line->{name} ), "test" ) == -1 )
        {
            $line->{email} = trim lc $line->{email};
            $line->{type}  = uc $line->{type};
            $line->{name}  = trim camelize lc $line->{name};

            push @filtered_leads, $line;
        }
        else {
            print "Ignorando Teste: $line->{email} $line->{name}\n";
        }
    }

    return \@filtered_leads;
}

sub hash_name_lead {
    my $leads   = shift;
    my $hash_ip = shift;
    my %hash_name_lead;
    while ( my $doc = $leads->next ) {
        my $time = substr( $doc->{'createdAt'}, 11, 8 );
        my $day  = substr( $doc->{'createdAt'}, 8,  2 );
        my $name  = encode( 'utf8', $doc->{'nome'} );
        my $email = $doc->{'email'};
        my $type  = $doc->{'tipo'};

        if ( exists $hash_ip->{ ( $time, $day ) } ) {
            my %data                = %{ $hash_ip->{ ( $time, $day ) } };
            my $ip                  = $data{ip};
            my $formatted_date_time = $data{formatted_date_time};

            if ( !exists $hash_name_lead{$name} ) {
                my %lead = (
                    day                 => $day,
                    time                => $time,
                    ip                  => $ip,
                    name                => $name,
                    email               => $email,
                    type                => $type,
                    formatted_date_time => $formatted_date_time
                );

                $hash_name_lead{$name} = \%lead;
            }
            else {
                print "Ignorando Lead repetido $name\n";
            }
        }
        else {
            print " Não existe IPs para o lead $name !!\n";
        }

    }

    return \%hash_name_lead;
}

sub print_csv {
    my $sorted = shift;

    print "email,nome,ip,tipo,data_hora\n";
    for my $line ( values @{$sorted} ) {
        print
"$line->{email},$line->{name},$line->{ip},$line->{type},$line->{formatted_date_time}\n";
    }
}

my $num_args = scalar @ARGV;
if ( $num_args != 1 ) {
    print "\nUso: leadscript.pl access.log\n";
    exit;
}

my %hash_ip        = %{ get_hash_ip( $ARGV[0] ) };
my $leads          = get_leads;
my $hash_name_lead = hash_name_lead $leads, \%hash_ip;
my @filtered_leads = @{ filter_lead $hash_name_lead };
my @sorted =
  sort { $a->{day} . $a->{time} cmp $b->{day} . $b->{time} } @filtered_leads;

print_csv \@sorted;
print "Total $#filtered_leads leads válidos\n";
