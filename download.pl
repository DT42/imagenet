#!/usr/bin/env perl

use Mojo::Base -strict;

use File::Path 'make_path';
use Mojo::Log;
use Mojo::UserAgent;
use POSIX 'nice';
use threads;
use Thread::Queue;

use Mojo::Util qw(dumper);

nice(19);

my $input = shift;
open my $handle, $input or die;
my ($release) = split '\.', $input;

my $q = Thread::Queue->new;
$q->limit = 10000;

my $master = async {
    my %dirs;
    while (my $line = <$handle>) {
        my ($id, $uri) = split ' ', $line;
        my ($wnid, $num) = split '_', $id;
        my $dir = "$release/$wnid";
        if (!exists $dirs{$dir}) {
            $dirs{$dir} = 1;
            make_path($dir);
        }
        $q->enqueue({path => "$dir/$num", uri => $uri});

        $q->end if eof;
    }
};

my @workers = map {
    async {
        my $log = Mojo::Log->new;
        my $ua = Mojo::UserAgent->new;
        while (my $i = $q->dequeue) {
            my $res = $ua->get($i->{uri})->res;
            next if $res->headers->content_type !~ /image/;

            my $asset = $res->content->asset;
            next unless $asset->size;

            $asset->move_to($i->{path});
            $log->info("$i->{path} $i->{uri}");
        }
    };
} 1 .. 100;

$master->join;
$_->join for @workers;
