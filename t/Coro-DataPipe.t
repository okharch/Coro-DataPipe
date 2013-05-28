# Before 'make install' is performed this script should be runnable with
# 'make test'. After 'make install' it should work as 'perl Coro-DataPipe.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;

use Coro;
use Coro::AnyEvent;
use Time::HiRes qw(time);

use Test::More tests => 4;
BEGIN { use_ok('Coro::DataPipe') };

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $t = time;
test_run();
printf "1. %d\n", (time-$t)*1000;

$t = time;
test_run();
printf "2. %d\n", (time-$t)*1000;

sub test_run {
    my $n_items = 100000;
    my $sleep = 0.01; # make $n_items * $sleep > 1 to test cooperative processing
    my @input = 1..$n_items;
    my @copy = @input;
    my @processed;
    my $t = time();
    my $number_of_data_processors = $n_items;#int($n_items/20);
    $number_of_data_processors = 500;
    Coro::DataPipe::run({
        input => \@input,
        process => sub{
            Coro::AnyEvent::sleep(rand() * $sleep);
            $_*2;
        },
        output=>\@processed,
        number_of_data_processors => $number_of_data_processors,
    });
    ok(time-$t<$n_items*($n_items/$number_of_data_processors)*$sleep,"cooperative processing ($number_of_data_processors data processors)");
    ok(@processed==$n_items,'processed length');
    ok(join(",",map $_*2,@copy) eq join(",",sort {$a <=> $b} @processed),'processed values');
}

