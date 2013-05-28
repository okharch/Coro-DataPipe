package Coro::DataPipe;

our $VERSION='0.01';
use 5.006; # Perl::MinimumVersion says that

use strict;
use warnings;
use List::Util qw(first);
use Coro; #qw(schedule async);

sub run {
    my $param = {};
    my ($input,$process,$output) = @_;
    if (ref($input) eq 'HASH') {
        $param = $input;
    } else {
        $param = {input=>$input, process=>$process, output=>$output };
    }
    my $conveyor = __PACKAGE__->new($param);
    # data processing conveyor.
    my $data_loaded = 1;
    while (1) {
        $data_loaded = $conveyor->load_data if $data_loaded && $conveyor->free_processors;
        if ($conveyor->busy_processors) {
            schedule unless $data_loaded && $conveyor->free_processors;
        } else {
            last unless $data_loaded;
        }
    }
    
    return unless defined wantarray;
    my $result = $conveyor->{output} || [];
    return wantarray? @$result : $result;
}

sub pipeline {
    my $class=shift;
    if (ref($class) eq 'HASH') {
        unshift @_, $class;
        $class = __PACKAGE__;
    }
    my @pipes;
    # init pipes
    my $default_input;
    for my $param (@_) {
        unless (exists $param->{input}) {
            $param->{input} = $default_input or die "You have to specify input for the first pipe";            
        }
        my $pipe = $class->new($param);
        if (ref($pipe->{output}) eq 'ARRAY') {
            $default_input = $pipe->{output};
        }
        push @pipes, $pipe;
    }
    run_pipes(0,@pipes);
    my $result = $pipes[$#pipes]->{output};
    # @pipes=() kills parent
    # as well as its implicit destroying
    # destroy pipes one by one if you want to survive!!! 
    undef $_ for @pipes;
    return unless defined(wantarray);
    return unless $result;
    return wantarray?@$result:$result;
}

sub run_pipes {
    my ($prev_busy,$me,@next) = @_;
    my $me_busy = $me->load_data || $me->busy_processors;
    while ($me_busy) {
        $me->receive_and_merge_data;
        $me_busy = $me->load_data || $me->busy_processors;
        my $next_busy = @next && run_pipes($prev_busy || $me_busy, @next);
        $me_busy ||= $next_busy;
        # get data from pipe if we have free_processors
        return $me_busy if $prev_busy && $me->free_processors;
    }
    return 0;
}

# this should work with Windows NT or if user explicitly set that
my $number_of_cpu_cores = $ENV{NUMBER_OF_PROCESSORS}; 
sub number_of_cpu_cores {
    #$number_of_cpu_cores = $_[0] if @_; # setter
    return $number_of_cpu_cores if $number_of_cpu_cores;
    eval {
        # try unix (linux,cygwin,etc.)
        $number_of_cpu_cores = scalar grep m{^processor\t:\s\d+\s*$},`cat /proc/cpuinfo 2>/dev/null`;
        # try bsd
        ($number_of_cpu_cores) = map m{hw.ncpu:\s+(\d+)},`sysctl -a` unless $number_of_cpu_cores;
    };
    # otherwise it sets number_of_cpu_cores to 1
    return $number_of_cpu_cores || 1;
}

sub set_input_iterator {
    my ($self,$param) = @_;
    my ($input_iterator) = extract_param($param, qw(input));
    if (ref($input_iterator) ne 'CODE') {
        die "array or code reference expected for input_iterator" unless ref($input_iterator) eq 'ARRAY';
        my $queue = $input_iterator;
        $input_iterator = sub {$queue?shift(@$queue):undef};
    }
    $self->{input_iterator} = $input_iterator;
}

sub set_output_iterator {
    my ($self,$param) = @_;
    my ($output_iterator) = extract_param($param, qw(output));
    if (ref($output_iterator) ne 'CODE') {
        my $queue = $output_iterator || [];
        $self->{output} = $queue;
        $output_iterator = sub {push @$queue,$_};
    }
    $self->{output_iterator} = $output_iterator;    
}

sub set_process_iterator {
    my ($self,$param) = @_;
    my $process_data_callback = extract_param($param,qw(process));
    my $main =  $Coro::current;
    $self->{process_iterator} = sub {
        my $data = shift;
        my $item_number = $self->{item_number}++;
        $self->{busy}++;
        my $coro = async {
            local $_ = $data;
            $_ = $process_data_callback->($data);
            $self->{output_iterator}->($_,$item_number);
            $self->{busy}--;
            $main->ready;
        };
    };
}

# loads all free processor with data from input
# return the number of loaded processors
sub load_data {
    my $self = shift;
    my $data = $self->{input_iterator}->();
    return 0 unless defined($data);
    $self->{process_iterator}->($data);
    return 1;
}

sub extract_param {
    my ($param, @alias) = @_;
    return first {defined($_)} map delete($param->{$_}), @alias;
}

sub busy_processors {
    my $self = shift;
    return $self->{busy};
}

sub free_processors {
    my $self = shift;
    return $self->{busy} < $self->{number_of_data_processors};    
}

sub new {
    my ($class, $param) = @_;	
	my $self = {};
    bless $self,$class;
    $self->{number_of_data_processors} = extract_param($param,'number_of_data_processors') || number_of_cpu_cores;
    # item_number & busy
    $self->{$_} = 0 for qw(item_number busy);
    $self->set_input_iterator($param);
    $self->set_output_iterator($param);
    $self->set_process_iterator($param);
    my $not_supported = join ", ", keys %$param;
    die "Parameters are redundant or not supported:". $not_supported if $not_supported;	
	return $self;
}

use Data::Dump qw(dump);
use Time::HiRes qw(time);
my $lt = time;
sub debug {
    $lt=time unless defined($lt);
    #return;
	my ($format,@par) = @_;
	my ($package, $filename, $line) = caller;
	printf STDERR "%s[%5d](%d) $format\n",$filename,(time-$lt)*1000,$line,map {defined($_)?(ref($_)?dump($_):$_):'undef'} @par;
    #<>;
}


1;

=head1 NAME

C<Parallel::DataPipe> - parallel data processing conveyor 

=encoding utf-8

=head1 SYNOPSIS

    use Parallel::DataPipe;
    Parallel::DataPipe::run {
        input => [1..100],
        process => sub { "$_:$$" },
        number_of_data_processors => 100,
        output => sub { print "$_\n" },
    };
    

=head1 DESCRIPTION


If you have some long running script processing data item by item
(having on input some data and having on output some processed data i.e. aggregation, webcrawling,etc)
you can speed it up 4-20 times using parallel datapipe conveyour.
Modern computer (even modern smartphones ;) ) have multiple CPU cores: 2,4,8, even 24!
And huge amount of memory: memory is cheap now.
So they are ready for parallel data processing.
With this script there is an easy and flexible way to use that power.

So what are the benefits of this module?

1) because it uses input_iterator it does not have to know all input data before starting parallel processing

2) because it uses merge_data processed data is ready for using in main thread immediately.

1) and 2) remove requirements for memory which is needed to store data items before and after parallel work. and allows parallelize work on collecting, processing and using processed data.

If you don't want to overload your database with multiple simultaneous queries
you make queries only within input_iterator and then process_data and then flush it with merge_data.
On the other hand you usually win if make queries in process_data and do a lot of data processors.
Possibly even more then physical cores if database queries takes a long time and then small amount to process.

It's not surprise, that DB servers usually serves N queries simultaneously faster then N queries one by one.

Make tests and you will know.

To (re)write your script for using all processing power of your server you have to find out:

1) the method to obtain source/input data. I call it input iterator. It can be either array with some identifiers/urls or reference to subroutine which returns next portion of data or undef if there is nor more data to process.

2) how to process data i.e. method which receives input item and produce output item. I call it process_data subroutine. The good news is that item which is processed and then returned can be any scalar value in perl, including references to array and hashes. It can be everything that Storable can freeze and then thaw.

3) how to use processed data. I call it merge_data. In the example above it just prints an item, but you could do buffered inserts to database, send email, etc.

Take into account that 1) and 3) is executed in main script thread. While all 2) work is done in parallel forked threads. So for 1) and 3) it's better not to do things that block execution and remains hungry dogs 2) without meat to eat. So (still) this approach will benefit if you know that bottleneck in you script is CPU on processing step. Of course it's not the case for some web crawling tasks unless you do some heavy calculations

=head1 SUBROUTINES

=head2 run

This is subroutine which covers magic of parallelizing data processing.
It receives paramaters with these keys via hash ref.

B<input> - reference to array or subroutine which should return data item to be processed.
    in case of subroutine it should return undef to signal EOF.
    In case of array it uses it as queue, i.e. shift(@$array) until there is no data item,
    This behaviour has been introduced in 0.06.
    Also you can use these aliases:
    input_iterator, queue, data
    
    Note: in version before 0.06 it was input_iterator and if reffered to array it remained untouched.
    while new behaviour is to treat this parameter like a queue.
    0.06 support old behaviour only for input_iterator,
    while in the future it will behave as a queue to make life easier

B<process> - reference to subroutine which process data items. they are passed via $_ variable
	Then it should return processed data. this subroutine is executed in forked process so don't
    use any shared resources inside it.
    Also you can update children state, but it will not affect parent state.
    Also you can use these aliases:
    process_data

These parameters are optional and has reasonable defaults, so you change them only know what you do

B<output> - optional. either reference to a subroutine or array which receives processed data item.
    subroutine can use $_ or $_[0] to access data item and $_[1] to access item_number.
	this subroutine is executed in parent thread, so you can rely on changes that it made.
    if you don't specify this parameter array with processed data can be received as a subroutine result.
    You can use this aliseases for this parameter:
    merge_data, merge

B<number_of_data_processors> - (optional) number of parallel data processors. if you don't specify,
    it tries to find out a number of cpu cores
	and create the same number of data processor children.
    It looks for NUMBER_OF_PROCESSORS environment variable, which is set under Windows NT.
    If this environment variable is not found it looks to /proc/cpuinfo which is availbale under Unix env.
    It makes sense to have explicit C<number_of_data_processors>
    which possibly is greater then cpu cores number
    if you are to use all slave DB servers in your environment 
    and making query to DB servers takes more time then processing returned data.
    Otherwise it's optimal to have C<number_of_data_processors> equal to number of cpu cores.

Note: run can also be called like this
    
    my @x2 = Parallel::DataPipe::run([1..100],sub {$_*2});
    
This feature is considered as experimental. Use it at your own risk.

=head2 pipeline
  
pipeline() is a chain of run() (parallel data pipes) executed in parallel
and input for next pipe is implicitly got from previous one.
  
  run {input => \@queue, process => \&process, output => \@out}
  
is the same as
  
  pipeline {input => \@queue, process => \&process, output => \@out}
  
But with pipeline you can create chain of connected pipes and run all of them in parallel
like it's done in unix with processes pipeline.

  pipeline(
    { input => \@queue, process => \&process1},
    { process => \&process2},
    { process => \&process3, output => sub {print "$_\n";} },
  );
  
And it works like in unix - input of next pipe is (implicitly) set to output from previous pipe.
You have to specify input for the first pipe explicitly (see example of parallel grep 'hello' below ).

If you don't specify input for next pipe it is assumed that it is output from previous pipe like in unix.
Also this assumption that input of next pipe depends on output of previous is applied for algorithm
on prioritizing of execution of pipe processors.
As long as the very right (last in list) pipe has input items to process it executes it's data processors.
If this pipe has free processor that is not loaded with data then the processors from previous pipe are executed
to produce an input data for next pipe.
This is recursively applied for all chain of pipes.

Here is parallel grep implemented in 40 lines of perl code:
  
  use List::More qw(part);
  my @dirs = '.';
  my @files;
  pipeline(
    # this pipe looks (recursively) for all files in specified @dirs
    { 
        input => \@dirs,
        process => sub {
            my ($files,$dirs) = part -d?1:0,glob("$_/*");
            return [$files,$dirs];
        },
        output => sub {
            my ($files,$dirs) = @$_;
            push @dirs,@$dirs;# recursion is here
            push @files,@$files;
        },
    },
    # this pipe grep files for word hello
    {
        input => \@files,
        process => sub {
            my ($file) = $_;
            open my $fh, $file;
            my @lines;
            while (<$fh>) {
                # line_number : line
                push @lines,"$.:$_" if m{hello};
            }
            return [$file,\@lines];
        },
        output => sub {
            my ($file,$lines) = @$_;
            # print filename, line_number , line
            print "$file:$_" for @$lines;
        }
    }
  );
  
=head1 HOW parallel pipe (run) WORKS

1) Main thread (parent) forks C<number_of_data_processors> of children for processing data.

2) As soon as data comes from C<input_iterator> it sends it to next child using
pipe mechanizm.

3) Child processes data and returns result back to parent using pipe.

4) Parent firstly fills up all the pipes to children with data and then
starts to expect processed data on pipes from children.

5) If it receives result from chidlren it sends processed data to C<data_merge> subroutine,
and starts loop 2) again.

6) loop 2) continues until input data is ended (end of C<input_iterator> array or C<input_iterator> sub returned undef).

7) In the end parent expects processed data from all busy chidlren and puts processed data to C<data_merge>

8) After having all the children sent processed data they are killed and run returns to the caller.

Note:
 If C<input_iterator> or <process_data> returns reference, it serialize/deserialize data before/after pipe.
 That way you have full control whether data will be serialized on IPC.
 
=head1 SEE ALSO

L<fork|http://perldoc.perl.org/functions/fork.html>

L<subs::parallel>

L<Parallel::Loops>

L<MCE>

L<IO::Pipely> - pipes that work almost everywhere

L<POE> - portable multitasking and networking framework for any event loop

=head1 DEPENDENCIES

Only core modules are used.

if found it uses Sereal module for serialization instead of Storable as the former is more efficient.

=head1 BUGS 

For all bugs please send an email to okharch@gmail.com.

=head1 SOURCE REPOSITORY

See the git source on github
 L<https://github.com/okharch/Parallel-DataPipe>

=head1 COPYRIGHT

Copyright (c) 2013 Oleksandr Kharchenko <okharch@gmail.com>

All right reserved. This library is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 AUTHOR

  Oleksandr Kharchenko <okharch@gmail.com>

=cut
