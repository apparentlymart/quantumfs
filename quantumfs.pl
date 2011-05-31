
=head1 NAME

quantumfs - Version-controlled FileSystem built on git

=head1 SYNOPSIS

    Usage: quantumfs <command>
    
        - mount <repodir> <headname> <mountpoint>

=cut

use strict;
use warnings;

use Data::Dumper;
use Pod::Usage;

my $cmd = shift || pod2usage(1);

my $method_name = "cmd_".$cmd;
if (QuantumFS->can($method_name)) {
    QuantumFS->$method_name(@ARGV);
}

exit(0);

package QuantumFS;

use Pod::Usage;

sub cmd_mount {
    my ($class, $repodir, $headname, $mountpoint) = @_;

    pod2usage(2) unless defined($mountpoint);

    QuantumFS::FS->run($repodir, $headname, $mountpoint);

}

package QuantumFS::FS;

use Fuse;
use Git::PurePerl;
use POSIX qw(ENOENT EISDIR EROFS ENOTDIR EBUSY EINVAL O_WRONLY O_RDWR);
use Fcntl qw(S_IFREG S_IFDIR S_IFLNK);

my $repodir;
my $ref_path;
my $mountpoint;
my $repo;

sub run {
    my ($class, $new_repodir, $new_headname, $new_mountpoint) = @_;

    $repodir = $new_repodir;
    $ref_path = 'refs/heads/'.$new_headname;
    $mountpoint = $new_mountpoint;

    $repo = Git::PurePerl->new(gitdir => $new_repodir);

    my @names = qw(getattr readlink getdir mknod mkdir unlink rmdir symlink
		    rename link chmod chown truncate utime open read write statfs
		    flush release fsync setxattr getxattr listxattr removexattr);

    my %param = ();
    foreach my $func (@names) {
	no strict 'refs';
	if (__PACKAGE__->can($func)) {
	    warn "Hooking up $func";
	    $param{$func} = \&{__PACKAGE__ . "::" . $func};
	}
    }

    warn Data::Dumper::Dumper(\%param);

    Fuse::main(
        debug => 1,
        mountpoint => $mountpoint,
        mountopts => "allow_other",
	%param,
    );
}

sub resolve_path {
    my ($path) = @_;

    my @chunks = split(m!/!, $path);
    my @stack = ();

    shift @chunks;

    warn Data::Dumper::Dumper(\@chunks);
    
    my $err = sub {
	my $err = shift;
	warn "$err\n";
	return wantarray ? (undef, \@stack) : undef;
    };

    my $commit = $repo->ref($ref_path);
    return $err->("Missing ref $ref_path") unless $commit;

    # Synthesize a node representing the root directory
    my $current = Git::PurePerl::DirectoryEntry->new(
	mode => 40000,
	filename => '',
	sha1 => $commit->tree_sha1,
	git => $repo,
    );

    foreach my $chunk (@chunks) {
	return $err->("no more trees")
	    unless $current && UNIVERSAL::isa($current->object, 'Git::PurePerl::Object::Tree');

	if ($chunk eq '.') {
	    # Just skip it
	    next;
	}
	elsif ($chunk eq '..') {
	    # Pop the stack
	    $current = pop @stack;
	}
	else {
	    # Look in the current tree for a matching entry
	    push @stack, $current;

	    my $tree = $current->object;
	    ($current) = grep { $_->filename eq $chunk } @{$tree->directory_entries};	    
	}
    }

    return wantarray ? ($current, \@stack) : $current;
}

sub getdir {
    my ($path) = @_;

    my $entry = resolve_path($path);
    return -ENOENT unless $entry;
    return -ENOTDIR unless $entry->object->kind eq 'tree';

    my @ret = map { $_->filename } $entry->object->directory_entries;

    return ('.', '..', @ret, 0);
}

sub readlink {
    my ($path) = @_;

    my $entry = resolve_path($path);
    return -ENOENT unless $entry;
    return -EINVAL unless $entry->object->kind eq 'blob';

    return $entry->object->content;
}

sub getattr {
    my ($path) = @_;

    my $entry = resolve_path($path);
    return -ENOENT unless $entry;

    my $now = time();

    return (
	0,                       # device number
	0,                       # inode
	oct($entry->mode),       # mode
	0,                       # number of hard links
	$<,                      # user id
	$(,                      # group id
	0,                       # device identifier
	$entry->object->size,    # size
	$now,                    # atime
	$now,                    # mtime
	$now,                    # ctime
	1024,                    # preferred block size
	1,                       # number of blocks
    );
}

sub open {
    my ($path, $flags, $fileinfo) = @_;

    my $entry = resolve_path($path);
    return -ENOENT unless $entry;
    my $kind = $entry->object->kind;
    return -EISDIR if $kind eq 'tree';
    return -EINVAL unless $kind eq 'blob';

    return 0;
}

sub read {
    my ($path, $size, $offset) = @_;

    my $entry = resolve_path($path);
    return -ENOENT unless $entry;
    return -EINVAL unless $entry->object->kind eq 'blob';

    my $content = $entry->object->content;

    return substr($content, $offset, $size);
}
