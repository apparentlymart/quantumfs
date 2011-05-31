
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
use DateTime;

my $repodir;
my $headname;
my $ref_path;
my $mountpoint;
my $repo;
# Buffers for open files, so we won't need to
# keep rewriting the whole tree with each
# individual write.
my %bufs = ();
# Number of times each path has been open,
# so we can throw away our buffer when
# callers are finished with it.
my %openct = ();

sub run {
    my ($class, $new_repodir, $new_headname, $new_mountpoint) = @_;

    $repodir = $new_repodir;
    $headname = $new_headname;
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
	return wantarray ? (undef, reverse @stack) : undef;
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

    return wantarray ? ($current, reverse @stack) : $current;
}

sub write_dir_entry {
    my ($path, %params) = @_;

    my @parts = resolve_path($path);
    my @backchunks = reverse(split(m!/!, $path));

    # If there aren't the right amount of @parts then
    # some parent trees are missing, so don't try to
    # write this thing.
    return undef if @parts != @backchunks;

    my $new_entry = Git::PurePerl::NewDirectoryEntry->new(
	filename => $backchunks[0],
	%params,
    );

    my $chunks = scalar(@parts);

    my $last_tree;

    for (my $i = 0; $i < ($chunks - 1); $i++) {
	my $part = $parts[$i];
	my $chunk = $backchunks[$i];
	my $next_part = $parts[$i + 1];
	my $next_chunk = $backchunks[$i + 1];

	# If $part is undef then we're creating a new
	# entry in the tree. Otherwise, we're rewriting
	# an existing one.
	my @new_entries = ();
	if (defined($part)) {
	    foreach my $entry ($next_part->object->directory_entries) {
		if ($entry->filename eq $chunk) {
		    push @new_entries, $new_entry;
		}
		else {
		    push @new_entries, Git::PurePerl::NewDirectoryEntry->new(
                        filename => $entry->filename,
                        mode => $entry->mode,
                        sha1 => $entry->sha1,
                    );
		}
	    }
	}
	else {
	    foreach my $entry ($next_part->object->directory_entries) {
		push @new_entries, Git::PurePerl::NewDirectoryEntry->new(
                    filename => $entry->filename,
                    mode => $entry->mode,
                    sha1 => $entry->sha1,
                );
	    }
	    push @new_entries, $new_entry;
	}

	$last_tree = Git::PurePerl::NewObject::Tree->new(
            directory_entries => \@new_entries,
        );
	$repo->put_object($last_tree);
	$new_entry = Git::PurePerl::NewDirectoryEntry->new(
	    filename => $next_chunk,
	    mode => 40000,
	    sha1 => $last_tree->sha1,
	);
    }

    return $last_tree;
}

sub update_ref {
    my ($new_tree) = @_;

    my $author = Git::PurePerl::Actor->new(
	name => ".",
	email => ".",
    );
    my $now = DateTime->now();

    my $old_commit = $repo->ref($ref_path);
    unless ($old_commit) {
	warn "Missing ref $ref_path\n";
	return;
    }

    my $new_commit = Git::PurePerl::NewObject::Commit->new(
	tree => $new_tree->sha1,
	parent => $old_commit->sha1,
	author => $author,
	authored_time => $now,
	committer => $author,
	committed_time => $now,
	comment => ".",
    );

    $repo->put_object( $new_commit );
    $repo->update_ref( $headname, $new_commit->sha1 );

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

    warn "getattr for $path yielded ".$entry->sha1;

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

    $bufs{$path} = \($entry->object->content);
    $openct{$path}++;

    return 0;
}

sub mknod {
    my ($path) = @_;

    my @parts = split(m!/!, $path);
    pop @parts;
    my $dir = join('/', @parts);

    if ($dir) {
	my $entry = resolve_path($dir);
	return -ENOENT unless $entry;
	return -ENOTDIR unless $entry->object->kind eq 'tree';
    }

    # Prepare a buffer for this new file
    # ready to be written to.
    my $buf = "";
    $openct{$path}++;
    $bufs{$path} = \$buf;

    return 0;    
}

sub release {
    my ($path) = @_;

    my $entry = resolve_path($path);
    return -ENOENT unless $entry;
    my $kind = $entry->object->kind;
    return -EISDIR if $kind eq 'tree';
    return -EINVAL unless $kind eq 'blob';

    if ((--$openct{$path}) < 1) {
	delete $bufs{$path};
	delete $openct{$path};
    }
}

sub read {
    my ($path, $size, $offset) = @_;

    if (exists $bufs{$path}) {
	my $buf_ref = $bufs{$path};
	return substr($$buf_ref, $offset, $size);
    }
    else {
	# If there's no buffer then somehow
	# we're servicing a read without
	# a prior open, which is not supported.
	return -EBUSY;
    }
}

sub write {
    my ($path, $new_buf, $offset) = @_;

    if (exists $bufs{$path}) {
	my $buf_ref = $bufs{$path};
	my $size = length($new_buf);
	substr($$buf_ref, $offset, $size) = $new_buf;
    }
    else {
	# If there's no buffer then somehow
	# we're servicing a write without
	# a prior open, which is not supported.
	return -EBUSY;
    }
}

sub mkdir {
    my ($path) = @_;

    my $new_tree = Git::PurePerl::NewObject::Tree->new(
	directory_entries => [],
    );
    $repo->put_object($new_tree);

    my $root_tree = write_dir_entry($path,
        mode => 40000,
        sha1 => $new_tree->sha1,
    );

    update_ref($root_tree);

    return 0;
}

# HACK: Override this so that an empty directory will
# yield an empty content, not undef.
sub Git::PurePerl::NewObject::Tree::_build_content {
    my $self = shift;
    my $content = '';
    foreach my $de ( $self->directory_entries ) {
        $content
            .= $de->mode . ' '
            . $de->filename . "\0"
            . pack( 'H*', $de->sha1 );
    }
    $self->content($content);
}

