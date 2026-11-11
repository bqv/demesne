use strict; use warnings;
my $dir = "/home/user/var/model/aux";
my $sockdir = "/home/user/tmp/hnet/models";
my @order; my %r;
my ($cur, $glob);
open my $fh, "<", "$dir/models.ini" or die $!;
while (<$fh>) {
  chomp; s/^\s+|\s+$//g; next if $_ eq "" or /^#/ or /^;/;
  if (/^\[(.+)\]$/) { $cur = $1; if ($cur eq "*") { $glob = 1; next } $r{$cur} = {}; push @order, $cur; $glob = 0; next }
  next unless /^([a-z-]+)\s*=\s*(.*)$/; my ($k,$v) = ($1,$2);
  if ($glob) { $r{_global}{$k} = $v; next }
  $r{$cur}{$k} = $v if $cur;
}
close $fh;
my %g = %{ $r{_global} || {} };
my $chat = "";
for my $n (@order) {
  my $m = $r{$n};
  my $gguf = $m->{model} // next;
  my $path = $gguf =~ m{^/} ? $gguf : "$dir/$gguf";
  my $bytes = -s $path;
  my $mb = $bytes ? int($bytes/1048576) + 900 : 4000;
  my $idle = ($n eq $chat) ? "45m" : "5m";
  my @a = ("-m", $path, "--alias", $n, "--host", "$sockdir/$n.sock");
  push @a, ("-c", $m->{"ctx-size"}) if $m->{"ctx-size"};
  push @a, ("-ngl", $m->{"n-gpu-layers"} // $g{"n-gpu-layers"} // 99);
  push @a, ("-np", $m->{parallel} // 1);
  push @a, ("--cache-type-k", $m->{"cache-type-k"}) if $m->{"cache-type-k"};
  push @a, ("--cache-type-v", $m->{"cache-type-v"}) if $m->{"cache-type-v"};
  push @a, ("--chat-template-file", "$dir/" . $m->{"chat-template-file"}) if $m->{"chat-template-file"};
  push @a, ("--mmproj", "$dir/" . $m->{mmproj}) if $m->{mmproj};
  push @a, ("--reasoning-format", $m->{"reasoning-format"}) if $m->{"reasoning-format"};
  push @a, ("--jinja") if ($m->{jinja} // "") =~ /^(1|on|true)$/;
  push @a, ("--temp", $m->{temp}) if $m->{temp};
  push @a, ("--top-p", $m->{"top-p"}) if $m->{"top-p"};
  push @a, ("--cache-ram", $m->{"cache-ram"}) if $m->{"cache-ram"};
  push @a, ("--device", "CUDA0");
  my $args = join " ", @a;
  my $hi = int($mb/1024) + 4; my $mx = int($mb/1024) + 6;
  print "[$n]\n";
  print "url = unix:$sockdir/$n.sock\n";
  print "bin = /usr/bin/llama-server\n";
  print "args = $args\n";
  print "models = $n\n";
  print "group = gpu\n";
  print "policy = on-demand\n";
  print "vram_mb = $mb\n";
  print "idle = $idle\n";
  print "memory_high = ${hi}G\n";
  print "memory_max = ${mx}G\n\n";
}
print "[llama]\nenabled = false\n";
