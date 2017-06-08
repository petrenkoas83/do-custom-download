#!/usr/bin/perl -w
#
#     Do Custom Download 0.4
#     Download a set of packages and sources for a custom deb pool
#     Copyright (C) 2007 Daniel Dickinson <cshore@wightman.ca>
#
#     This program is free software; you can redistribute it and/or modify
#     it under the terms version 2 of the GNU General Public License as 
#     published by the Free Software Foundation.

#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.

#     You should have received a copy of the GNU General Public License
#     along with this program; if not, write to the Free Software
#     Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

# Core modules
use IO::File;
use strict;
use warnings;
use File::Temp; 
use Fcntl qw/F_SETFD F_GETFD/;
use Cwd;

# libcarp-clan-perl
use Carp::Clan;

# libgetopt-mixed-perl
use Getopt::Mixed "nextOption";

# libapt-pkg-perl
use AptPkg::Config '$_config';
use AptPkg::System '$_system';
use AptPkg::Version;


use constant TRUE => 1;
use constant FALSE => 0;

use constant EXIT_OK => 0;
use constant EXIT_BAD_COMMAND_LINE => 1;
use constant EXIT_UNREADABLE_FILE => 2;
use constant EXIT_REPEATED_FIELD => 3;
use constant EXIT_MISSING_FIELD => 4;
use constant EXIT_MULTIPLE_STDIN => 5;
use constant EXIT_BAD_ARCHIVE => 6;
use constant EXIT_MISSING_FIELD_NAME => 7;
use constant EXIT_BAD_SOURCE_FILES_FIELD => 8;
use constant EXIT_STDIN_ERR => 9;
use constant EXIT_FILE_OPEN_FAILED => 10;
use constant EXIT_NO_HIGH_FIELDS => 11;
use constant EXIT_NO_VERSION_FOR_PACKAGE => 12;
use constant EXIT_CHDIR_FAILED => 13;
use constant EXIT_WGET_TEMP_CLEARONEXEC_FAILED => 14;
use constant EXIT_CWD_CHDIR_FAILED => 15;

use constant GERMINATE_HEADER_LINES => 2;

my $pkg_control_dir = '';
my @distros;
my @components;
my @ignore_missing;
my $include_installer = TRUE;
my $from_germinate = FALSE;
my $quiet = FALSE;
my $verbose = 0;
my $source_list_filename;
my $binary_list_filename;
my $pool_dir;
my $archive = 'http://archive.debian.org/debian/';
my %binaries;
my %sources;
my @archs;
my $filename;
my $wget_list_handle;
my $wget_filename;
my %apt_ftparchive_file;
my $download_size = 0;
my $skip_download = FALSE;
my $keep_wget_file = FALSE;

my @required_binary_fields = qw/Package Version Filename Architecture Size/;
my @required_source_fields = qw/Package Version Directory Architecture Files/;

# TODO: _FileList & SourceList for dists/components/Packages.gz

&parse_command_line();

if ((defined $pkg_control_dir) && ($pkg_control_dir ne '')) {
   if (substr ($pkg_control_dir, -1, 1) ne '/') {
      $pkg_control_dir .= '/';
   }
} else {
   $pkg_control_dir = '';
}

if ((defined $pool_dir) && ($pool_dir ne '')) {
   if (substr ($pool_dir, -1, 1) ne '/') {
      $pool_dir .= '/';
   }
} else {
   $pool_dir = '';
}

if ($verbose > 0) {
   print "Base dir for package control files: $pkg_control_dir\n";
}

if ((defined $archive) && ($archive ne '')) {
   if (substr ($archive, -1, 1) ne '/') {
      $archive .= '/';
   }
} else {
   &print_error_exit("You specified an empty or invalid archive for downloads.\n",
      EXIT_BAD_ARCHIVE);
}

if ($verbose > 0) {
   print "Mirror archive for downloads: $archive\n";
}

if (!$quiet) {
   print "Initializing package & source database...";
}

&parse_package_control_files($pkg_control_dir, \@distros, \@components, 
        $include_installer);

if (!$quiet) {
   print "ok\n";
}

if (!$quiet) {
   print "Processing package lists...";
}

if (!$keep_wget_file) {
   $wget_list_handle = new File::Temp();
} else {
   $wget_list_handle = new IO::File $wget_filename, "w";
}   

&process_package_lists($binary_list_filename, $source_list_filename);

if (!$quiet) {
   print "ok\n";
}

if (!$quiet) {
   print "Initiating download...";
}

if (!$skip_download) {
   &do_download();
} else {
   print "skipped\n";
}

sub do_download {
   my $cwd = getcwd();
    
   if ($pool_dir ne '') {
      if (!chdir($pool_dir)) {
         print_error_exit("Unable to change to directory $pool_dir\n", 
            EXIT_CHDIR_FAILED);         
      }
   }
   if (!(fcntl($wget_list_handle, F_SETFD, 0))) {
      print_error_exit("Couldn't clear close-on-exec flag on temp filehandle: $wget_list_handle",
         EXIT_WGET_TEMP_CLEARONEXEC_FAILED);
   }
   my $wgetfile = "/dev/fd/" . fileno($wget_list_handle);
   my $megs = $download_size / 1000000;

   my $wget_verbosity = '';
   
   if ($verbose > 1) {
      $wget_verbosity = '-v';
   } elsif (!$quiet) {
      $wget_verbosity = '-nv';
   } else {
      $wget_verbosity = '-q';
   }
   printf("Downloading %.0f Mb (less previous downloads)\n", $megs);
   my @wgetcmd = ('/usr/bin/wget', '-i', "$wgetfile", "$wget_verbosity", '-nc', '-B', "$archive", '-r', '-nH', '--cut-dirs=1'); 
   system @wgetcmd;
   
   if (!chdir($cwd)) {
      print_error_exit("Unable to return to original directory $cwd after downloading files\n",
         EXIT_CWD_CHDIR_FAILED);
   }
}

sub process_package_lists {
   my $binary_list_filename = shift;
   my $source_list_filename = shift;
   my $package_list_filehandle;   
   my $is_source = FALSE;   
   
   foreach my $filename ($binary_list_filename, $source_list_filename) {
      if (($filename ne '') && ($filename eq $source_list_filename)) {
         $is_source = TRUE;
      }
      if ($filename eq '') {         
         next;
      }
      if ($filename eq '-') {
         $package_list_filehandle = new IO::File;
         if (!($package_list_filehandle->fdopen(fileno(STDIN),"r"))) {
            print_error_exit("Unable to access standard input.\n",
               EXIT_STDIN_ERR);
         }
      } else {
         if (!($package_list_filehandle = new IO::File $filename, "r")) {
            print_error_exit("Uable to open file: $filename.\n",
               EXIT_FILE_OPEN_FAILED);
         }
      }     
      &process_package_list($package_list_filehandle, $is_source);
   }   
}

sub process_package_list {
   my $package_list_filehandle = shift;
   my $is_source = shift;
   my $line;
   my $line_number = 0;
   my $package_name;
   
   while ($line = <$package_list_filehandle>) {
      if ($from_germinate) {
         if ($line_number < GERMINATE_HEADER_LINES) {
            $line_number++;            
            next;
         } elsif (!$is_source) {
            if ($line =~ m/^(.*?)(\s*?\|).*/) {
               $package_name = &trim($1);               
            }
         } else {
            if ($line =~ m/^(.*?)\s+\|.*/) {
               $package_name = &trim($1);
            }
         }
      } else {
         $package_name = &trim($line);
      }
      if ((defined $package_name) && ($package_name ne '')) {
         if ($verbose > 3) {
            print "$package_name ";                     
            if (!$is_source) {
               print ': binary\n';
            } else {
               print ': source\n';
            }
         }      
         &process_package($package_name, $is_source);
      }
   }
}

sub process_package {
   my $package_name = shift;
   my $is_source = shift;
   my $ver_hash;
   my $version;
   my $fields;
   my $highest_fields;
   my $highest_version;
   
   if (!$is_source) {
      $ver_hash = $binaries{$package_name};
   } else {
      $ver_hash = $sources{$package_name};
   }
   
   # initialise the global config object with the default values
   $_config->init;

   # determine the appropriate system type
   $_system = $_config->system;

   # fetch a versioning system
   my $versys = $_system->versioning;

while (($version, $fields) = each (%{$ver_hash})) {
      if (! defined($version)) {
         &print_error_exit("No version for '$package_name'",
            EXIT_NO_VERSION_FOR_PACKAGE);
      }     
      if (! defined($highest_version)) {
         $highest_version = $version;
         $highest_fields = $fields;
      } elsif ($versys->compare($highest_version, $version) > 0 ) {
         if ($verbose > 2) {
            print "ver: $version, high_ver: $highest_version\n";
         }
         $highest_version = $version;
         $highest_fields = $fields;
      }
   }
   if (! defined ($highest_fields)) {      
      &print_error_exit("No highest field for '$package_name'",
         EXIT_NO_HIGH_FIELDS);
   } else {
      if (&filter_package($package_name, $highest_fields, $highest_version, $is_source)) {
         my $filepath;
         my $filehandle;
         my $directory;
         if (!$is_source) {
            if ($verbose > 2) {
               print $archive . ${$highest_fields}{'Filename'} . "\n";   
            }
            $download_size += ${$highest_fields}{'Size'};
            $filepath = ${$highest_fields}{'Filename'};            
            print $wget_list_handle $filepath . "\n";
         } else {
            my @files = split /\s/, ${$highest_fields}{'Files'};
            my @sizes = split /\s/, ${$highest_fields}{'Sizes'};

            foreach my $size (@sizes) {
               $download_size += $size;
            }                        
            
            foreach my $file (@files) {
               $directory = ${$highest_fields}{'Directory'};
               $filepath = $directory . '/' . $file;
               if ($verbose > 2) {
                  print $directory . '/' . $file . "\n";
               }
               if ($verbose > 2) {
                  print $filepath . "\n";
               }
               print $wget_list_handle $filepath . "\n";
            }
         }
         my $dist = ${$highest_fields}{'Distro'};
         my $component = ${$highest_fields}{'Component'};
         my $architecture = ${$highest_fields}{'Architecture'};
         my $installer = ${$highest_fields}{'Installer'};
         
         if ($is_source) {
            my $filename = $dist . '_' . $component . '_source';  
            my @files = split /\s/, ${$highest_fields}{'Files'};
            
            foreach my $file (@files) {
               if ($file =~ m/.*\.dsc$/) {
                  $filepath = $directory . '/' . $file;
                  &emit_ftparchive_list($filepath, $filename);
               }
            }
         } elsif ($installer) {
            foreach my $arch (split /\s/, $architecture) {
               my $filename = $dist . '_' . $component . 
               '_debian-installer_binary-' . $arch;
               &emit_ftparchive_list($filepath, $filename);
            }
         } else {
            foreach my $arch (split /\s/, $architecture) {               
               my $filename = $dist . '_' . $component .
               '_binary-' . $arch;
               &emit_ftparchive_list($filepath, $filename);
            }
         }
      }
   }
}

sub emit_ftparchive_list {
   my $filepath = shift;
   my $filename = shift;
   
   my $filehandle = $apt_ftparchive_file{$filename};
   if (!defined $filehandle) {
      $filehandle = new IO::File $filename, "w";
      $apt_ftparchive_file{$filename} = $filehandle;      
   }
   print $filehandle $filepath . "\n";
}

sub filter_package {
   my $package_name = shift;
   my $field_hash_ref = shift;
   my $version_hash_ref = shift;
   my $is_source = shift;
   my $pkgarch;
   my $cmdarch;
   my $exclude_reason = '';
   my $excluded = FALSE;
   
   {  
      if ( index("@components", ${$field_hash_ref}{'Component'}) == -1) {
         $excluded = TRUE;
         $exclude_reason = "Component ${$field_hash_ref}{'Component'} not included\n";
         last;
      } elsif ($verbose > 3) {
         print "$package_name in included component\n";                  
      }
      
      my $included_arch = FALSE;
      foreach $pkgarch (split /\s/, ${$field_hash_ref}{'Architecture'}) {
         foreach $cmdarch (@archs) {
            if (($pkgarch eq 'any') || ($pkgarch eq $cmdarch)) {
               $included_arch = TRUE;
               last;
            }            
         }
         if ($included_arch) {
            last;
         }
      }
      
      if (!$included_arch) {
         $excluded = TRUE;
         $exclude_reason = "None of archs @{$field_hash_ref}{'Architecture'} included\n";
         last;
      }
      
      my $is_installer = ${$field_hash_ref}{'Installer'};
      if (! defined($is_installer)) {
         $is_installer = FALSE;
      }
      
      if ((!$include_installer) && $is_installer) {
         $excluded = TRUE;
         $exclude_reason = "Installer package but installer packages are being excluded";
         last;
      }
      
      return TRUE;
   }
   
   if ($excluded && ($verbose > 0)) {
      print STDERR "$package_name excluded: $exclude_reason";
   }
   return FALSE;
}

sub trim {
    $_ = $_[0];
    if (defined $_) {
        s/^\s+//;
        s/\s+$//;
    }
    return $_;
}

sub print_error_exit {
   my $error_message = shift;
   my $error_number = shift;
   
   if (!$quiet) {
      print STDERR "$error_message\n";
   }
   exit $error_number;
}

sub parse_command_line {
   my $option;
   my $option_value;
   my $print_help = TRUE;
   my $custom_archs = FALSE;
   
   Getopt::Mixed::init('l=s pkg-control>l','a=s archive>a', 'd=s distro>d',
   'c=s components>c', 'n no-installer>n','g germinate>g', 'q quiet>q', 
   'v verbose>v', 'h help>h', 'k=s architecture>k', 's=s source>s', 
   'b=s binary>b', 'p=s pool>p', 'skip-download', 'keep-wget-file=s');
   while (($option, $option_value) = nextOption()) {
      if ((defined $option) && ($option ne '')) {
         $option_value = &trim($option_value);
         if ($option eq 'a') {
            # Archive mirror to download from
            $archive = $option_value;
            $print_help = FALSE;
         } elsif ($option eq 'l') {
            # Directory which contains the package control files
            $pkg_control_dir = $option_value;
            $print_help = FALSE;
         } elsif ($option eq 'p') {
            # Directory which contains the pool subdirectory
            $pool_dir = $option_value;
            $print_help = FALSE;
         } elsif ($option eq 'd')  {
            # parse comma-separated list of distributions (e.g. dapper,
            # dapper-updates,dapper-security)
            my @distros_split = split /,/, $option_value;
            foreach my $distro (@distros_split) {
               if ((defined $distro) && ($distro ne '')) {
                  chomp $distro;
                  $distro = &trim($distro);
                  push @distros, $distro;
               }
            }
            $print_help = FALSE;
         } elsif ($option eq 'c') {
            # parse comma-separates list of components (e.g. main,
            # restricted,universe)
            my @components_split = split /,/, $option_value;
            foreach my $component (@components_split) {
               if ((defined $component) && ($component ne '')) {
                  push @components, $component;
               }
            }
         } elsif ($option eq 'k') {
            # Architectures to include
            my @cmdarchs = split /,/, $option_value;
            foreach my $arch (@cmdarchs) { 
              if ((defined $arch) && ($arch ne '')) {
                  push @archs, $arch;
               }
            }         
            $custom_archs = TRUE;
            $print_help = FALSE;
         } elsif ($option eq 'n') {
            # Don't download installer packages
            $include_installer = FALSE;
            $print_help = FALSE;
         } elsif ($option eq 'g') {
            # Package list is of the output format generated by 
            # germinated
            $from_germinate = TRUE;
            $print_help = FALSE;
         } elsif ($option eq 'q') {
            # Run with no messages (even errors)
            $quiet = TRUE;
            $print_help = FALSE;
            $verbose = 0;
         } elsif ($option eq 'v') {
            # Display verbose progress messages for debugging purposes.
            # Level of debugging depends on how many times this option is
            # specified
            if (!$quiet) {
               $verbose++;
            } else {
               $verbose = 0;
            }
            $print_help = FALSE;         
         } elsif ($option eq 's') {
            # List of source packages to include
            $source_list_filename = $option_value;
         } elsif ($option eq 'b') {
            # List of binary packages to include
            $binary_list_filename = $option_value;
         } elsif ($option eq 'skip-download') {
            $skip_download = TRUE;
         } elsif ($option eq 'keep-wget-file') {
            $keep_wget_file = TRUE;
            $wget_filename = $option_value;
         } else {
            # Unknown option so show help info
            $print_help = TRUE;
         }
      }
   }
   
   if (!$custom_archs) {
      @archs = qw/i386 all/;
   }
   
   if (!defined $binary_list_filename) {
      $binary_list_filename = '';
   }
   if (!defined $source_list_filename) {
      $source_list_filename = '';
   }
      
   if (($source_list_filename eq '') && ($binary_list_filename eq '')) {
      # No filename for packages to include is an error
      $print_help = TRUE;
   } else {
      # Check filename(s) for package to include for read permission, or for
      # specifying stdin (-) more than once.
      my $use_stdin = FALSE;
      
      foreach my $filename (($binary_list_filename, $source_list_filename)) {         
         if ($filename ne '') {
            if (! -r $filename) {
               if ($filename eq '-') {
                  if ($use_stdin) {
                     &print_error_exit("Error: You specified stdin as more than once.",
                        EXIT_MULTIPLE_STDIN);
                  }
               } elsif (!$quiet) {
                  &print_error_exit('Error: You don\'t have read permissions on \'' . 
                     $filename . "'. Aborting.\n", EXIT_UNREADABLE_FILE);
               }
            }
         }
      }
   }
   
   if ($verbose > 1) {
      print "-----------------------\n";
      print "Commandline parameters:\n";
      print "-----------------------\n";
      print "archive mirror: $archive\n";
      print "directory with pool subdir: $pool_dir\n";  
      print "package control base dir: $pkg_control_dir\n";
      print "distributions: @distros\n";
      print "components: @components\n";
      print "architectures: @archs\n";
      if ($include_installer) {
         print "include installer\n";
      } else {
         print "don't include installer\n";
      }
      if ($from_germinate) {
         print "package list from germinate\n";
      } else {
         print "plain package list (not from germinate)\n";
      }
      if ($binary_list_filename ne '') {
         print "binary package list filename: $binary_list_filename\n";
      }
      if ($source_list_filename ne '') {
         print "source package list filename: $source_list_filename\n";
      }
      if ($keep_wget_file ne '') {
         print "wget filename: $wget_filename\n";
      }
      if ($skip_download) {
         print "skipping download\n";
      }
      print "-----------------------\n";
   }
   
   if ($print_help) {
      print <<'EOT';
Custom Apt Pool 0.4
Download a set of packages and sources for a custom deb pool
Copyright (C) 2007 Daniel Dickinson <cshore@wightman.ca>
Usage: do-custom-download.pl [options] [-a archive] -d=distro[,distro[,...]] 
       -c=component[,...] -b binary-package-list-file -s src-package-list-file
-a mirror --archive=mirror : Mirror and root dir from which to download
     packages.  (e.g http://archive.debian.org/debian).     
-l path --pkg-control=path : Where to look for package control files (relative
     to current directory)     
-d distro --distro=distro : Comma-separated list of distributions (e.g 
     -d sarge,sarge-security).  At least one is required     
--no-installer : Don't include installer packages
-c component,.. --component=... : Comma-separated list of components to
      include (e.g. -c=main,contrib).      
-k architecture --architecture=architecture : Comma-separated lists of 
     architectures for which to include packages. Defaults to i386,all
-g --from-germinate : Package list is from germinate
-p --pool : Directory containing the pool subdirectory
-q --quiet : No messages, not even error (exit code only)
-v --verbose : Display debugging messages 
This program is free software; you can redistribute it and/or modify it under
the terms of the version 2 of the GNU General Public License as published by
the Free Software Foundation.
EOT

      Getopt::Mixed::cleanup();
      exit EXIT_BAD_COMMAND_LINE;
   }
   Getopt::Mixed::cleanup();
}

sub parse_package_control_files { 
   my $base_dir = shift;
   my $distros_ref = shift;
   my $components_ref = shift;
   my $include_installer = shift;
   
   my $distro;
   my $component;
   my $value;
   my $filename;
   
   # Package control files are of the form distribution_component_controltype
   # where controltype is Packages, InstallerPackages, or Sources
   
   # So we iterate through the distros and components looking for readable files to parse
   # (in the base directory specified on the command line, or (if non specified) the current
   # directory).
   foreach $distro (@{$distros_ref}) {
      foreach $component (qw/main contrib/) {
         $filename = $base_dir . $distro . '_' . $component . '_Packages';
         if (-r $filename) {
            if ($verbose > 0) {
               print "Parsing $filename\n";
            }
            # FALSE, FALSE means, Not installer, not source
            &parse_package_control_file($filename, $distro, $component, FALSE, FALSE);
         } elsif ($verbose > 0) {
            print "Couldn't read $filename\n";
         }
         if ($include_installer) {
            $filename = $base_dir . $distro . '_' . $component . '_InstallerPackages';
            if (-r $filename) {
               if ($verbose > 0) {
                  print "Parsing $filename\n";
               }
               # TRUE, FALSE means, installer, not source
               &parse_package_control_file($filename, $distro, $component, TRUE, FALSE);
            } elsif ($verbose > 0) {
               print "Couldn't read $filename\n";
            }
         }
         $filename = $base_dir . $distro . '_' . $component . '_Sources';
         if (-r $filename) {
            if ($verbose > 0) {
               print "Parsing $filename\n";
            }
            # FALSE, TRUE means, not installer, is source
            # source control files don't list installer udebs generated
            &parse_package_control_file($filename, $distro, $component, FALSE, TRUE);
         } elsif ($verbose > 0) {
            print "Couldn't read $filename\n";
         }
      }               
   }
}

sub parse_package_control_package {
   my $package_hash_ref = shift;
   my $dist = shift;
   my $component = shift;
   my $is_installer = shift;
   my $is_source = shift;
   my $line_number = shift;   
   my @required_fields;
   my %pkg_hash;
   my %ver_hash;
   my %field_hash;
   my $ver_hash_ref;
   my $version;
   my %pkg_archs;
      
   if ($is_source) {
      @required_fields = @required_source_fields;
   } else {
      @required_fields = @required_binary_fields;
   } 
   
   $version = ${$package_hash_ref}{'Version'};

   # Make sure all required fields are present in the package record
   foreach my $required_field (@required_fields) {
      if ((! defined ${$package_hash_ref}{$required_field}) || 
         (${$package_hash_ref}{$required_field} eq '')) {
         &print_error_exit("Missing required field '$required_field' at or near line: $line_number",
            EXIT_MISSING_FIELD);
      }
   }
   
   if (!$is_source) {
      if ((! defined ${$package_hash_ref}{'Source'}) || (${$package_hash_ref}{'Source'} eq '')) {
         ${$package_hash_ref}{'Source'} = ${$package_hash_ref}{'Package'};
      }
      
      # For fields that shouldn't have a space (like source package name) but
      # sometimes do (packages for which gcc-defaults is in the Source 
      # field, for instance), match up to but not including the first space
      # and make this our field value
      # Look for the special case of package_name<whitespace>(version) like
      # gcc-defaults, if a match note the version as well as field value, otherwise
      # only field value
      my $field_value = ${$package_hash_ref}{'Source'};
      my $source_version;
      
      if ($field_value =~ m/^(.+)\s*\((.+)\)/) {
         $field_value = &trim($1);
         $source_version = &trim($2);
         if ($verbose > 2) {
            print "${$package_hash_ref}{'Package'} has source name '$field_value' and source version '$source_version'\n";         
         }
      } else {
         $source_version = $version;
      }      

      # Create hash with contents of fields we want
      $field_hash{'Source'} = $field_value;
      $field_hash{'SourceVersion'} = $source_version;
      $field_hash{'Distro'} = $dist;
      $field_hash{'Component'} = $component;
      $field_hash{'Architecture'} = ${$package_hash_ref}{'Architecture'};
      $field_hash{'Filename'} = ${$package_hash_ref}{'Filename'};
      $field_hash{'Installer'} = $is_installer;
      $field_hash{'Size'} = ${$package_hash_ref}{'Size'};
      
      # Get current hash with all versions of the current package
      $ver_hash_ref = $binaries{${$package_hash_ref}{'Package'}};
   
      if (defined $ver_hash_ref) {
         # If version hash exists, add current version to it (or replace
         # existing version if version string identical
         ${$ver_hash_ref}{$version} = \%field_hash;
      } else {
         # Otherwise create new hash and make it the version hash 
         # for the current package
         $ver_hash{$version} = \%field_hash;      
         $binaries{${$package_hash_ref}{'Package'}} = \%ver_hash;
      }
   } else {
      my @files;
      my @sizes;
      my $has_file = FALSE;
      
      foreach my $file_line (split /\n/, ${$package_hash_ref}{'Files'}) {
         if ($verbose > 3) {
            print "multi-line raw: $file_line\n";
         }
         if ($file_line =~ m/[a-zA-Z0-9]{32}\s([0-9]+)\s(.+)/) {
            push @files, $2;
            push @sizes, $1;
            $has_file = TRUE;
            if ($verbose > 3) {
               print "Found file '$1'\n";
            }
         }
      }
      
      if (!$has_file) {
         &print_error_exit("Invalid Files: field (${$package_hash_ref}{'Files'})",
         EXIT_BAD_SOURCE_FILES_FIELD);
      }
      
      # Create hash with contents of fields we want
      $field_hash{'Directory'} = ${$package_hash_ref}{'Directory'};
      $field_hash{'Architecture'} = ${$package_hash_ref}{'Architecture'};
      $field_hash{'Distro'} = $dist;
      $field_hash{'Component'} = $component;
      $field_hash{'Files'} = "@files";
      $field_hash{'Sizes'} = "@sizes";
      
      # Get hash with all versions of the current package
      $ver_hash_ref = $sources{${$package_hash_ref}{'Package'}};
   
      if (defined $ver_hash_ref) {
         # If version hash exists, add current version, or replace 
         # current version (if version strings identical)
         ${$ver_hash_ref}{$version} = \%field_hash;
      } else {
         # Otherwise create new hash and make it the version hash 
         # for the current package
         $ver_hash{$version} = \%field_hash;      
         $sources{${$package_hash_ref}{'Package'}} = \%ver_hash;
      }
   }
   
   if ($verbose > 3) {
      my $field;
      my $value;
      print "Package: ${$package_hash_ref}{'Package'}\n";
      print "Version: $version\n";
      while (($field, $value) = each (%field_hash)) {
         print "$field: $value\n";
      }
   }
}

sub parse_package_control_file {
   my $package_control_filename = shift;
   my $dist = shift;
   my $component = shift;
   my $is_installer = shift;
   my $is_source = shift;
   my $packages_control_handle = new IO::File $package_control_filename, "r";


   my $line;
   my $line_number = 0;
   my $package_name = '';
   my @line_array;
   my %package_hash;
   
   my $field_name = '';
   my $field_value = '';
   my $new_package = TRUE;
   my $package_number = 0;

   if ($verbose > 3) {
      print "Package#: $package_number\n";
   }
      
   # Parse the control file a line at a time
   while ($line = <$packages_control_handle>) {
      chomp $line;
      $line_number++;
      
      # In the Packages and Sources control files an initial space on a line means that the line
      # is the part of a multi-line field.
      my $first_space = index($line, ' ');
      
      # If the current line is an empty line, or not the continuation of a multi-line field,
      # then the line immediately prior was the completion of a field (and start for single-line fields)
      # We therefore record the field and value for this package.
      if ((! defined $line) || ($line eq '') || (defined $first_space) && ($first_space != 0)) {
         if ((defined $field_name) && ($field_name ne '')) {
            # If we already have a field of this name something is wrong
            if ((defined $package_hash{$field_name}) && ($package_hash{$field_name} ne '')) {
               &print_error_exit("Repeated field $field_name at line $line_number in $package_control_filename",
                  EXIT_REPEATED_FIELD);
            } else {
               # Otherwise record the field name and value
               $package_hash{$field_name} = $field_value;
            }
         } elsif (!$new_package) {
            &print_error_exit("Missing field name at line $line_number in $package_control_filename",
            EXIT_MISSING_FIELD_NAME);
         }
      }     
      $new_package = FALSE;
      
      # Packages are always separated by a blank line, so when we encounter a blank line we
      # process the package information gathered since the last blank line.
      if ((! defined $line) || ($line eq '')) {
         &parse_package_control_package(\%package_hash, $dist, $component,
            $is_installer, $is_source, $line_number);
         $field_name = '';
         $field_value = '';
         $new_package = TRUE;
         %package_hash = ();
         if ($verbose > 3) {
            print "Package#: $package_number\n";
         }
         $package_number++;
         next;
      }      
      
      # Lines beginning with a space ought to be the continuation of a multi-line field
      if ($first_space == 0) {
         # So if we hit such a line, keep the current field name, and add this line to the value of
         # of the field (as a line)
         $line = &trim($line);
         $field_value .= "$line\n";
      } else {
         # Otherwise, the field ought to be 'fieldname: value', so split at the colon-space
         # and call the first item the fieldname, and the rest (space separated), the field value
         @line_array = split /:\s/, $line;
         $field_name = &trim(shift @line_array);
         $field_value = &trim("@line_array");
         
         if ($field_value eq '') {
            if (substr($line, -1, 1) eq ':') {
               $field_name = substr($line, 0, length($line) - 1);
               if ($verbose > 3) {
                  print "Found multi-line field '$field_name'\n";
               }
            }
         }           
      }      
   }
   close $packages_control_handle;
}
