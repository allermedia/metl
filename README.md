Mini-ETL system
---------------

For those who want to process just a few *(or many)* lines from a CSV file *(or others)*.

metl does not daemonize so you need to use cron to provide scheduling; it runs 'stand-alone' - the metl binary is run each time a job is processed.

This is a very early, non-tested or in-use code base (- there is still lots of work [and documentation] to do!), so use at your own peril.

# Current feature set

## Fetching

* From filesystem
* Over HTTP

## Parser

* CSV
** Header row mapping
** Row skipping

## Processing

* Row addition (add extra rows from what the parser finds)
* Configure number of workers

## Outputting

* STDOUT
* MySQL

## Notifcation

* HipChat

# Using the metl

First build a binary.

```
$ make get-deps
$ make build
```

Then move the binary somewhere useful and create an directory for metl (`/etc/metl`).  You can alternatively do this yourself (binary will be at `bin/metl`) or run:

```
$ make install
```

```
$ metl help      
Usage:
  metl [options...] <command> [arg...]

mini ETL system

Options:
  -h, --help                                                                # show help and exit
  -f, --job-files="/metl-jobs/"                                             # Location of job configration files
  -s, --local-storage="/etc/metl/.metl"  # Location for application's file storage (downloaded files, local db etc.)
  -l, --log="debug"                                                         # Log level output (error, warning, info)

Commands:
  run         Run a job
  unlock      Unlock a job
  add         Schedule a new job
  status      Display running job list
  list        List available jobs
  version     Display version information
  help        Display usage information
```

## Sample job file

See the `sample_jobs` folder.

# Developing

You're on your own for now, but thank you for thinking about it! :)
