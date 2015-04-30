# Misc

* handle hanging jobs
* code cleanup .. seperate out parser, outputting elements etc.  Things are way too messy

# Testing

# Metl.go

* missing OpenJobFile
* reusable file reading/writing routines

# Job

* refactor access to lock file, and how it is read (dupe code in status/Job regarding parsing of lock file)
* Add failure threshold checks and alerting (% of rows processed)
* File downloading should rename the previously downloaded file

# Commands

* Errors need to be handled by the calling code.
* Job files test command (metl test <jobname>) - for building new jobs with full variable debug output (+ config syntax check (adding new columns))

## Add

* Actually add job to crontab

# Outputting

* Currently can only do inserts
* MySQL - missing table cleanup options
