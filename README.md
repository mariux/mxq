# MXQ - mariux64 job scheduling system
- by Marius Tolzmann <marius.tolzmann@molgen.mpg.de> 2013-2015
- and Donald Buczek <buczek@molgen.mpg.de> 2015-2015

## Sources
### Main git repository

https://github.molgen.mpg.de/mariux64/mxq

### github.com clone

https://github.com/mariux/mxq

## Installation
### Install using `GNU make`
```
make
make install
```
```
make PREFIX=...
make PREFIX=... [DESTDIR=...] install
```
### Install using `bee`
```
bee init $(bee download git://github.molgen.mpg.de/mariux64/mxq.git) --execute
bee update mxq
```
```
bee init $(bee download git://github.molgen.mpg.de/mariux64/mxq.git) --prefix=... --execute
bee update mxq
```

## Initial setup
Definitions of the tables and triggers for the MySQL database can be found in
[mysql/create_tables.sql](https://github.molgen.mpg.de/mariux64/mxq/blob/master/mysql/create_tables.sql)
Be sure to create those once and check the same 
[directory for alter_tables*.sql`](https://github.molgen.mpg.de/mariux64/mxq/blob/master/mysql/)
files when upgrading. 


## Development builds
The `devel` target in the Makefile will enable all devolopment features
by defining `MXQ_DEVELOPMENT` when compiling C sources.

```
make clean
make devel PREFIX=/path/to/test
make install PREFIX=/path/to/test
```

### Differences to production builds
Some new features and improvements are enabled in development builds.
Those features may not be tested for every situation yet and may result
in database corruption and/or failing jobs.

#### changed `mxqd` default options
In devolopment builds `--no-log` is default (enable loggin with `--log`)

#### Development database access
Devolopment builds default to use `[mxqdevel]` groups from mysql config files 
for servers and clients (instead of the default `[mxqclient]` and `[mxqd]` groups)
