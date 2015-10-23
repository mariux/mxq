# mxq
MXQ - mariux64 job scheduling system

## Sources
### Main git repository

https://github.molgen.mpg.de/mariux64/mxq

### github.com clone

https://github.com/mariux/mxq

## Installation
### Install using `GNU make`
```
make 
make install [DESTDIR=...]
```
### Install using `bee`
```
bee init $(bee download git://github.molgen.mpg.de/mariux64/mxq.git) -e
bee update mxq
```
