Workerman
=========

Worker manager (workerman) is a tool that runs worker scripts based on Beanstalkd queue jobs available.

It is written in [Google Go language](http://golang.org/) and can be run either as interpreted script of be statically compiled to get standalone self-contained executable file.

## Compiling/running

The tool may be either run: `go run main.go`

Or compiled: `go build main.go`

## Command line options

`--connect <addr:port>` -- Address and port of the beanstalk server to connect to. If ommited, defaults to 0.0.0.0:11300

`--workers <path/to/directory>` -- Directory path with worker scripts. Default: ./workers/

Delays can be tweaked in source file header.

## Dependencies

For beanstalkd connection it uses https://github.com/nutrun/lentil client library.

## Links

* lentil: https://github.com/nutrun/lentil/

## Copyright & Licence

Software provided as-is. Use it for whatever you want. Modify how you want. See [LICENCE](https://raw.github.com/dmitry-vovk/workerman/master/LICENCE).
