# Obslytics (Observability Analytics)

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/thanos-community/obslytics)
[![Latest Release](https://img.shields.io/github/release/thanos-community/obslytics.svg?style=flat-square)](https://github.com/thanos-community/obslytics/releases/latest)
[![CI](https://github.com/thanos-community/obslytics/workflows/go/badge.svg)](https://github.com/thanos-community/obslytics/actions?query=workflow%3Ago)
[![Go Report Card](https://goreportcard.com/badge/github.com/thanos-community/obslytics)](https://goreportcard.com/report/github.com/thanos-community/obslytics)
[![Slack](https://img.shields.io/badge/join%20slack-%23analytics-brightgreen.svg)](https://slack.cncf.io/)

## Usage

[embedmd]:# (bingo-help.txt $)
```$
bingo: 'go get' like, simple CLI that allows automated versioning of Go package level binaries (e.g required as dev tools by your project!)
built on top of Go Modules, allowing reproducible dev environments.

The key idea is that 'bingo' allows to easily maintain a separate, nested Go Module for each binary. By default, it will keep it '.bingo/<tool>.mod'
This allows to correctly pin the tool without polluting the main go module or other's tool module.

For detailed examples see: https://github.com/bwplotka/bingo

'bingo' supports following commands:

Commands:

  get <flags> [<package or binary>[@version1 or none,version2,version3...]]

Similar to 'go get' you can pull, install and pin required 'main' (buildable Go) package as your tool in your project.

'bingo get <repo/org/tool>' will resolve given main package path, download it using 'go get -d', then will produce directory (controlled by -moddir flag) and put
separate, specially commented module called <tool>.mod. After that, it installs given package as '$GOBIN/<tool>-<Version>'.

Once installed at least once, 'get' allows to reference the tool via it's name (without Version) to install, downgrade, upgrade or remove.
Similar to 'go get' you can get binary with given Version: a git commit, git tag or Go Modules pseudo Version after @:

'bingo get <repo/org/tool>@<Version>' or 'bingo get <tool>@<Version>'

'get' without any argument will download and get ALL the tools in the moddir directory.
'get' also allows bulk pinning and install. Just specify multiple versions after '@':

'bingo get <tool>@<version1,version2,tag3>'

Similar to 'go get' you can use -u and -u=patch to control update logic and '@none' to remove binary.

Once pinned apart of 'bingo get', you can also use 'go build -modfile .bingo/<tool>.mod -o=<where you want to build> <tool package>' to install
correct Version of a tool.

Note that 'bingo' creates additional useful files inside -moddir:

* '<moddir>/Variables.mk': When included in your Makefile ('include <moddir>/Variables.mk'), you can refer to each binary
using '$(TOOL)' variable. It will also  install correct Version if missing.
* '<moddir>/variables.env': When sourced ('source <moddir>/variables.env') you can refer to each binary using '$(TOOL)' variable.
It will NOT install correct Version if missing.

  -go string
    	Path to the go command. (default "go")
  -insecure
    	Use -insecure flag when using 'go get'
  -moddir string
    	Directory where separate modules for each binary will be maintained. Feel free to commit this directory to your VCS to bond binary versions to your project code. If the directory does not exist bingo logs and assumes a fresh project. (default ".bingo")
  -n string
    	The -n flag instructs to get binary and name it with given name instead of default, so the last element of package directory. Allowed characters [A-z0-9._-]. If -n is used and no package/binary is specified, bingo get will return error. If -n is used with existing binary name, copy of this binary will be done. Cannot be used with -r
  -r string
    	The -r flag instructs to get existing binary and rename it with given name. Allowed characters [A-z0-9._-]. If -r is used and no package/binary is specified or non existing binary name is used, bingo will return error. Cannot be used with -n.
  -u	The -u flag instructs get to update modules providing dependencies of packages named on the command line to use newer minor or patch releases when available.
  -upatch
    	The -upatch flag (not -u patch) also instructs get to update dependencies, but changes the default to select patch releases.
  -v	Print more'


  list <flags> [<package or binary>]

List enumerates all or one binary that are/is currently pinned in this project. It will print exact path, Version and immutable output.

  -moddir string
    	Directory where separate modules for each binary is maintained. If does not exists, bingo list will fail. (default ".bingo")
  -v	Print more'


  Version

Prints bingo Version.
```

## Contributing

Any contributions are welcome! Just use GitHub Issues and Pull Requests as usual.
We follow [Thanos Go coding style](https://thanos.io/contributing/coding-style-guide.md/) guide.

## Initial Author

[@bwplotka](https://bwplotka.dev)