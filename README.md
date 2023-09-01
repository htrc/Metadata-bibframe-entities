[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/htrc/Metadata-marcjson2bibframexml/ci.yml?branch=main)](https://github.com/htrc/HTRC-FeatureExtractor/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/htrc/Metadata-marcjson2bibframexml/graph/badge.svg?token=U7QSqR9yri)](https://codecov.io/github/htrc/Metadata-marcjson2bibframexml)
[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/htrc/Metadata-marcjson2bibframexml?include_prereleases&sort=semver)](https://github.com/htrc/Metadata-marcjson2bibframexml/releases/latest)

# Metadata-bibframe-entities
Used to extract entities from the BIBFRAME-XML for purposes of enrichment from external sources

# Build
* To generate a package that can be invoked via a shell script, run:  
  `sbt stage`  
  then find the result in `target/universal/stage/` folder.
* To generate a distributable ZIP package, run:  
  `sbt dist`  
  then find the result in `target/universal/` folder.

# Run
```
bibframe-entities
  -l, --log-level  <LEVEL>    (Optional) The application log level; one of INFO,
                              DEBUG, OFF (default = INFO)
  -n, --num-partitions  <N>   (Optional) The number of partitions to split the
                              input set of HT IDs into, for increased
                              parallelism
  -o, --output  <DIR>         Write the output to DIR
      --spark-log  <FILE>     (Optional) Where to write logging output from
                              Spark to
  -h, --help                  Show help message
  -v, --version               Show version of this program

 trailing arguments:
  input (required)   The path to the folder containing the Bibframe XML sequence
                     files to process
```
