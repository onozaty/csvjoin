
It has been moved to the following project.

* https://github.com/onozaty/csvt

# csvjoin

Join CSV

## Usage

```
$ csvjoin -1 parent.csv -2 child.csv -c id -o joined.csv
```

The arguments are as follows.

```
Usage of csvjoin:
  -1, --first string    First CSV file path
  -2, --second string   Second CSV file path
  -c, --column string   Name of the column to use for the join
  -o, --output string   Output CSV file path
  -h, --help            Help
```
