# find-relations

This program converts SQLite databases into specially encoded files for faster search of relationships between tables.

## Usage

```
find-relations [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  encode  Encode a database.
  search  Search an encoded database.
```

The first step is to encode the database using `encode` command.

Once the encoded file is ready, the `search` command can look for specific values.

### Encode

```
find-relations encode [OPTIONS] FILE [OUTPUT]

Options:                                                                      
  --hash NAME     The hash algorithm to use.  [default: md5]
  --sample ROWS   Encode a random sample of ROWS rows for each table.  [x>=1]
  --ignore-types  Do not encode type information.
  --help          Show this message and exit.
```

Encode a SQLite database `FILE` into a searchable format containing the hashes of each cell's value.

The result file will be saved as `OUTPUT` or as `FILE`.dat.

Always available hash algorithms are: blake2b, blake2s, md5, sha1, sha224, sha256, sha384, sha3_224, sha3_256,
sha3_384, sha3_512, sha512, shake_128, shake_256.


## Search

```
find-relations search [OPTIONS] FILE

Options:
  --value <SQL-TYPE JSON-VALUE>  Search for a specific value.
  --cell <TABLE ROW COLUMN>      Search for the value in a cell.
  --column <TABLE COLUMN>        Search for all values in column.
  --max-results INTEGER          Stop after INTEGER results.  [x>=1]
  --include-null                 Do not skip null values.
  --show-all-results             Do not aggregate results.
  --help                         Show this message and exit.
```

Search for specific values, cells, or columns inside an encoded FILE.

The `--max-results` option cannot be used when searching for multiple values with the `--value` option.

See [`find-relations encode`](#encode) for help on encoding a database.
