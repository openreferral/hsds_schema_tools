# HSDS Schema Tool.

Tools for woring with HSDS schemas.


## Install

- Clone this repo
- Optionally make a python virtualenv

```
cd hsds_schema_tools
pip install .
```

## Usage

```
hsds_schema.py --help
```

Returns

```
Usage: hsds_schema.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  clean-datapackage
  compile-schemas
  datapackage-to-schemas
  flatten
  schemas-to-csv
  schemas-to-datapackage
  schemas-to-example
  unflatten
```

### Clean Datapackage

Takes a ```datapackage.json``` and returns one reformatted with consistant indentation and whitespace.
Also makes sure contrains keys always exists and contain `required` and `unique` keys.
Will overwrite `datapackage.json` inplace.

Example:
```
hsds_schema.py clean-datapackage datapackage.json
```

### Flatten

Takes a ```datapackage.json``` and converts it into a CSV representation of the datgapackage.  This can be useful for editing in the flattened form.  Requires an output directory and will delete directory if it already exists.

Example:
```
hsds_schema.py flatten datapackage.json output_dir
```

### Unflatten

Takes a directory of CSV files created by `flatten` command and converts it back into a datapackage. It prints the results so will need to save to a file.

Example:
```
hsds_schema.py unflatten output_dir > datapackage.json
```


### Datapackage to schemas

Gets a hsds datapackage and convert it to a directory of json-schemas for use in hsds 3.0/

Example:
```
python hsds_schema.py datapackage-to-schemas latest/datapackage.json schemas
```


### Schemas to datapackage

Gets a direcory of hsds json-schemas and converts them into a datapackage.json

Example:
```
python hsds_schema.py schemas-to-datapackage schemas > new_datapackage.json

```

### Schemas to csv

Makes a csv representation of the schema directory

Example:
```
python hsds_schema.py schemas-to-csv schemas > schema.csv
```

### Compile schemas

Makes Compiled version of schemas

Example:
```
python hsds_schema.py compile-schemas schema_directory output_directory 
```

### Generate examples

Makes examples from schema files. 

Examples:
```
python hsds_schema.py schemas-to-examples schema_directory service > service_example.json 
python hsds_schema.py schemas-to-examples --simple schema_directory service > simple_service_example.json

python hsds_schema.py schemas-to-examples schema_directory organization > organization_example.json 
python hsds_schema.py schemas-to-examples --simple schema_directory organization > simple_organization_example.json
```

### profile-all

Runs all actions required to generate a profile from HSDS Schemas + defined changes. Requires a URI.

Example:

```
hsds_schema.py profile-all https://github.com/openreferral/hsds_example_profile --clean
```
