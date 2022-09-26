#!/usr/bin/env python3

import csv
import os
import json
import click
import pathlib 
import flatterer
import glob

FILES = ['metadata', 'tables', 'fields', 'foreign_keys']


def unflatten_dict(obj, file):
    output = {}

    if file == 'fields':
        for heading in ["constraints_required", "constraints_unique"]:
            value = obj[heading]
            new_value = ""
            if value == 'true':
                new_value = True
            if value == 'false':
                new_value = False
            obj[heading]  = new_value
        enum = obj['constraints_enum']
        if enum:
            obj['constraints_enum'] = enum.split(",")

    for key, value in obj.items():
        if value == "":
            continue
        cur_output = output
        parts = key.split('_')

        for num, part in enumerate(parts):
            if num + 1 == len(parts):
                cur_output[part] = value
            else:
                new_output = cur_output.get(part, {})
                cur_output[part] = new_output
                cur_output = new_output
    
    return output

                
def unflatten_datapackage(directory):
    directory = pathlib.Path(directory)

    csv_data = {}

    for file in FILES:
        file_path = directory / f'{file}.csv'
        with open(file_path) as fd:
            reader = csv.DictReader(fd)
            csv_data[file] = list(unflatten_dict(row, file) for row in reader)
    
    datapackage = csv_data['metadata'][0]

    tables_indexed = {table['name']: table for table in csv_data['tables']}

    for field in csv_data['fields']:
        table = field.pop("table")
        field_list = tables_indexed[table]['schema'].get('fields', [])
        tables_indexed[table]['schema']['fields'] = field_list
        field_list.append(field)

    for fk in csv_data['foreign_keys']:
        table = fk.pop("table")
        fk_list = tables_indexed[table]['schema'].get('foreignKeys', [])
        tables_indexed[table]['schema']['foreignKeys'] = fk_list
        fk_list.append(fk)
    
    datapackage['resources'] = list(tables_indexed.values())

    return datapackage


@click.group()
def cli():
    pass


@cli.command()
@click.argument('datapackage')
@click.argument('directory')
def flatten(datapackage, directory):
    flatterer.flatten(datapackage, directory, fields_csv="fields.csv", tables_csv="tables.csv", 
                      only_tables=True, only_fields=True, pushdown=['name'], force=True)
    
    for file in glob.glob(directory.rstrip('/') + '/*'):
        path = pathlib.Path(file)
        if path.is_file():
            os.unlink(file)
    
    csv_dir = directory.rstrip('/') + '/csv/'

    for file in glob.glob(csv_dir + '*'):
        path = pathlib.Path(file)
        if path.is_file():
            os.rename(file, file.replace('/csv', ''))

    os.removedirs(csv_dir)
        

@cli.command()
@click.argument('directory')
def unflatten(directory):
    datasource = unflatten_datapackage(directory)
    print(json.dumps(datasource, indent=4))


@cli.command()
@click.argument('datapackage')
def clean_datapackage(datapackage):
    with open(datapackage) as f:
        datapackage_obj = json.load(f)
    
    for resource in datapackage_obj['resources']:
        for field in resource['schema']['fields']:
            constraints = field.get("constraints", {})
            if "required" not in constraints:
                constraints["required"] = False
            if "unique" not in constraints:
                constraints["unique"] = False
            field['constraints'] = constraints

    with open(datapackage, "w+") as f:
        datapackage_obj = json.dump(datapackage_obj, f, indent=4)


if __name__ == '__main__':
    cli()