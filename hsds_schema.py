#!/usr/bin/env python3

import csv
import os
import json
import copy
import click
import pathlib 
import flatterer
import glob
import shutil
import tempfile

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



@cli.command()
@click.argument('datapackage')
@click.argument('output_dir')
def datapackage_to_schemas(datapackage, output_dir):

    output_path = pathlib.Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    with open(datapackage) as f:
        datapackage_obj = json.load(f)
    
    all_refs = {}
    to_process = ['service', 'attribute', 'organization', 'service_at_location', 'location', 'contact', 'phone']
    all_fks = set()

    for resource in datapackage_obj['resources']:
        fks = resource["schema"].get("foreignKeys", [])
        for fk in fks:
            all_fks.add((resource["name"], fk["reference"]["resource"]))
    
    while to_process:
        table = to_process.pop(0)
        refs = []
        for fk_table, reference_table in list(all_fks):
            if table == fk_table:
                refs.append((reference_table, "object"))
                if reference_table not in to_process:
                    to_process.append(reference_table)
                all_fks.remove((fk_table, reference_table))
            if table == reference_table:
                refs.append((fk_table, "array"))
                if fk_table not in to_process:
                    to_process.append(fk_table)
                all_fks.remove((fk_table, reference_table))
        if refs:
            all_refs[table] = refs
    

    for num, resource in enumerate(datapackage_obj['resources']):
        json_schema_path = output_path / (resource['name'] + '.json')
        resource_schema = copy.deepcopy(resource)
        resource_schema["order"] = num + 1
        resource_schema["type"] = "object"
        resource_schema["properties"] = {}
        fields = resource_schema["schema"].pop("fields")

        required_list = []
        tabular_required_list = []

        for field in fields:
            resource_schema["properties"][field['name']] = field
            constraints = field.get('constraints', {})
            required = constraints.pop('required', False)
            if required:
                if field['name'].endswith("_id") or field['name'] in ['link_entity', 'resource_type']:
                    tabular_required_list.append(field['name'])
                else:
                    required_list.append(field['name'])

            enum = constraints.pop('enum', False)
            if enum:
                field['enum'] = enum
                
        
        if required_list:
            resource_schema['required'] = required_list

        if tabular_required_list:
            resource_schema['tabular_required'] = tabular_required_list
        
        table_refs = all_refs.get(resource['name'])

        if table_refs:
            for table, relationship in table_refs:
                if relationship == "object":
                    resource_schema["properties"][f"{table}"] = {"name": f"{table}", "$ref": f"{table}.json"}
                else:
                    resource_schema["properties"][f"{table}s"] = {"name": f"{table}s", "type": "array", "items": {"$ref": f"{table}.json"}}

        if resource['name'] not in ["attribute", "taxonomy", "taxonomy_term", "metadata"]:
            resource_schema["properties"]["attributes"] = {"name": "attributes", "type": "array", "items": {"$ref": f"attribute.json"}}

        if resource['name'] != "metadata":
            resource_schema["properties"]["metadata"] = {"name": "metadata", "type": "array", "items": {"$ref": f"metadata.json"}}

        resource_schema.pop("schema")

        with open(json_schema_path, "w+") as f:
            json.dump(resource_schema, f, indent=4)


@cli.command()
@click.argument('jsonschema_dir')
def schemas_to_datapackage(jsonschema_dir):
    input_path = pathlib.Path(jsonschema_dir)
    
    fks = []
    schemas = []

    for json_schema in input_path.glob("*.json"):
        schema = json.loads(json_schema.read_text())
        schemas.append(schema)
        name = schema['name']
        for field, prop in schema['properties'].items():
            array_ref = prop.get('items', {}).get("$ref")
            if array_ref and array_ref not in ['attribute.json', 'metadata.json']:
                table = array_ref.replace('.json', '')
                fks.append((table, name))
            obj_ref = prop.get("$ref")
            if obj_ref:
                table = obj_ref.replace('.json', '')
                fks.append((name, table))
    
    resources = []

    for schema in sorted(schemas, key=lambda i: i['order']):
        foreign_keys = []

        required = []
        required.extend(schema.get("required", []))
        required.extend(schema.get("tabular_required", []))

        schema.pop("order")

        name = schema['name']

        for table, foriegn_table in fks:
            if table == name:
                foreign_keys.append(
                    {
                        "fields": f"{foriegn_table}_id",
                        "reference": {
                            "resource": foriegn_table,
                            "fields": "id"
                        }
                    }
                )

        if foreign_keys:
            schema["foriegn_keys"] = foreign_keys
        
        fields = []

        for field, prop in list(schema.pop('properties').items()):
            contraints = prop.get('constraints')
            if contraints:
                prop['constraints']['required'] = field in required
                fields.append(prop)
        
        schema.pop('required', None)
        schema.pop('tabular_required', None)
        
        schema['schema'] = {"primaryKey": "id"} # {""}['fields'] = fields
        schema['schema']['fields'] = fields

        resources.append(schema)

    datapackage = {
        "name": "human_services_data",
        "title": "Human Services Data Specification",
        "description": "HSDS describes data about organizations, the services they provide, the locations at which these services can be accessed, and associated details.",
        "profile": "tabular-data-package",
        "version": "3.0.0",
        "homepage": "http://docs.openreferral.org",
        "license": {
            "url": "https://creativecommons.org/licenses/by-sa/4.0/",
            "type": "CC-BY-SA-4.0",
            "name": "Creative Commons Attribution-ShareAlike 4.0"
        },
        "resources": resources
    }

    print(json.dumps(datapackage, indent=2))


@cli.command()
@click.argument('jsonschema_dir')
def schemas_to_csv(jsonschema_dir):

    input_path = pathlib.Path(jsonschema_dir)

    def table_iterator():
        schemas = []
        for json_schema in input_path.glob("*.json"):
            schema = json.loads(json_schema.read_text())
            schemas.append(schema)

        for schema in sorted(schemas, key=lambda i: i['order']):
            name = schema['name']

            required = schema.get("required", [])
            tabular_required = schema.get("tabular_required", [])

            for field, prop in list(schema.pop('properties').items()):
                prop['table_name'] = name

                contraints = prop.get('constraints')
                if not contraints:
                    prop['constraints'] = {}

                prop['constraints']['required'] = field in required
                prop['constraints']['tablular_required'] = field in tabular_required
                yield prop
    
    with tempfile.TemporaryDirectory() as tmpdirname:
        flatterer.flatten(table_iterator(), tmpdirname, force=True, fields_csv='fields_for_csv.csv', only_fields=True)
        path = pathlib.Path(tmpdirname) / 'csv' / 'main.csv'
        print(path.read_text())


if __name__ == '__main__':
    cli()