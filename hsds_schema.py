#!/usr/bin/env python3

import csv
import os
import json
import copy
import click
import pathlib 
import flatterer
import glob
from collections import OrderedDict
import shutil
import tempfile
from compiletojsonschema.compiletojsonschema import CompileToJsonSchema

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
        datapackage_obj = json.dump(datapackage_obj, f, indent=4, sort_keys=True)



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
        resource_schema['datapackage_metadata'] = {
            'format': resource_schema.pop('format'),
            'mediatype': resource_schema.pop('mediatype'),
            'profile': resource_schema.pop('profile'),
            'order': num + 1
        }

        resource_schema["type"] = "object"
        resource_schema["properties"] = {}
        fields = resource_schema["schema"].pop("fields")

        required_list = []
        tabular_required_list = []

        for field in fields:

            type_ = field.get("type")
            if type_.startswith("date") or type_ == 'time':
                field['type'] = "string"
                field['datapackage_type'] = type_

            #format = field.pop("format", None)
            #if format:
            #    field['original_format'] = format

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
    datapackage = _schemas_to_datapackage(jsonschema_dir)
    print(datapackage)

def _schemas_to_datapackage(jsonschema_dir):
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

    for schema in sorted(schemas, key=lambda i: i['datapackage_metadata']['order']):
        foreign_keys = []

        required = []
        required.extend(schema.get("required", []))
        required.extend(schema.get("tabular_required", []))

        schema.update(schema.pop("datapackage_metadata"))

        schema.pop("order")
        schema.pop("type")

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

        
        fields = []

        for field, prop in list(schema.pop('properties').items()):
            contraints = prop.get('constraints')
            if contraints:
                prop['constraints']['required'] = field in required
                prop['constraints']['unique'] = prop['constraints'].pop('unique')
                fields.append(prop)
            enum = prop.pop('enum', None)
            if enum:
                prop['constraints']['enum'] = enum
            datapackage_type = prop.pop('datapackage_type', None)
            if datapackage_type:
                prop['type'] = datapackage_type


        schema.pop('required', None)
        schema.pop('tabular_required', None)
        
        schema['schema'] = {"primaryKey": "id"} # {""}['fields'] = fields

        schema['schema']['fields'] = fields

        if foreign_keys:
            schema['schema']["foreignKeys"] = foreign_keys

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

    return json.dumps(datapackage, indent=4)



@cli.command()
@click.argument('jsonschema_dir')
def schemas_to_csv(jsonschema_dir):

    input_path = pathlib.Path(jsonschema_dir)

    def table_iterator():
        schemas = []
        for json_schema in input_path.glob("*.json"):
            schema = json.loads(json_schema.read_text())
            schemas.append(schema)

        for schema in sorted(schemas, key=lambda i: i['datapackage_metadata']['order']):
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


def get_example(schemas, schema_name, simple):
    results = {}

    schema = schemas[schema_name]

    for key, value in schema["properties"].items():
        if key.endswith('_id') and 'parent' not in key:
            continue
        example = value.get("example")
        if example:
            try:
                results[key] = int(example)
            except ValueError:
                results[key] = example
        
        obj_ref = value.get('$ref')

        if obj_ref:
           results[key] = get_example(schemas, obj_ref[:-5], simple)

        if not simple:
            array_ref = value.get('items', {}).get("$ref")
            if array_ref and (array_ref not in ('metadata.json', 'attribute.json') or schema_name == "service"):
                results[key] = [get_example(schemas, array_ref[:-5], simple)]

    return results

def example(schemas, base, simple):
    input_path = pathlib.Path(schemas)

    schemas = {}
    for json_schema in input_path.glob("*.json"):
        schema = json.loads(json_schema.read_text())
        schemas[schema["name"]] = schema
    
    if base == 'organization':
        schemas["service"]["properties"].pop("organization")
        schemas["organization"]["properties"]["services"] = {"type": "array", "items": {"$ref": "service.json"}}

    if base == 'service_at_location':
        schemas["service"]["properties"].pop("service_at_locations")
        schemas["service_at_location"]["properties"]["service"] = {"$ref": "service.json"}

    
    return get_example(schemas, base, simple)


def tabular_example(schemas):
    input_path = pathlib.Path(schemas)

    schemas = {}
    for json_schema in input_path.glob("*.json"):
        schema = json.loads(json_schema.read_text())
        schemas[schema["name"]] = schema

    output = {}

    for name, schema in sorted(schemas.items(), key=lambda i:i[1]['datapackage_metadata']['order']) :
        table_example = {}

        for key, value in schema['properties'].items():
            example = value.get("example")
            if example:
                try:
                    table_example[key] = int(example)
                except ValueError:
                    table_example[key] = example

        output[schema['name']] = [table_example]

    return output


@cli.command()
@click.argument('schemas')
@click.argument('output')
def schemas_to_doc_examples(schemas, output):
    _schemas_to_doc_examples(schemas, output)

def _schemas_to_doc_examples(schemas, output):
    output_path = pathlib.Path(output)
    examples = [
        # entity, filename, simple
        ('service', 'service_full.json', False),
        ('service', 'service_simple.json', True),
        ('service_at_location', 'service_at_location_full.json', False),
        ('service_at_location', 'service_at_location_simple.json', True),
        ('organization', 'organization_full.json', False),
        ('organization', 'organization_simple.json', True),
        ('taxonomy', 'taxonomy.json', False),
        ('taxonomy_term', 'taxonomy_term.json', False),
        ('location', 'location.json', False),
    ]


    for entity, filename, simple in examples:
        with open(output_path / filename, 'w+') as f:
            json.dump(example(schemas, entity, simple), f, indent=2)

    with open(output_path / 'tabular.json', 'w+') as f:
        json.dump(tabular_example(schemas), f, indent=2)
    
    os.makedirs(output_path / 'csv', exist_ok=True)

    for table, rows in tabular_example(schemas).items():
        with open(output_path / 'csv' / f'{table}.csv', 'w+') as f:
            dict_writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            dict_writer.writeheader()
            for row in rows:
                dict_writer.writerow(row)


@cli.command()
@click.argument('schamas')
@click.argument('base')
@click.option('--simple', is_flag=True)
def schemas_to_example(schemas, base, simple):
    print(json.dumps(example(schemas, base, simple), indent=2))


def compile_tabular(schemas_path, output_path):
    compiled = {"type": "object",
                "properties": {}}

    schemas = {}
    for json_schema in schemas_path.glob("*.json"):
        schema = json.loads(json_schema.read_text())
        schemas[schema["name"]] = schema
        
    for name, schema in sorted(schemas.items(), key=lambda i:i[1]['datapackage_metadata']['order']) :
        for key, value in list(schema['properties'].items()):
            if "$ref" in value:
                schema['properties'].pop(key)
            compiled["properties"][name.split(".")[0]] = {"type": "array", "items": schema}

    (output_path / 'tabular.json').write_text(json.dumps(compiled, indent=2))


def compile_definitions(schemas_path, output_path):

    schemas = {}
    for json_schema in schemas_path.glob("*.json"):
        schema = json.loads(json_schema.read_text())

        for field, prop in schema['properties'].items():
            array_ref = prop.get('items', {}).get("$ref")
            if array_ref:
                prop['items']['$ref'] = f'#/definitions/{array_ref.split(".")[0]}'

            obj_ref = prop.get("$ref")
            if obj_ref:
                prop['$ref'] = f'/definitions/{obj_ref.split(".")[0]}'

        schemas[schema["name"]] = schema
    
    compiled = schemas.pop('service')
    compiled['definitions'] = {}

    for name, schema in sorted(schemas.items(), key=lambda i:i[1]['datapackage_metadata']['order']):
        compiled['definitions'][name] = schema
    
    (output_path / 'service_with_definitions.json').write_text(json.dumps(compiled, indent=2))


# def add_descriptions(schemas_path):
#     schemas = {}
#     for json_schema in schemas_path.glob("*.json"):
#         schema = json.loads(json_schema.read_text())
#         schemas[schema["name"]] = schema
    

#     for json_schema in schemas_path.glob("*.json"):
#         schema = json.loads(json_schema.read_text(), object_pairs_hook=OrderedDict)
#         for field, prop in list(schema['properties'].items()):
#             array_ref = prop.get('items', {}).get("$ref")
#             if array_ref:
#                 prop['description'] = schemas[array_ref.split(".")[0]]['description']
#                 prop.move_to_end('description', False)
#                 prop.move_to_end('name', False)


#             obj_ref = prop.get("$ref")
#             if obj_ref:
#                 prop['description'] = schemas[obj_ref.split(".")[0]]['description']
#                 prop.move_to_end('description', False)
#                 prop.move_to_end('name', False)

#         with open(json_schema, 'w+') as f:
#             json.dump(schema, f, indent=4)
    


@cli.command()
@click.argument('schemas')
@click.argument('output_dir')
def compile_schemas(schemas, output_dir):
    _compile_schemas(schemas, output_dir)

def _compile_schemas(schemas, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    output_path = pathlib.Path(output_dir)
    schemas_path = pathlib.Path(schemas)

    compile_tabular(schemas_path, output_path)
    compile_definitions(schemas_path, output_path)
    #add_descriptions(schemas_path)

    output = CompileToJsonSchema(str(schemas_path / 'service.json')).get_as_string()
    (output_path / 'service.json').write_text(output)

    with tempfile.NamedTemporaryFile(dir=schemas) as fp:
        package = {
            "type": "array", "items": {"$ref": "service.json"}
        }

        fp.write(json.dumps(package).encode())
        fp.flush()
        output = CompileToJsonSchema(str(schemas_path / fp.name)).get_as_string()
        (output_path / 'service_package.json').write_text(output)


    with tempfile.NamedTemporaryFile(dir=schemas) as fp:

        organization = json.loads((schemas_path / 'organization.json').read_text())
        organization['properties']['services'] = {
            "type": "array", "items": {"$ref": "service.json"}
        }

        fp.write(json.dumps(organization).encode())
        fp.flush()

        organization_name = fp.name

        output = CompileToJsonSchema(str(schemas_path / organization_name)).get_as_string()
        (output_path / 'organization.json').write_text(output)

        with tempfile.NamedTemporaryFile(dir=schemas) as fp:
            package = {
                "type": "array", "items": {"$ref": organization_name}
            }

            fp.write(json.dumps(package).encode())
            fp.flush()
            output = CompileToJsonSchema(str(schemas_path / fp.name)).get_as_string()
            (output_path / 'organization_package.json').write_text(output)


    with tempfile.NamedTemporaryFile(dir=schemas) as fp:

        service_at_location = json.loads((schemas_path / 'service_at_location.json').read_text())
        service_at_location['properties']['service'] = {
            "name": "service",
            "$ref": "service.json"
        }
        fp.write(json.dumps(service_at_location).encode())
        fp.flush()

        service_at_location_name = fp.name

        output = CompileToJsonSchema(str(schemas_path / fp.name)).get()

        output['properties']['service']['properties'].pop('service_at_locations')

        (output_path / 'service_at_location.json').write_text(json.dumps(output, indent=2))

        with tempfile.NamedTemporaryFile(dir=schemas) as fp:
            package = {
                "type": "array", "items": {"$ref": service_at_location_name}
            }

            fp.write(json.dumps(package, indent=2).encode())
            fp.flush()
            output = CompileToJsonSchema(str(schemas_path / fp.name)).get()

            output['items']['properties']['service']['properties'].pop('service_at_locations')

            (output_path / 'service_at_location_package.json').write_text(json.dumps(output, indent=2))


@cli.command()
def docs_all():
    schema_dir = pathlib.Path('schema') 
    example_dir = pathlib.Path('examples') 
    compiled_dir = schema_dir / 'compiled'
    with open('datapackage.json', 'w+') as f: 
        datapackage = _schemas_to_datapackage(schema_dir)
        f.write(datapackage)

    _schemas_to_doc_examples(schema_dir, example_dir)
    _compile_schemas(schema_dir, compiled_dir)


if __name__ == '__main__':
    cli()
