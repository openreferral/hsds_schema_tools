#!/usr/bin/env python3

import csv
import os
import json
import click
import pathlib
import flatterer
import tempfile
import requests
import json_merge_patch
from compiletojsonschema.compiletojsonschema import CompileToJsonSchema

def tabular_example(schemas):
    input_path = pathlib.Path(schemas)

    schemas = {}
    for json_schema in input_path.glob("*.json"):
        if str(json_schema).endswith('openapi.json'):
            continue
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

        output[schema['path']] = [table_example]

    return output

def get_schemas_from_github(profile_url, branch='main'):
    url = "https://api.github.com/repos/openreferral/specification/contents/schema?ref=3.0"

    if profile_url.startswith("https://github.com"):
        path = profile_url.replace("https://github.com", "")
        profile_url = "https://raw.githubusercontent.com" + path.rstrip("/") + "/" + branch

    response = requests.get(url)
    data = json.loads(response.text)

    schemas = {}
    for file in data:
        # get the download URL and the file name
        url = file['download_url']
        if not url:  # skip directories
            continue
        filename = file['name']

        response_text = requests.get(url).text
        if filename == 'openapi.json':
            response_text = response_text.replace('https://raw.githubusercontent.com/openreferral/specification/3.0', profile_url)

        response = json.loads(response_text)
        schemas[filename] = response

    return schemas


@click.group()
def cli():
    pass


def profile_to_schema(profile_url, branch='main', profile_dir='profile', schema_dir='schema'):
    core_schemas = get_schemas_from_github(profile_url, branch=branch)

    final_schemas = {}
    profile_schemas = {}

    for profile_schema in sorted(pathlib.Path(profile_dir).glob("*.json")):
        profile_schemas[profile_schema.name] = json.loads(profile_schema.read_text())

    removed = []
    for name, schema in profile_schemas.items():
        if not schema:
            removed.append(name)

    for name, schema in profile_schemas.items():
        if name not in core_schemas:
            final_schemas[name] = schema
            continue
        if schema:
            merged = json_merge_patch.merge(core_schemas[name], schema)
            for removed_name in removed:
                if name == 'openapi.json':
                    continue

                properties = merged['properties']
                for prop in list(properties):
                    if properties[prop].get('$ref') == removed_name or properties[prop].get('items', {}).get('$ref') == removed_name:
                        properties.pop(prop)
            final_schemas[name] = merged

    for name, schema in core_schemas.items():
        if name in profile_schemas:
            continue
        for removed_name in removed:
            if name == 'openapi.json':
                continue
            properties = schema['properties']
            for prop in list(properties):
                if properties[prop].get('$ref') == removed_name or properties[prop].get('items', {}).get('$ref') == removed_name:
                    properties.pop(prop)
        final_schemas[name] = schema


    schema_path = pathlib.Path(schema_dir)

    for name, schema in final_schemas.items():
        (schema_path / name).write_text(json.dumps(schema, indent=2))


def clean_dir(directory):
    for file in directory.iterdir():
        if file.is_file():
            file.unlink()

@cli.command()
@click.argument('jsonschema_dir')
def schemas_to_datapackage(jsonschema_dir):
    datapackage = _schemas_to_datapackage(jsonschema_dir)
    print(datapackage)

def _schemas_to_datapackage(jsonschema_dir):
    input_path = pathlib.Path(jsonschema_dir)
    
    fks = []
    schemas = []

    for json_schema in sorted(input_path.glob("*.json")):
        if str(json_schema).endswith('openapi.json'):
            continue
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
                prop.pop('format', None)


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
            if str(json_schema).endswith('openapi.json'):
                continue
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
            if value.get("type") == "string":
                results[key] = example
            else:
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
                #if array_ref in ('metadata.json', 'attribute.json') and array_ref not in schemas:
                #    continue
                results[key] = [get_example(schemas, array_ref[:-5], simple)]

    return results

page = {
    "total_items": 10,
    "total_pages": 10,
    "page_number": 1,
    "size": 1,
    "first_page": True,
    "last_page": False,
    "empty": False,
}          

def example(schemas, base, paginated):
    input_path = pathlib.Path(schemas)

    schemas = {}
    for json_schema in input_path.glob("*.json"):
        if str(json_schema).endswith('openapi.json'):
            continue
        schema = json.loads(json_schema.read_text())
        schemas[schema["name"]] = schema
    
    if base == 'organization':
        schemas["service"]["properties"].pop("organization")
        schemas["organization"]["properties"]["services"] = {"type": "array", "items": {"$ref": "service.json"}}

    if base == 'service_at_location':
        schemas["service"]["properties"].pop("service_at_locations")
        schemas["service_at_location"]["properties"]["service"] = {"$ref": "service.json"}

    example = get_example(schemas, base, paginated)
    if paginated:
        new_example = page.copy()
        new_example["contents"] = [example]
        example = new_example

    return example


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
        ('service', 'service_list.json', True),
        ('service_at_location', 'service_at_location_full.json', False),
        ('service_at_location', 'service_at_location_list.json', True),
        ('organization', 'organization_full.json', False),
        ('organization', 'organization_list.json', True),
        ('taxonomy', 'taxonomy.json', False),
        ('taxonomy', 'taxonomy_list.json', True),
        ('taxonomy_term', 'taxonomy_term.json', False),
        ('taxonomy_term', 'taxonomy_term_list.json', True),
    ]

    for entity, filename, simple in examples:
        with open(output_path / filename, 'w+') as f:
            json.dump(example(schemas, entity, simple), f, indent=2)

    os.makedirs(output_path / 'csv', exist_ok=True)

    for path, rows in tabular_example(schemas).items():
        with open(output_path / 'csv' / path, 'w+') as f:
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


def compile_definitions(schemas_path, output_path):

    schemas = {}
    for json_schema in schemas_path.glob("*.json"):
        if str(json_schema).endswith('openapi.json'):
            continue
        schema = json.loads(json_schema.read_text())

        for field, prop in schema['properties'].items():
            array_ref = prop.get('items', {}).get("$ref")
            if array_ref:
                prop['items']['$ref'] = f'#/definitions/{array_ref.split(".")[0]}'

            obj_ref = prop.get("$ref")
            if obj_ref:
                prop['$ref'] = f'#/definitions/{obj_ref.split(".")[0]}'

        schemas[schema["name"]] = schema
    
    compiled = schemas.pop('service')
    compiled['definitions'] = {}

    for name, schema in sorted(schemas.items(), key=lambda i:i[1]['datapackage_metadata']['order']):
        compiled['definitions'][name] = schema
    
    (output_path / 'service_with_definitions.json').write_text(json.dumps(compiled, indent=2))


def remove_one_to_many(properties):
    for key, value in list(properties.items()):
        if value.get("type") == "array" and "items" in value:
            properties.pop(key)
        if value.get("type") == "object" and "properties" in value:
            remove_one_to_many(value["properties"])


def compile_to_openapi30(schemas_path, docs_dir):
    open_api_data = json.loads((schemas_path / 'openapi.json').read_text())
    open_api_data['openapi'] = "3.0.0"
    open_api_data.pop('jsonSchemaDialect')

    open_api_data['info'] = {
        "title": "HSDS OpenAPI",
        "version": "3.0",
        "description": "Open API for the Human Services Data Specification. See [HSDS documentation](http://docs.openreferral.org/en/3.0/) for more details on the specification." ,
        "license": {
          "name": "Creative Commons Attribution Share-Alike 4.0 license",
          "url": "https://creativecommons.org/licenses/by/4.0/"
        }
      }
    (docs_dir / 'extras' / 'openapi30.json').write_text(json.dumps(open_api_data, indent=2))



@cli.command()
@click.argument('schemas')
@click.argument('output_dir')
def compile_schemas(schemas, output_dir):
    _compile_schemas(schemas, output_dir)

def _compile_schemas(schemas, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    output_path = pathlib.Path(output_dir)
    schemas_path = pathlib.Path(schemas)

    compile_definitions(schemas_path, output_path)
    #add_descriptions(schemas_path)

    output = CompileToJsonSchema(str(schemas_path / 'service.json')).get_as_string()
    (output_path / 'service.json').write_text(output)

    output = json.loads(output)
    remove_one_to_many(output['properties'])
    (output_path / 'service_list.json').write_text(json.dumps(output, indent=2))

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

        output = json.loads(output)
        remove_one_to_many(output['properties'])
        (output_path / 'organization_list.json').write_text(json.dumps(output, indent=2))

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

        remove_one_to_many(output['properties'])
        (output_path / 'service_at_location_list.json').write_text(json.dumps(output, indent=2))

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
    docs_dir = pathlib.Path('docs') 
    example_dir = pathlib.Path('examples') 
    compiled_dir = schema_dir / 'compiled'

    compile_to_openapi30(schema_dir, docs_dir)
    #add_titles(schema_dir)
    with open('datapackage.json', 'w+') as f: 
        datapackage = _schemas_to_datapackage(schema_dir)
        f.write(datapackage)

    _schemas_to_doc_examples(schema_dir, example_dir)
    _compile_schemas(schema_dir, compiled_dir)


@cli.command()
@click.argument('profile_url')
@click.option('--branch', default='main')
@click.option('--clean', is_flag=True, default=False)
def profile_all(profile_url, branch, clean=False):
    schema_dir = pathlib.Path('schema')

    if clean:
        clean_dir(schema_dir)

    profile_to_schema(profile_url, branch, profile_dir='profile', schema_dir='schema')

    example_dir = pathlib.Path('examples')
    example_dir.mkdir(exist_ok=True)

    if clean:
        clean_dir(example_dir)

    compiled_dir = schema_dir / 'compiled'
    compiled_dir.mkdir(exist_ok=True)

    if clean:
        clean_dir(compiled_dir)
    #compile_to_openapi30(schema_dir, docs_dir)
    with open('datapackage.json', 'w+') as f: 
        datapackage = _schemas_to_datapackage(schema_dir)
        f.write(datapackage)
    
    _schemas_to_doc_examples(schema_dir, example_dir)
    _compile_schemas(schema_dir, compiled_dir)


if __name__ == '__main__':
    cli()
