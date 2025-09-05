from argparse import ArgumentParser

from slurm_monitor.cli.base import BaseParser

import slurm_monitor.db.v2.db_tables as db_tables
import yaml
import warnings
from argparse import ArgumentParser
from pathlib import Path

def validate(spec_file: str | Path):
    spec_file = Path(spec_file)

    with open(spec_file) as f:
        spec = yaml.load(f, Loader=yaml.FullLoader)

    ignored_tables = []
    covered_spec = {}
    for table_name, table in db_tables.Cluster.metadata.tables.items():
        external_columns = []

        spec_field = None
        if table.info and 'sonar_spec' in table.info:
            spec_object = table.info["sonar_spec"]
            if '.' in spec_object:
                spec_object, spec_field = spec_object.split('.')
            if 'requires_tables' in table.info:
                requires_tables = table.info['requires_tables']
                for e_table in requires_tables:
                    external_columns += db_tables.Cluster.metadata.tables[e_table].columns

            if spec_object not in covered_spec:
                covered_spec[spec_object] = { 'implemented': {}, 'required': set({})}
        else:
            ignored_tables.append(table_name)
            continue

        if spec_object not in spec:
            raise KeyError(f"No definition available for referenced spec object '{spec_object}'")

        table_spec = spec[spec_object]
        fields_in_spec = set(table_spec['fields'].keys())

        # if a table_name.column is defined as references
        # identify the (probably) array type and use the target type for matching
        if spec_field:
            if spec_field not in table_spec['fields']:
                raise KeyError("No column '{spec_field}' in table '{table_name}' - check reference")

            # consider field being implemented - error on missing subfields
            # will pop up on the respective type
            covered_spec[spec_object]['implemented'].update({ table_name: set({ spec_field}) })

            field_type = table_spec['fields'][spec_field]['type']
            if field_type.startswith("[]"):
                spec_object = field_type[2:]
            elif field_type.startswith("*"):
                spec_object = field_type[1:]
            else:
                raise KeyError("Field '{spec_field}' has neither array, nor pointer type (in '{spec_object}')")

            # the spec of the type being used for this field
            table_spec = spec[spec_object]
            fields_in_spec = set(table_spec['fields'].keys())

            if spec_object not in covered_spec:
                covered_spec[spec_object] = { 'implemented': {}, 'required': set({})}

        covered_spec[spec_object]['implemented'].update({ table_name: set([x.name for x in table.columns]) | set([x.name for x in external_columns]) })

        covered_spec[spec_object]['required'] = fields_in_spec

    warnings.warn(f"Extra tables: {ignored_tables} - table has no associated info in 'spec'")
    ignored_spec = set([x for x in spec.keys() if 'meta' not in spec[x]['fields'] and 'attributes' not in spec[x]['fields'] and not x.endswith('Object')]) - set(covered_spec)

    print("Spec Implementation Status")
    for spec_object, fulfillment in covered_spec.items():
        all_implemented_columns = set()
        for table_name, implemented_columns in fulfillment['implemented'].items():
            all_implemented_columns |= implemented_columns

        missing_columns = fulfillment['required'] - all_implemented_columns

        ljust_spec_object = str(spec_object).ljust(25)
        if missing_columns:
            print(f"     {ljust_spec_object} INCOMPLETE - missing implementation of: {','.join(missing_columns)}")
        else:
            print(f"     {ljust_spec_object} COMPLETE (implemented by {[x for x in fulfillment['implemented'].keys()]})")

    warnings.warn(f"Potentially missing implementation: {ignored_spec} - specs have no associated tables")


class SpecParser(BaseParser):
    def __init__(self, parser: ArgumentParser):
        super().__init__(parser=parser)

        parser.add_argument("yaml_spec_file",
                            type=str,
                            help="YAML spec file of sonar-types"
                            )

    def execute(self, args):
        super().execute(args)

        if not Path(args.yaml_spec_file).exists():
            raise FileNotFoundError(f"Could not find {args.yaml_spec_file}")

        validate(args.yaml_spec_file)
