import yaml
import warnings
from sqlalchemy import MetaData
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

SONAR_DEFAULT_SPEC_FILENAME = (
    Path(__file__).parent.parent.parent
    / "resources"
    / "sonar-latest"
    / "types.spec.yaml"
)


class Specification:
    _spec: dict[str, any]

    def __init__(self, filename: str | Path = SONAR_DEFAULT_SPEC_FILENAME):
        """
        :param filename Path to the sonar specification (which is usually)
        """
        if not Path(filename).exists():
            raise RuntimeError(
                f"Missing sonar specification: failed to load documentation from {filename}"
            )

        with open(filename) as f:
            self._spec = yaml.load(f, Loader=yaml.FullLoader)

    def __getitem__(self, item: str):
        """
        :param str item Name of the spec entry
        """
        return self._spec[item]

    def validate(self, tables: MetaData, show: bool = False):
        """
        Compare intended schema for with the sonar specification

        :param MetaData tables Database metadata describing table schemas
        :param bool show If show is true, output warnings and implementation status
                         on stdout
        """
        ignored_tables = []
        covered_spec = {}
        for table_name, table in tables.items():
            external_columns = []

            spec_field = None
            if table.info and "sonar_spec" in table.info:
                spec_object = table.info["sonar_spec"]
                if "." in spec_object:
                    spec_object, spec_field = spec_object.split(".")
                if "requires_tables" in table.info:
                    requires_tables = table.info["requires_tables"]
                    for e_table in requires_tables:
                        external_columns += tables[e_table].columns

                if spec_object not in covered_spec:
                    covered_spec[spec_object] = {"implemented": {}, "required": set({})}
            else:
                ignored_tables.append(table_name)
                continue

            if spec_object not in self._spec:
                raise KeyError(
                    f"No definition available for referenced spec object '{spec_object}'"
                )

            table_spec = self._spec[spec_object]
            fields_in_spec = set(table_spec["fields"].keys())

            # if a table_name.column is defined as references
            # identify the (probably) array type and use the target type for matching
            if spec_field:
                if spec_field not in table_spec["fields"]:
                    raise KeyError(
                        "No column '{spec_field}' in table '{table_name}' - check reference"
                    )

                # consider field being implemented - error on missing subfields
                # will pop up on the respective type
                covered_spec[spec_object]["implemented"].update(
                    {table_name: set({spec_field})}
                )

                field_type = table_spec["fields"][spec_field]["type"]
                if field_type.startswith("[]"):
                    spec_object = field_type[2:]
                elif field_type.startswith("*"):
                    spec_object = field_type[1:]
                else:
                    raise KeyError(
                        "Field '{spec_field}' has neither array, nor pointer type (in '{spec_object}')"
                    )

                # the spec of the type being used for this field
                table_spec = self._spec[spec_object]
                fields_in_spec = set(table_spec["fields"].keys())

                if spec_object not in covered_spec:
                    covered_spec[spec_object] = {"implemented": {}, "required": set({})}

            covered_spec[spec_object]["implemented"].update(
                {
                    table_name: set([x.name for x in table.columns])
                    | set([x.name for x in external_columns])
                }
            )

            covered_spec[spec_object]["required"] = fields_in_spec

        if show:
            warnings.warn(
                f"Extra tables: {ignored_tables} - table has no associated info in 'spec'"
            )

        ignored_spec = set(
            [
                x
                for x in self._spec.keys()
                if "meta" not in self._spec[x]["fields"]
                and "attributes" not in self._spec[x]["fields"]
                and not x.endswith("Object")
            ]
        ) - set(covered_spec)

        if show:
            print("Spec Implementation Status")
            for spec_object, fulfillment in covered_spec.items():
                all_implemented_columns = set()
                for table_name, implemented_columns in fulfillment[
                    "implemented"
                ].items():
                    all_implemented_columns |= implemented_columns

                missing_columns = fulfillment["required"] - all_implemented_columns

                ljust_spec_object = str(spec_object).ljust(25)
                if missing_columns:
                    print(
                        f"     {ljust_spec_object} "
                        f"INCOMPLETE - missing implementation of: {','.join(missing_columns)}"
                    )
                else:
                    print(
                        f"     {ljust_spec_object} "
                        f"COMPLETE (implemented by {[x for x in fulfillment['implemented'].keys()]})"
                    )

            warnings.warn(
                f"Potentially missing implementation: {ignored_spec} - specs have no associated tables"
            )

        return {"ignored_spec": ignored_spec, "covered_spec": covered_spec}

    def augment(self, tables: list[any]):
        """
        Augment existing table schema to ensure the 'doc' / comments
        are in sync with the sonar specification
        """
        spec_coverage = self.validate(tables)
        covered_spec = spec_coverage["covered_spec"]
        for spec_object_name, fulfillment in covered_spec.items():
            implemented = fulfillment["implemented"]
            for table, implemented_columns in implemented.items():
                for column in tables[table].columns:
                    if column.name in implemented_columns:
                        try:
                            spec_column = self._spec[spec_object_name]["fields"][
                                column.name
                            ]
                            if "doc" in spec_column:
                                column.comment = spec_column["doc"].strip()
                                logger.debug(
                                    f"Found doc for {table}.{column.name}: {column.comment}"
                                )
                        except KeyError as e:
                            # The database schema can have more or deviating field, so generally ignore missing
                            # items here
                            logger.debug(
                                f"No column '{e}' in spec object '{spec_object_name}' - current table {table}"
                            )
