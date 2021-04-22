# -*- coding: utf-8 -*-
import os
import typing
from pyarrow import csv
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs


class ImportTableModuleConfig(KiaraModuleConfig):

    only_columns: typing.List = Field(
        description="If non-empty, only import the columns that match the names in this list.",
        default_factory=list,
    )


class ImportTableModule(KiaraModule):
    """Import table-like data from a file or fileset."""

    _config_cls = ImportTableModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "path": {
                "type": "string",
                "doc": "The path to a file or folder that contains tabular data.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"table": {"type": "table", "doc": "the imported table"}}

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        path = inputs.path
        if not os.path.exists(path):
            raise Exception(f"Path '{path}' does not exist")

        imported_data = csv.read_csv(path)

        if self.get_config_value("only_columns"):
            imported_data = imported_data.select(self.get_config_value("only_columns"))

        outputs.table = imported_data
