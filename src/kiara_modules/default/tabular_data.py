# -*- coding: utf-8 -*-
import typing
from pyarrow import csv
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.types import FileModel
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs


class CreateTableModuleConfig(KiaraModuleConfig):

    allow_column_filter: bool = Field(
        description="Whether to add an input option to filter columns.", default=False
    )


class CreateTableModule(KiaraModule):
    """Import table-like data from an item in the data registry."""

    _config_cls = CreateTableModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "file": {
                "type": "file",
                "doc": "The file that contains tabular data.",
                "optional": False,
            }
        }

        if self.get_config_value("allow_column_filter"):

            inputs["columns"] = {
                "type": "array",
                "doc": "If provided, only import the columns that match items in this list.",
                "optional": False,
            }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"table": {"type": "table", "doc": "the imported table"}}

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        input_file: FileModel = inputs.file

        print(input_file)

        imported_data = csv.read_csv(input_file.path)

        if self.get_config_value("allow_column_filter"):
            if self.get_config_value("columns"):
                imported_data = imported_data.select(
                    self.get_config_value("only_columns")
                )

        outputs.table = imported_data
