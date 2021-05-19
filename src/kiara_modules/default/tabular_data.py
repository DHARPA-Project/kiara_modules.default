# -*- coding: utf-8 -*-
import pyarrow as pa
import typing
from pydantic import Field, validator

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.types.files import FileBundleModel, FileModel
from kiara.data.values import ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException


class CreateTableModuleConfig(KiaraModuleConfig):

    allow_column_filter: bool = Field(
        description="Whether to add an input option to filter columns.", default=False
    )


class CreateTableFromFileModule(KiaraModule):
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

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_file: FileModel = inputs.get_value_data("file")

        imported_data = pa.csv.read_csv(input_file.path)

        if self.get_config_value("allow_column_filter"):
            if self.get_config_value("columns"):
                imported_data = imported_data.select(
                    self.get_config_value("only_columns")
                )

        outputs.set_value("table", imported_data)


AVAILABLE_FILE_COLUMNS = [
    "id",
    "rel_path",
    "orig_filename",
    "orig_path",
    "import_time",
    "mime_type",
    "size",
    "content",
    "path",
    "file_name",
]
DEFAULT_COLUMNS = ["id", "rel_path", "content"]


class CreateTableFromTextFilesConfig(KiaraModuleConfig):

    columns: typing.List[str] = Field(
        description=f"A list of columns to add to the table. Available properties: {', '.join(AVAILABLE_FILE_COLUMNS)}",
        default=DEFAULT_COLUMNS,
    )

    @validator("columns")
    def _validate_columns(cls, v):

        if isinstance(v, str):
            v = [v]

        if not isinstance(v, typing.Iterable):
            raise ValueError("'columns' value must be a list.")

        invalid = set()
        for item in v:
            if item not in AVAILABLE_FILE_COLUMNS:
                invalid.add(item)

        if invalid:
            raise ValueError(
                f"Items in the 'columns' value must be one of {AVAILABLE_FILE_COLUMNS}. Invalid value(s): {', '.join(invalid)}"
            )

        return v


class CreateTableFromTextFilesModule(KiaraModule):

    _config_cls = CreateTableFromTextFilesConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "files": {"type": "file_bundle", "doc": "The files to use for the table."}
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        id_column = "id"
        path_column = "rel_path"
        content = "content"

        outputs = {
            "table": {
                "type": "table",
                "doc": f"A table with the index column '{id_column}', a column '{path_column}' that indicates the relative path of the file in the bundle, and a column '{content}' that holds the (text) content of every file.",
            }
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        bundle: FileBundleModel = inputs.get_value_data("files")

        columns = self.get_config_value("columns")
        if not columns:
            columns = DEFAULT_COLUMNS

        if "content" in columns:
            file_dict = bundle.read_text_file_contents()
        else:
            file_dict = {}
            for rel_path in bundle.included_files.keys():
                file_dict[rel_path] = None  # type: ignore

        tabular: typing.Dict[str, typing.List[typing.Any]] = {}
        for column in columns:
            for index, rel_path in enumerate(sorted(file_dict.keys())):

                if column == "content":
                    value: typing.Any = file_dict[rel_path]
                elif column == "id":
                    value = index
                elif column == "rel_path":
                    value = rel_path
                else:
                    file_model = bundle.included_files[rel_path]
                    value = getattr(file_model, column)

                tabular.setdefault(column, []).append(value)

        table = pa.Table.from_pydict(tabular)

        outputs.set_value("table", table)


class MergeTableModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "sources": {"type": "dict", "doc": "The source tables and/or columns."}
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "table": {
                "type": "table",
                "doc": "The merged table, including all source tables and columns.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        sources = inputs.get_value_data("sources")

        len_dict = {}
        arrays = []
        column_names = []
        for source_key, table_or_column in sources.items():

            if isinstance(table_or_column, pa.Table):
                rows = table_or_column.num_rows
                for name in table_or_column.schema.names:
                    column = table_or_column.column(name)
                    arrays.append(column)
                    column_names.append(name)

            elif isinstance(table_or_column, pa.Array):
                rows = len(table_or_column)
                arrays.append(table_or_column)
                column_names.append(source_key)
            else:
                raise KiaraProcessingException(
                    f"Can't merge table: invalid type '{type(table_or_column)}' for source '{source_key}'."
                )

            len_dict[source_key] = rows

        all_rows = None
        for source_key, rows in len_dict.items():
            if all_rows is None:
                all_rows = rows
            else:
                if all_rows != rows:
                    all_rows = None
                    break

        if all_rows is None:
            len_str = ""
            for name, rows in len_dict.items():
                len_str = f" {name} ({rows})"

            raise KiaraProcessingException(
                f"Can't merge table, sources have different lengths:{len_str}"
            )

        table = pa.Table.from_arrays(arrays=arrays, names=column_names)

        outputs.set_value("table", table)


class TableFilterModuleConfig(KiaraModuleConfig):

    pass


class FilterTableModule(KiaraModule):

    _config_cls = TableFilterModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs = {
            "table": {"type": "table", "doc": "The table to filter."},
            "mask": {
                "type": "array",
                "doc": "An mask array of booleans of the same length as the table.",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {"table": {"type": "table", "doc": "The filtered table."}}
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_table: pa.Table = inputs.get_value_data("table")
        filter_array: pa.Array = inputs.get_value_data("mask")

        filtered = input_table.filter(filter_array)

        outputs.set_value("table", filtered)
