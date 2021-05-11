# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.types.files import FileBundleModel, FileModel, FolderImportConfig
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs


class ImportLocalPathConfig(KiaraModuleConfig):

    source_is_immutable: bool = Field(
        description="Whether the data that lives in source path can be relied upon to not change, and always be available",
        default=False,
    )


class ImportLocalFileModule(KiaraModule):
    """Read a file into the data registry."""

    _config_cls = ImportLocalPathConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"path": {"type": "string", "doc": "The path to the file."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "file": {
                "type": "file",
                "doc": "A representation of the original file content in the kiara data registry.",
            }
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        path = inputs.path
        file_model = FileModel.import_file(path)
        outputs.file = file_model


class ImportLocalFolderModule(KiaraModule):

    _config_cls = ImportLocalPathConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "path": {"type": "string", "doc": "The path to the folder."},
            "included_files": {
                "type": "array",
                "doc": "A list of strings, include all files where the filename ends with that string.",
                "optional": True,
            },
            "excluded_dirs": {
                "type": "array",
                "doc": "A list of strings, exclude all folders whose name ends with that string.",
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The collection of files contained in the bundle.",
            }
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        path = inputs.path

        included_files = inputs.included_files
        excluded_dirs = inputs.excluded_dirs

        import_config = FolderImportConfig(
            include_files=included_files, exclude_dirs=excluded_dirs
        )

        bundle = FileBundleModel.import_folder(source=path, import_config=import_config)

        outputs.file_bundle = bundle
