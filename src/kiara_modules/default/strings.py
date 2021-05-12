# -*- coding: utf-8 -*-
import re
import typing
from abc import abstractmethod
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module import StepInputs, StepOutputs


class StringManipulationModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"text": {"type": "string", "doc": "The input string."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"text": {"type": "string", "doc": "The processed string."}}

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        input_string = inputs.get_value_data("text")
        result = self.process_string(input_string)
        outputs.text = result

    @abstractmethod
    def process_string(self, text: str) -> str:
        pass


class RegexModuleConfig(KiaraModuleConfig):

    regex: str = Field(description="The regex to apply.")
    only_first_match: bool = Field(
        description="Whether to only return the first match, or all matches.",
        default=False,
    )


class RegexModule(KiaraModule):

    _config_cls = RegexModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"text": {"type": "string", "doc": "The text to match."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        if self.get_config_value("only_first_match"):
            output_schema = {"text": {"type": "string", "doc": "The first match."}}
        else:
            raise NotImplementedError()

        return output_schema

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        text = inputs.get_value_data("text")
        regex = self.get_config_value("regex")
        matches = re.findall(regex, text)

        if not matches:
            raise KiaraProcessingException(f"No match for regex: {regex}")

        if self.get_config_value("only_first_match"):
            result = matches[0]
        else:
            result = matches

        outputs.text = result


class ReplaceModuleConfig(KiaraModuleConfig):

    replacement_map: typing.Dict[str, str] = Field(
        description="A map, containing the strings to be replaced as keys, and the replacements as values."
    )
    default_value: typing.Optional[str] = Field(
        description="The default value to use if the string to be replaced is not in the replacement map. By default, this just returns the string itself.",
        default=None,
    )


class ReplaceStringModule(KiaraModule):

    _config_cls = ReplaceModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"text": {"type": "string", "doc": "The input string."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"text": {"type": "string", "doc": "The replaced string."}}

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        text = inputs.get_value_data("text")
        repl_map = self.get_config_value("replacement_map")
        default = self.get_config_value("default_value")

        if text not in repl_map.keys():
            if default is None:
                outputs.text = text
            else:
                outputs.text = default
        else:
            outputs.text = repl_map[text]
