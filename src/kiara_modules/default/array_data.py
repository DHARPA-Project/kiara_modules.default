# -*- coding: utf-8 -*-
import pyarrow as pa
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs


class MapModuleConfig(KiaraModuleConfig):

    module_type: str = Field(
        description="The name of the kiara module to use to map the input data."
    )
    module_config: typing.Optional[typing.Dict[str, typing.Any]] = Field(
        description="The config for the kiara module.", default_factory=dict
    )


class MapModule(KiaraModule):

    _config_cls = MapModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {"array": {"type": "array", "doc": "The input array."}}
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {"array": {"type": "array", "doc": "The output array."}}
        return outputs

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        input_array: pa.Array = inputs.array

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        create_date = self._kiara.create_module(
            "_map_module", module_name, module_config=module_config
        )

        assert len(create_date.input_names) == 1
        assert len(create_date.output_names) == 1
        create_date_input_name = list(create_date.input_names)[0]
        create_date_output_name = list(create_date.output_names)[0]

        result = []
        for text in input_array:
            s = str(text)
            init_data = {create_date_input_name: s}

            r = create_date.run(**init_data)
            result.append(r[create_date_output_name])

        test = pa.array(result)
        outputs.array = test
