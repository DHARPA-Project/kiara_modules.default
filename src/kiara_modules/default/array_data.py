# -*- coding: utf-8 -*-
import copy
import pyarrow as pa
import typing
from concurrent.futures import ThreadPoolExecutor
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module import StepInputs, StepOutputs


class MapModuleConfig(KiaraModuleConfig):

    module_type: str = Field(
        description="The name of the kiara module to use to filter the input data."
    )
    module_config: typing.Optional[typing.Dict[str, typing.Any]] = Field(
        description="The config for the kiara filter module.", default_factory=dict
    )
    input_name: typing.Optional[str] = Field(
        description="The name of the input name of the module which will receive the items from our input array. Can be omitted if the configured module only has a single input.",
        default=None,
    )


class MapModule(KiaraModule):
    """Map a list of values into another list of values.

    Currently, this only supports a single
    """

    _config_cls = MapModuleConfig

    def __init__(self, *args, **kwargs):

        self._child_module: typing.Optional[KiaraModule] = None
        self._module_input_name: typing.Optional[str] = None
        super().__init__(*args, **kwargs)

    @property
    def child_module(self) -> KiaraModule:

        if self._child_module is not None:
            return self._child_module

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        self._child_module = self._kiara.create_module(
            "_map_module", module_name, module_config=module_config
        )
        return self._child_module

    @property
    def module_input_name(self) -> str:

        if self._module_input_name is not None:
            return self._module_input_name

        self._module_input_name = self.get_config_value("input_name")
        if self._module_input_name is None:
            if len(self.child_module.input_names) == 1:
                self._module_input_name = next(iter(self.child_module.input_names))
            else:
                raise KiaraProcessingException(
                    f"No 'input_name' specified, and configured module has more than one inputs. Please specify an 'input_name' value in your module config, pick one of: {', '.join(self.child_module.input_names)}"
                )

        return self._module_input_name

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "array": {
                "type": "array",
                "doc": "The array containing the values the filter is applied on.",
            }
        }
        for input_name, schema in self.child_module.input_schemas.items():
            assert input_name != "array"
            if input_name == self.module_input_name:
                continue
            inputs[input_name] = schema
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "array": {
                "type": "array",
                "doc": "An array of equal length to the input array, containing the 'mapped' values.",
            }
        }
        return outputs

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        input_array: pa.Array = inputs.get_value_data("array")

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        module_obj = self._kiara.create_module(
            "_map_module", module_name, module_config=module_config
        )
        # TODO: validate that the selected module is appropriate
        assert len(module_obj.output_names) == 1

        module_output_name = list(module_obj.output_names)[0]

        init_data = {}
        for input_name in self.input_schemas.keys():
            if input_name in ["array", self.module_input_name]:
                continue

            init_data[input_name] = inputs.get_value_obj(input_name)

        multi_threaded = False
        if multi_threaded:

            def run_module(item):
                _d = copy.copy(init_data)
                _d[self._module_input_name] = item
                r = module_obj.run(**_d)
                return r.get_all_value_data()

            executor = ThreadPoolExecutor()
            results: typing.Any = executor.map(run_module, input_array)
            executor.shutdown(wait=True)

        else:
            results = []
            for item in input_array:
                _d = copy.copy(init_data)
                _d[self._module_input_name] = item
                r = module_obj.run(**_d)
                results.append(r.get_all_value_data())

        result_list = []
        result_types = set()
        for r in results:
            r_item = r[module_output_name]
            result_list.append(r_item)
            result_types.add(type(r_item))

        assert len(result_types) == 1
        outputs.set_value("array", pa.array(result_list))
