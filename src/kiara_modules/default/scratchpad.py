# -*- coding: utf-8 -*-
import pyarrow
import typing

from kiara import KiaraModule
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs


class PrepareNodesTableLenaModule(KiaraModule):
    """Prepare tabular data so it can be used as a 'nodes_table' input in the a directed graph module.

    This is a very specific module, only accepting a very specific data format and as such only suitable as a proof-of-concept.
    Later on, this will be replaced by a more generic module (or pipeline).
    """

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"table": {"type": "table", "doc": "The 'raw' table incl. edges."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "table": {
                "type": "table",
                "doc": "A normalized table where every row represents the metadata for a single network node.",
            },
            "index_column_name": {
                "type": "string",
                "doc": "The name of the column that contains the node identifier.",
            },
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        t: pyarrow.Table = inputs.table
        df = t.to_pandas()

        df1 = df.iloc[:, 0:11]
        df1.columns = [
            "Id",
            "LabelOrig",
            "LabelTrans",
            "Year",
            "Type",
            "Language",
            "City",
            "CountryOld",
            "CountryNew",
            "Latitude",
            "Longitude",
        ]
        df2 = df.iloc[
            :, 11:
        ]  # This slices the dataframe in half creating a df of just the TargetJournals data
        df2.columns = [
            "Id",
            "Year",
            "LabelOrig",
            "LabelTrans",
            "Type",
            "Language",
            "City",
            "CountryOld",
            "CountryNew",
            "Latitude",
            "Longitude",
        ]
        extr_nodes = df1.append(df2)
        extr_nodes_unique = extr_nodes.drop_duplicates(subset=["Id"])

        result = pyarrow.Table.from_pandas(extr_nodes_unique)
        outputs.table = result

        outputs.index_column_name = "Id"
