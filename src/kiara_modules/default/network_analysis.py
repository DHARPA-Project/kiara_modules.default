# -*- coding: utf-8 -*-
import copy
import networkx as nx
import pyarrow
import typing
from networkx import DiGraph, Graph
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.module import StepInputs, StepOutputs

# class NetworkGraphModuleConfig(KiaraModuleConfig):
#
#     source_column_name: str = Field(description="")


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


class CreateDirectedGraphModule(KiaraModule):
    """Create a directed network graph object from tabular data."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "edges_table": {
                "type": "table",
                "doc": "The table to extract the edges from.",
            },
            "source_column": {
                "type": "string",
                "default": "source",
                "doc": "The name of the column that contains the edge source in edges table.",
            },
            "target_column": {
                "type": "string",
                "default": "target",
                "doc": "The name of the column that contains the edge target in the edges table.",
            },
            "weight_column": {
                "type": "string",
                "default": "weight",
                "doc": "The name of the column that contains the edge weight in edges table.",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "graph": {"type": "network_graph", "doc": "The (networkx) graph object."},
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        edges_table_value = inputs.get_value_obj("edges_table")
        edges_table_obj: pyarrow.Table = edges_table_value.get_value_data()

        source_column = inputs.source_column
        target_column = inputs.target_column
        weight_column = inputs.weight_column

        min_table = edges_table_obj.select(
            (source_column, target_column, weight_column)
        )
        pandas_table = min_table.to_pandas()

        graph: DiGraph = nx.from_pandas_edgelist(
            pandas_table,
            source_column,
            target_column,
            edge_attr=True,
            create_using=nx.DiGraph(),
        )
        outputs.graph = graph


class AugmentNetworkGraphModule(KiaraModule):
    """Augment an existing graph with new node attributes."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "graph": {"type": "network_graph", "doc": "The network graph"},
            "node_attributes": {
                "type": "table",
                "doc": "The table containing node attributes.",
            },
            "index_column_name": {
                "type": "string",
                "doc": "The name of the column that contains the node index in the node attributes table.",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"graph": {"type": "network_graph", "doc": "The network graph"}}

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        input_graph: Graph = inputs.graph
        graph: Graph = copy.deepcopy(input_graph)

        nodes_table_value = inputs.get_value_obj("node_attributes")
        nodes_table_obj: pyarrow.Table = nodes_table_value.get_value_data()
        nodes_table_index = inputs.index_column_name

        attr_dict = (
            nodes_table_obj.to_pandas()
            .set_index(nodes_table_index)
            .to_dict("index")
            .items()
        )
        graph.add_nodes_from(attr_dict)

        outputs.graph = graph


class FindShortestPathModule(KiaraModule):
    """Find the shortest path between two nodes in a graph."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "graph": {"type": "network_graph", "doc": "The network graph"},
            "source_node": {"type": "string", "doc": "The id of the source node."},
            "target_node": {"type": "string", "doc": "The id of the target node."},
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "path": {"type": "array", "doc": "The shortest path between two nodes."}
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        pass


class ExtractGraphPropertiesModuleConfig(KiaraModuleConfig):

    find_largest_component: bool = Field(
        description="Find the largest component of a graph.", default=True
    )
    number_of_nodes: bool = Field(
        description="Count the number of nodes.", default=True
    )
    number_of_edges: bool = Field(description="Count the number of edges", default=True)
    density: bool = Field(description="Calculate the graph density.", default=True)


class ExtractGraphPropertiesModule(KiaraModule):
    """Extract inherent properties of a network graph."""

    _config_cls = ExtractGraphPropertiesModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"graph": {"type": "network_graph", "doc": "The network graph"}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        result = {}
        if self.get_config_value("find_largest_component"):
            result["largest_component"] = {
                "type": "network_graph",
                "doc": "A sub-graph of just the largest component of the graph.",
            }
            result["density_largest_component"] = {
                "type": "integer",
                "doc": "The density of the largest component.",
            }

        if self.get_config_value("number_of_nodes"):
            result["number_of_nodes"] = {
                "type": "integer",
                "doc": "The number of nodes in the graph.",
            }

        if self.get_config_value("number_of_edges"):
            result["number_of_edges"] = {
                "type": "integer",
                "doc": "The number of edges in the graph.",
            }

        return result

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        graph: Graph = inputs.graph

        if self.get_config_value("find_largest_component"):
            lc_graph = copy.deepcopy(graph)
            # largest_component = max(nx.strongly_connected_components_recursive(lc_graph), key=len)
            lc_graph.remove_nodes_from(
                list(nx.isolates(lc_graph))
            )  # remove unconnected nodes from graph
            lc_density = nx.density(lc_graph)
            outputs.set_values(
                largest_component=lc_graph, density_largest_component=lc_density
            )

        if self.get_config_value("number_of_nodes"):
            outputs.set_values(number_of_nodes=len(graph.nodes))

        if self.get_config_value("number_of_edges"):
            outputs.set_values(number_of_edges=len(graph.edges))

        if self.get_config_value("density"):
            density = nx.density(graph)
            outputs.set_values(density=density)
