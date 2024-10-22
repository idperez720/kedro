"""
This is a boilerplate pipeline 'first_load'
generated using Kedro 0.19.9
"""

from kedro.pipeline import Pipeline, pipeline
from kedro.pipeline.node import node

from maverick_data_lake.pipelines.first_load.nodes.ergast_f1_drivers import (
    create_drivers_data,
)


def create_pipeline(**kwargs) -> Pipeline:  # pylint: disable=unused-argument
    """This pipeline is used to load the data from the Ergast F1 API and store it in the data lake.

    Returns:
        Pipeline: The pipeline that loads the data from the Ergast F1 API and stores it in
        the data lake.
    """
    return pipeline(
        [
            node(
                func=create_drivers_data,
                inputs=["params:catalog_info_drivers_api"],
                outputs="drivers_data@spark",
                name="create_drivers_data_table",
                tags=["first_load", "ergast_f1"],
            )
        ]
    )
