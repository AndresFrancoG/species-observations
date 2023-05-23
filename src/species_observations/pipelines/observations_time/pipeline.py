"""
This is a boilerplate pipeline 'observations_time'
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import node_preprocessing_time_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
                func=node_preprocessing_time_data,
                inputs=["species_data", "parameters"],
                outputs="resampled_data",
                name="node_preprocessing_time_data",
            ),
    ])
