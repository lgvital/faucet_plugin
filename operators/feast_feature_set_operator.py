from airflow.models import BaseOperator
from faucet_plugin.hooks.feast_hook import FeastHook
from feast import Client, FeatureSet


class FeastFeatureSetOperator(BaseOperator):
    def __init__(
            self,
            conn_id: str,
            project: str,
            feature_set_dict: dict = None,
            feature_set_yaml: str = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        if feature_set_dict is None and feature_set_yaml is None:
            raise ValueError('Please include either feature_set_dict or feature_set_yaml')

        if feature_set_dict is not None and feature_set_yaml is not None:
            raise ValueError('Please only specify one feature_set_dict or feature_set_yaml')

        self.feast_client = FeastHook(conn_id).get_client(project)
        self.project = project
        self.feature_set_dict = feature_set_dict
        self.feature_set_yaml = feature_set_yaml

    def execute(self, context):
        if self.feature_set_dict:
            fs = FeatureSet.from_dict(self.feature_set_dict)

        if self.feature_set_yaml:
            fs = FeatureSet.from_yaml(self.feature_set_yaml)

        self.feast_client.apply(fs)
