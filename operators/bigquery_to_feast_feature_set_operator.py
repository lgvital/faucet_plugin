from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from faucet_plugin.hooks.feast_hook import FeastHook
from feast import Client, FeatureSet, Entity, ValueType
from google.protobuf.duration_pb2 import Duration


class BigQueryToFeastFeatureSetOperator(BaseOperator):
    def __init__(
            self,
            conn_id: str,
            project: str,
            feature_set_name: str,
            entity_name: str,
            sql: str,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.feast_client = FeastHook(conn_id).get_client(project)
        self.project = project
        self.feature_set_name = feature_set_name
        self.entity_name = entity_name
        self.sql = sql
        self.bq = BigQueryHook(use_legacy_sql=False, location='US')

    def execute(self, context):
        features_df = self.bq.get_pandas_df(self.sql)
        fs = FeatureSet(
            self.feature_set_name,
            max_age=Duration(seconds=86400),
            entities=[Entity(name=self.entity_name, dtype=ValueType.INT64)]
        )
        fs.infer_fields_from_df(features_df, replace_existing_features=True)
        self.feast_client.apply(fs)
