from airflow.plugins_manager import AirflowPlugin
from faucet_plugin.hooks.feast_hook import FeastHook
from faucet_plugin.operators.bigquery_to_feast_operator import BigQueryToFeastOperator
from faucet_plugin.operators.bigquery_to_feast_feature_set_operator import \
    BigQueryToFeastFeatureSetOperator
from faucet_plugin.operators.feast_feature_set_operator import FeastFeatureSetOperator


class FaucetPlugin(AirflowPlugin):
    name = "faucet_plugin"
    operators = [
        BigQueryToFeastOperator,
        BigQueryToFeastFeatureSetOperator,
        FeastFeatureSetOperator
    ]
    hooks = [FeastHook]
