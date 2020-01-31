from airflow.plugins_manager import AirflowPlugin
from faucet_plugin.hooks.feast_hook import FeastHook
from faucet_plugin.operators.bigquery_to_feast_operator import BigQueryToFeastOperator


class FaucetPlugin(AirflowPlugin):
    name = "faucet_plugin"
    operators = [
        BigQueryToFeastOperator,
    ]
    hooks = [FeastHook]
