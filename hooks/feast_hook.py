from airflow.hooks.base_hook import BaseHook
from feast import Client


class FeastHook(BaseHook):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = None
        self.extras = None
        self.client = None

    def get_conn(self):
        """
        Initialize a mailchimp instance.
        """
        if self.client:
            return self.client

        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson
        self.client = Client(core_url=self.extras["core_url"],
                             serving_url=self.extras.get("serving_url"),
                             project=self.extras.get("project"))

        return self.client

    def get_client(self, project: str, create_project: bool = True) -> Client:
        """
        Get feast client with proper project context
        :param project: Feast project name
        :param create_project: Indicate whether a new project should be craeated
          if it doesn't exist
        :return: feast.Client
        """
        client = self.get_conn()
        if client.project != project:
            existing_projects = client.list_projects()
            if project not in existing_projects:
                if not create_project:
                    raise ValueError(f"{project} does not exist. create_project must be set to True to create it.")
                client.create_project(project)

            client.set_project(project)

        return client

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
