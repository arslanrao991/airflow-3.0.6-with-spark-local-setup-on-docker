from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
# from airflow.utils.decorators import apply_defaults
import requests


class GithubFileOperator(BaseOperator):
    """
    Custom Operator to fetch a file from a GitHub repository using an Airflow connection.
    """

    # @apply_defaults
    def __init__(self, 
                 github_conn_id: str,
                 repo_name: str,
                 file_path: str,
                 ref: str = 'main',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.github_conn_id = github_conn_id
        self.repo_name = repo_name
        self.file_path = file_path
        self.ref = ref

    def execute(self, context):
        self.log.info(f"Fetching file '{self.file_path}' from repo '{self.repo_name}' (ref: {self.ref})")

        github_conn = BaseHook.get_connection(self.github_conn_id)
        github_session = requests.Session()
        github_session.auth = (github_conn.login, github_conn.password)

        url = f"https://raw.githubusercontent.com/{github_conn.login}/{self.repo_name}/{self.ref}/{self.file_path}"
        self.log.info(f"Fetching from URL: {url}")

        response = github_session.get(url)

        if response.status_code == 200:
            file_content = response.text
            self.log.info(f"Successfully fetched file: {self.file_path}")
            return file_content
        else:
            raise Exception(f"Failed to fetch file. Status code: {response.status_code}")


class GithubPlugin(AirflowPlugin):
    name = "github_plugin"
    operators = [GithubFileOperator]
