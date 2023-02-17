import time
from types import TracebackType
from typing import Any, Dict
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.synapse import SynapseClient
from azure.synapse.operations import SparkSessionOperations
from azure.synapse.spark.models import SparkSession
from azure.synapse.models import LivyStatementRequestBody, LivyStatementResponseBody, ExtendedLivySessionRequest, ExtendedLivyListSessionResponse, ExtendedLivySessionResponse
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions


class LivyCursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self) -> None:
        self._schema = None
        self._rows = None
        self.session_id = -1
        self.spark_session_operations = None
        self.workspace_name = None
        self.spark_pool_name = None

    def __init__(self, session_id, 
                 spark_session_operations: SparkSessionOperations, 
                 workspace_name, spark_pool_name) -> None:
        self._rows = None
        self._schema = None
        self.session_id = session_id
        self.spark_session_operations = spark_session_operations
        self.workspace_name = workspace_name
        self.spark_pool_name = spark_pool_name

    def __enter__(self):
        print("LivyCursor - enter")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        print("LivyCursor - exit")
        self.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        print("LivyCursor - decription")
        """
        Get the description.

        Returns
        -------
        out : list[tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._schema is None:
            description = list()
        else:
            description = [
                (
                    field['name'],
                    field['type'], # field['dataType'],
                    None,
                    None,
                    None,
                    None,
                    field['nullable'],
                )
                for field in self._schema
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        print("LivyCursor - close")
        self._rows = None
        
    def _submitLivyCode(self, code) -> int:
        print("LivyCursor - _getLivyResult")
        logger.debug(f"""Executing query: 
        {code}
        """)

        response: LivyStatementResponseBody = self.spark_session_operations.create_statement(
            self.workspace_name, self.spark_pool_name, self.session_id,
            LivyStatementRequestBody(kind='sql', code=code))

        return response.id


    def _getLivyResult(self, statement_id: int):
        print("LivyCursor - _getLivyResult")
        while True:
            result = self.spark_session_operations.get_statement(self.workspace_name, 
                                                        self.spark_pool_name, 
                                                        self.session_id,
                                                        statement_id)
            if result.state == 'available':
                return result
            print("Waiting for query results to become available")
            time.sleep(2)


    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        print("LivyCursor - execute")
        # if len(parameters) > 0:
        #     sql = sql % parameters
        
        # TODO: handle parameterised sql

        res = self._getLivyResult(self._submitLivyCode(sql))
        if (res.output.status == 'ok'):
            # values = res['output']['data']['application/json']
            values = res.output.data['application/json']
            print(values)
            if (len(values) >= 1):
                self._rows = values['data'] # values[0]['values']
                self._schema = values['schema']['fields'] # values[0]['schema']
                # print("rows", self._rows)
                # print("schema", self._schema)
            else:
                self._rows = []
                self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise dbt.exceptions.raise_database_error(
                        'Error while executing query: ' + res['output']['evalue']
                    ) 

    def fetchall(self):
        """
        Fetch all data.

        Returns
        -------
        out : list() | None
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        return self._rows

    def fetchone(self):
        """
        Fetch the first output.

        Returns
        -------
        out : one row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """
       
        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row


class LivySessionWrapper():
    """Wrapper around a session to support calls as a handle."""
    def __init__(self, livy_session_id: int, spark_session_operations: SparkSessionOperations, workspace_name, spark_pool_name):
        print("Creating LivySessionWrapper")
        self.livy_session_id = livy_session_id
        self.spark_session_operations = spark_session_operations 
        self._cursor = LivyCursor(livy_session_id, spark_session_operations, workspace_name, spark_pool_name)

    def cursor(self):
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return self._cursor

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        logger.debug("Connection.close()")
        self._cursor.close()


def connect(workspace_name: str, spark_pool_name: str, user: str, authentication: str, conf: Dict[str, str | int]) -> LivySessionWrapper:
    print("Connecting")
    if authentication == 'DefaultAzureCredential':
        credential = DefaultAzureCredential()
    elif authentication == 'AzureCliCredential':
        credential = AzureCliCredential()
    synapse_client = SynapseClient(credential)
    spark_session_operations: SparkSessionOperations = synapse_client.spark_session

    session_name = f'dbt-{user}'

    session = get_existing_session(workspace_name, spark_pool_name, session_name, spark_session_operations)

    if session is None:
        print('Did not find an existing session')
        session = create_new_session(workspace_name, spark_pool_name, spark_session_operations, session_name, conf)
    else:
        print(f'Found existing session (id: {session.livy_session_id})')
    return session


def create_new_session(workspace_name, spark_pool_name, spark_session_operations: SparkSessionOperations,
                       session_name, conf):
    print('Creating a new session')
    livy_session_response: ExtendedLivySessionResponse = spark_session_operations.create(
        workspace_name, spark_pool_name, 
        ExtendedLivySessionRequest(
            name=session_name, 
            driver_cores=conf['driver_cores'],
            driver_memory=conf['driver_memory'],
            executor_cores=conf['executor_cores'],
            executor_memory=conf['executor_memory'],
            num_executors=conf['num_executors']
        )
    )
    state = livy_session_response.state
    session_id = livy_session_response.id
    print(state)
    while True:
        livy_session_response = spark_session_operations.get(workspace_name, spark_pool_name, session_id)
        state = livy_session_response.state
        print(state)
        time.sleep(2)
        if state == 'idle':
            break
        if state == 'dead':
            print('Session is dead')
            return None
    return LivySessionWrapper(session_id, spark_session_operations, workspace_name, spark_pool_name)


def get_existing_session(workspace_name: str, spark_pool_name: str, session_name: str, spark_session_operations: SparkSessionOperations) -> LivySessionWrapper:
    print(f'Searching for existing session with name {session_name}')
    start = 0
    size = 20
    while True:
        session_list: ExtendedLivyListSessionResponse = spark_session_operations.list(
            workspace_name, spark_pool_name, from_parameter=start, size=size,
            detailed=True)
        for session in session_list.sessions:
            print(f"{session.name} ({session.id}): {session.state}")
            if session.name == session_name and session.state == 'idle':
                # print(session.state)
                return LivySessionWrapper(session.id, spark_session_operations, workspace_name, spark_pool_name)
        start = start+size
        if len(session_list.sessions) == 0:
            return None
        
        # Todo als we er een vinden in starting phase, dan nemen we deze

# def wait_for_idle(workspace_name: str, spark_pool_name: str, session_name: str, spark_session_operations: SparkSessionOperations)

