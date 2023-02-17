import time
from dbt.events import AdapterLogger
from types import TracebackType
from typing import Any, Dict
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.synapse import SynapseClient
from azure.synapse.operations import SparkSessionOperations
from azure.synapse.spark.models import SparkSession
from azure.synapse.models import LivyStatementRequestBody, LivyStatementResponseBody, ExtendedLivySessionRequest, ExtendedLivyListSessionResponse, ExtendedLivySessionResponse
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions

logger = AdapterLogger("SynapseSpark")

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
        self.statement_id = -1
        self.poll_interval = 1

    def __init__(self, session_id, 
                 spark_session_operations: SparkSessionOperations, 
                 workspace_name, spark_pool_name, poll_interval) -> None:
        self._rows = None
        self._schema = None
        self.session_id = session_id
        self.spark_session_operations = spark_session_operations
        self.workspace_name = workspace_name
        self.spark_pool_name = spark_pool_name
        self.statement_id = -1
        self.poll_interval = poll_interval

    def __enter__(self):
        # print("LivyCursor - enter")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        # print("LivyCursor - exit")
        self.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        print("LivyCursor - description")
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
        logger.debug("LivyCursor - close")
        self._rows = None
        
    def _submitLivyCode(self, code) -> int:
        logger.debug(f"""Executing query: 
        {code}
        """)

        response: LivyStatementResponseBody = self.spark_session_operations.create_statement(
            self.workspace_name, self.spark_pool_name, self.session_id,
            LivyStatementRequestBody(kind='sql', code=code))

        return response.id


    def _get_statement(self) -> LivyStatementResponseBody:
        logger.debug("LivyCursor - _get_statement")
        result = self.spark_session_operations.get_statement(self.workspace_name, 
                                                        self.spark_pool_name, 
                                                        self.session_id,
                                                        self.statement_id)
        return result

    def get_sql_state(self):
        logger.debug("LivyCursor - get_sql_state()")
        return self._get_statement().output.status


    def _getLivyResult(self):
        logger.debug("LivyCursor - _getLivyResult")
        while True:
            result = self._get_statement()
            previous_state = 'unknown'
            current_state = result.state
            if current_state != previous_state:
                logger.debug(f"Query status: {current_state}")
            if current_state == 'available':
                return result
            time.sleep(self.poll_interval)

    def cancel(self):
        logger.debug(f"Cancelling query: {self.statement_id}")
        self.spark_session_operations.delete_statement(
            self.workspace_name,
            self.spark_pool_name,
            self.session_id,
            self.statement_id
        )
        while True:
            result = self._get_statement()
            previous_state = 'unknown'
            current_state = result.state
            if current_state != previous_state:
                logger.debug(f"Query status: {current_state}")
            if current_state in ('available', 'error', 'cancelled'):
                return


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
        logger.debug("LivyCursor - execute")
        logger.debug(sql)
        # if len(parameters) > 0:
        #     sql = sql % parameters
        
        # TODO: handle parameterised sql

        statement_id = self._submitLivyCode(sql)
        self.statement_id = statement_id

        res = self._getLivyResult()
        if (res.output.status == 'ok'):
            # values = res['output']['data']['application/json']
            values = res.output.data['application/json']
            # print(values)
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
                        'Error while executing query: ' + res.output.evalue
                    ) 

    def fetchall(self):
        """
        Fetch all data.

        Return
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

class SynapseStatement:
    """The handle of the connection."""

    def __init__(self, livy_session_id, spark_session_operations, 
                 workspace_name, spark_pool_name, poll_interval):
        self._cursor = LivyCursor(livy_session_id, spark_session_operations, 
                                     workspace_name, spark_pool_name, 
                                     poll_interval)


    def cursor(self):
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return self._cursor
    
    def cancel(self) -> None:
        self._cursor.cancel()

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        logger.debug("Connection.close()")
        self._cursor.close()

class LivySessionWrapper():
    """Wrapper around a session to support calls as a handle."""
    def __init__(self, livy_session_id: int, spark_session_operations: SparkSessionOperations, workspace_name, spark_pool_name, poll_interval):
        logger.debug("Creating LivySessionWrapper")
        self.livy_session_id = livy_session_id
        self.spark_session_operations = spark_session_operations 
        self.workspace_name = workspace_name
        self.spark_pool_name = spark_pool_name
        self.poll_interval = poll_interval
        
    def get_statement(self) -> SynapseStatement:
        return SynapseStatement(self.livy_session_id, 
            self.spark_session_operations, self.workspace_name, 
            self.spark_pool_name, self.poll_interval)



class LivySessionFactory():

    def __init__(self, workspace_name: str, spark_pool_name: str, user: str, 
                 authentication: str, conf: Dict[str, str | int],
                 poll_interval: int):
        self.workspace_name = workspace_name
        self.spark_pool_name = spark_pool_name
        self.session_name = f'dbt-{user}'
        self.conf = conf
        self.poll_interval = poll_interval
        if authentication == 'DefaultAzureCredential':
            credential = DefaultAzureCredential()
        elif authentication == 'AzureCliCredential':
            credential = AzureCliCredential()
        synapse_client = SynapseClient(credential)
        self.spark_session_operations: SparkSessionOperations = synapse_client.spark_session


    SESSION: LivySessionWrapper = None

    def connect(self) -> LivySessionWrapper:
        if LivySessionFactory.SESSION is not None:
            logger.debug("Can reuse session")
            return LivySessionFactory.SESSION
        session = self.get_existing_session()
        if session is None:
            logger.debug('Did not find an existing session')
            session = self.create_new_session()
        else:
            logger.debug(f'Found existing session (id: {session.livy_session_id})')
        LivySessionFactory.SESSION = session
        return session


    def create_new_session(self):
        logger.debug('Creating a new session')
        livy_session_response: ExtendedLivySessionResponse = self.spark_session_operations.create(
            self.workspace_name, self.spark_pool_name, 
            ExtendedLivySessionRequest(
                name=self.session_name, 
                driver_cores=self.conf['driver_cores'],
                driver_memory=self.conf['driver_memory'],
                executor_cores=self.conf['executor_cores'],
                executor_memory=self.conf['executor_memory'],
                num_executors=self.conf['num_executors']
            )
        )
        session_id = livy_session_response.id
        return self.wait_for_available(session_id)


    def get_existing_session(self) -> LivySessionWrapper:
        logger.debug(f'Searching for existing session with name {self.session_name}')
        start = 0
        size = 20
        while True:
            session_list: ExtendedLivyListSessionResponse = self.spark_session_operations.list(
                self.workspace_name, self.spark_pool_name, from_parameter=start, size=size,
                detailed=True)
            for session in session_list.sessions:
                if session.name == self.session_name:
                    logger.debug(f"Found {session.name} ({session.id}): {session.state}")
                    if session.state != 'dead':
                        return self.wait_for_available(session.id)
            if len(session_list.sessions) == 0:
                return None
            start = start+size


    def wait_for_available(self, session_id: int):
        previous_state = 'Unknown state'
        state = previous_state
        while True:
            livy_session_response = self.spark_session_operations.get(
                self.workspace_name, self.spark_pool_name, session_id)
            state = livy_session_response.state
            if state != previous_state:
                logger.debug(f"Session state ({session_id}): {state}")
                previous_state = state
            time.sleep(self.poll_interval)
            if state == 'idle':
                break
            if state == 'dead':
                logger.debug(f'Session ({session_id}) is dead')
                return None
        return LivySessionWrapper(session_id, self.spark_session_operations, 
            self.workspace_name, self.spark_pool_name, self.poll_interval)

