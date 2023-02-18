from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Optional
import dbt.exceptions # noqa
from dbt.adapters.base import Credentials

from dbt.events import AdapterLogger

from dbt.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLConnectionManager

from dbt.adapters.synapsespark.synapse_spark import LivyCursor, LivySessionFactory, LivySessionWrapper

import time

logger = AdapterLogger("SynapseSpark")

@dataclass
class SynapseSparkCredentials(Credentials):
    workspace: str
    database: Optional[str]
    authentication: str
    user: str
    spark_pool: str
    cluster_configuration: Dict[str, str | int]
    poll_interval: int
    
    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data
    
    def __post_init__(self):
        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

    @property
    def type(self):
        """Return name of adapter."""
        return "synapsespark"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("workspace","authentication","user")

class SynapseSparkConnectionManager(SQLConnectionManager):
    TYPE = "synapsespark"


    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        # ## Example ##
        # try:
        #     yield
        # except myadapter_library.DatabaseError as exc:
        #     self.release(connection_name)

        #     logger.debug("myadapter error: {}".format(str(e)))
        #     raise dbt.exceptions.DatabaseException(str(exc))
        # except Exception as exc:
        #     logger.debug("Error running SQL: {}".format(sql))
        #     logger.debug("Rolling back transaction.")
        #     self.release(connection_name)
        #     raise dbt.exceptions.RuntimeException(str(exc))
        try:
            yield
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            logger.debug(exc)
            raise dbt.exceptions.RuntimeException(str(exc))

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.

        An handle is this case is actually a statement. So a thread will create
        a new statement on a Livy Session.
        """
        start_time = time.process_time()
        #do some stuff
        # ## Example ##
        if connection.state == 'open':
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            handle = LivySessionFactory(
                workspace_name=credentials.workspace,
                authentication=credentials.authentication,
                spark_pool_name=credentials.spark_pool,
                user=credentials.user,
                conf=credentials.cluster_configuration,
                poll_interval=credentials.poll_interval
            ).connect().get_statement()
            connection.state = "open"
            connection.handle = handle
        except Exception as exc:
            # print(f"Exception: {exc}")
            logger.error(exc)
        elapsed_time = time.process_time() - start_time
        logger.debug(f"SynapseSparkConnectionManager - open(): {elapsed_time}")
        return connection

    @classmethod
    def get_response(cls,cursor: LivyCursor) -> AdapterResponse:
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse ojbect
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        logger.debug("SynapseSparkConnectionManager - get_response()")
        code = cursor.get_sql_state() or "OK"
        status_message = f"{code}"
        return AdapterResponse(
            _message=status_message,
            code=code,
            rows_affected=0
        )

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        logger.debug("SynapseSparkConnectionManager - cancel()")
        handle: LivySessionWrapper = connection.handle
        handle.close()
        # ## Example ##
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query "{}" ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query "{}": {}".format(connection_name, res))
        # pass
