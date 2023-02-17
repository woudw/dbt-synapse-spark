from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Optional
import dbt.exceptions # noqa
from dbt.adapters.base import Credentials

from dbt.adapters.sql import SQLConnectionManager

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.adapters.synapsespark import synapse_spark

@dataclass
class SynapseSparkCredentials(Credentials):
    workspace: str
    database: Optional[str]
    authentication: str
    user: str
    spark_pool: str
    cluster_configuration: Dict[str, str | int]
    
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
            raise dbt.exceptions.RuntimeException(str(exc))



    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        print("open connection")
        # ## Example ##
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            handle = synapse_spark.connect(
                workspace_name=credentials.workspace,
                authentication=credentials.authentication,
                spark_pool_name=credentials.spark_pool,
                user=credentials.user,
                conf=credentials.cluster_configuration

                # host=credentials.host,
                # port=credentials.port,
                # username=credentials.username,
                # password=credentials.password,
                # catalog=credentials.database
            )
            connection.state = "open"
            connection.handle = handle
        except Exception as exc:
            print(f"Exception: {exc}")
            logger.error(exc)
        print(f"Connection: {connection}")
        return connection

    @classmethod
    def get_response(cls,cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse ojbect
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        print("get_response()")
        # ## Example ##
        # return cursor.status_message
        # pass

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        # ## Example ##
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query "{}" ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query "{}": {}".format(connection_name, res))
        pass
