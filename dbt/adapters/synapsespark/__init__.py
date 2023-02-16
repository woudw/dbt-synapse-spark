from dbt.adapters.synapsespark.connections import SynapseSparkConnectionManager # noqa
from dbt.adapters.synapsespark.connections import SynapseSparkCredentials
from dbt.adapters.synapsespark.impl import SynapseSparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import synapsespark


Plugin = AdapterPlugin(
    adapter=SynapseSparkAdapter,
    credentials=SynapseSparkCredentials,
    include_path=synapsespark.PACKAGE_PATH
    )
