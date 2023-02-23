## Thanks to:
[dbt-spark-livy ](https://github.com/cloudera/dbt-spark-livy)

Most of the code it theirs (or dbt-spark). I only converted it to use the Azure 
Synapse Library. The only actually change is in `connections.py` and 
`synapse_spark.py`.

## Warning:
**This is in an experimental phase. A lot of testing still has to be done.**

Also, this library relies on the Microsoft library for connecting to Azure
Synapse which is in preview.

## Example profile
```yaml
my_dbt_project:
  outputs:
    dev:
      workspace: synapse_workspace
      schema: my_schema
      authentication: AzureCliCredential # (or DefaultAzureCredential)
      threads: 4
      type: synapsespark
      user: whoami
      spark_pool: MySparkPool
      cluster_configuration:
        driver_memory: "4g"
        driver_cores: 4
        executor_memory: "4g"
        executor_cores: 4
        num_executors: 2

  target: dev

```

## Authentication
This library uses azure-identity for authentication. You can use the example
profile after you have been logged in to Azure (e.g. `az login`, with the Azure
CLI).
