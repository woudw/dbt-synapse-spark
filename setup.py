#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-synapse-spark"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.3.0"
description = """The SynapseSpark adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="woudw",
    author_email="ewoudwesterbaan@gmail.com",
    url="<- todo ->",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.3.0",
        "azure-identity==1.12.0",
        "azure-synapse==0.1.1",
        "azure-synapse-spark==0.7.0"
    ],
)
