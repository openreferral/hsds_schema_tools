from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install


install_requires = [
    "click",
    "flatterer",
    "compiletojsonschema",
]

setup(
    name="hsds_schema_tools",
    version="0.0.8",
    author="Open Data Services",
    author_email="code@opendataservices.coop",
    py_modules=["hsds_schema"],
    scripts=["hsds_schema.py"],
    url="https://github.com/openreferral/hsds_schema_tool/s",
    license="MIT",
    description="Tools for dealing with HSDS schema",
    install_requires=install_requires,
)
