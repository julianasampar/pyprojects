from setuptools import find_packages, setup

setup(
    name="dagster_dev",
    packages=find_packages(exclude=["dagster_dev_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
