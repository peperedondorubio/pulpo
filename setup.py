from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="1.6.2",
    packages=find_packages(),
    install_requires=[
        "requests", 
        "pyjwt",
        "aiokafka",
        "python-arango"
    ],
)