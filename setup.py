from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="1.2_main",
    packages=find_packages(),
    install_requires=[
        "requests",  # Agrega aquí tus dependencias
    ],
)