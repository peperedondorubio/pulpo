# pulpo
Módulo de apoyo de los contenedores


## Para instalarlo en un proyecto poner:

Crear una deploy key para la máquina cliente (id_rsa_pulpo) en github.com (settings)

ssh-keygen -t rsa -b 4096


## Estructura (hay que hacerlo así)

pulpo/
├── pulpo/
│   ├── __init__.py
│   └── otros_archivos.py
├── setup.py
├── requirements.txt
└── README.md

## Rellenar el setup.py

from setuptools import setup, find_packages

setup(
    name="pulpo",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests",  # Agrega aquí tus dependencias
    ],
)

## Entorno de ssh

eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa_pulpo

## Instalacion

Commit del pulpo en github

### Privado
pip install git+ssh://git@github.com/peperedondorubio/pulpo.git@main#egg=pulpo

### Publico
pip install git+http://git@github.com/peperedondorubio/pulpo.git@main#egg=pulpo


## y para desinstalarlo

pip uninstall pulpo

## si no funciona

pip cache purge




