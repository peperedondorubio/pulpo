# pulpo
Módulo de apoyo de los contenedores


## Para instalarlo en un proyecto poner:

Crear una deploy key para la máquina cliente (id_rsa_mi_logs) en github.com (settings)

ssh-keygen -t rsa -b 4096


## Estructura (hay que hacerlo así)

mi_logs/
├── mi_logs/
│   ├── __init__.py
│   └── otros_archivos.py
├── setup.py
├── requirements.txt
└── README.md


eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa_mi_logs

pip install git+ssh://git@github.com/peperedondorubio/mi_logs.git@main#egg=mi_logs

## y para desinstalarlo

pip uninstall mi_logs


## si no funciona

pip cache purge




