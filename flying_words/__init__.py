import os
from dotenv import load_dotenv

version_file = '{}/version.txt'.format(os.path.dirname(__file__))

if os.path.isfile(version_file):
    with open(version_file) as version_file:
        __version__ = version_file.read().strip()

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)
