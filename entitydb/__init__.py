# Import essential classes
from entitydb.system import SystemCommands
from entitydb.entity import Entity
from entitydb.entitydb import EntityDB

# Components are actually python dataclasses
# This line is here for faster importing in other files
from dataclasses import dataclass as component
