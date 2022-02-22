from enum import Enum
import dataclasses
import inspect
from typing import Callable
from entitydb.entity import Entity

import entitydb

class SystemCommands(Enum):
    '''
    Return values from this enum in Systems
    to tell the EntityDB what to do
    '''

    # * Entity operations in group 100

    SAVE_ENTITY = 100  # Saves the current entity
    DELETE_ENTITY = 101  # Deletes the current entity

    # * Flow control in group 200

    BREAK = 200  # Stop running this system
    '''
    Works the same as the `break` keyword in loops
    '''


class SystemWrapper():
    def __init__(self, edb: 'entitydb.EntityDB', system: Callable) -> None:
        # Which components we are searching for
        components: dict[str, type] = {}
        optional_components: dict[str, type] = {}
        exclude_components: list[type] = []
        entity_input = ""
        edb_input = ""
        index_input = ""

        # Parse function input to see what to give it
        spec = inspect.getfullargspec(system)
        annotations = spec.annotations
        for arg in spec.args:
            # Check if it is an annotated or plain arg
            if arg in annotations:
                # args includes the type of object this func wants to return, ignore it.
                if arg == "return":
                    continue

                elif dataclasses.is_dataclass(annotations[arg]):
                    if arg.startswith("opt_"):
                        optional_components[arg] = annotations[arg]
                    else:
                        components[arg] = annotations[arg]

                elif annotations[arg] is entitydb.EntityDB:
                    if edb_input:
                        raise Exception(
                            "Can't have multiple of the same type!")
                    edb_input = edb

                elif annotations[arg] is Entity:
                    if entity_input:
                        raise Exception(
                            "Can't have multiple of the same type!")
                    entity_input = arg

                elif annotations[arg] is int:
                    if index_input:
                        raise Exception(
                            "Can't have multiple of the same type!")
                    index_input = arg

            else:
                if arg == "exclude":
                    exclude_components = spec.defaults[0]

        self.edb: entitydb.EntityDB = edb
        self.system: Callable = system
        self.include_components = components
        self.optional_components = optional_components
        self.exclude_components = exclude_components
        self.entity_input = entity_input
        self.edb_input = edb_input
        self.index_input = index_input

    def run(self, entity: Entity, index: int) -> list[SystemCommands]:
        '''
        Runs the system using fields from the object. Assumes that the entity has
        all of the correct fields for this function signature.

        Will throw an exception if the entity passed in does not match!
        '''
        args: dict[str, object] = {}

        # Add components to args
        for arg_name in self.include_components:
            args[arg_name] = entity._components[self.include_components[arg_name]]

        # Add optional components to args
        for arg_name in self.optional_components:
            if self.optional_components[arg_name] in entity._components:
                args[arg_name] = entity._components[self.optional_components[arg_name]]
            else:
                args[arg_name] = None

        # Add the Entity
        if self.entity_input:
            args[self.entity_input] = entity

        # Add the EntityDB
        if self.edb_input:
            args[self.edb_input] = self.edb

        # Add the index
        if self.index_input:
            args[self.index_input] = index

        system_output = self.system(**args)

        # Parse the output and return a system command list
        result: list[SystemCommands] = []

        # Let the system return stuff in various formats for ease of use
        if system_output:
            if isinstance(system_output, list):
                result = system_output
            elif isinstance(system_output, SystemCommands):
                result = [system_output]

        return result

    def get_components_from_signature(self) -> list[type]:
        '''
        Returns every component type mentioned in this system's signature (via type hints),
        except for excluded components because we don't care too much about them.
        '''
        # ? Probably don't need to know about excluded components, as this function is only
        # ? used by the EntityDB to register them. Might change in the future, keep an eye on this.
        # + self.exclude_components
        return list(self.include_components.values()) + list(self.optional_components.values())
