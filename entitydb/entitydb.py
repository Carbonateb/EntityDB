
import inspect


from typing import Callable, Type

from entitydb.entity import Entity
from entitydb.system import SystemCommands, SystemWrapper


# Constants
PRIMARY_KEY = "_uid"
ENTITY_TABLE = "_entities"
ENTITY_REFERENCE = "_entity"


class EntityDB():
    '''
    Equivalent to the "world" in ECS terminology.
    Stores entities, can be queried.
    By default has no storage, so cannot function.
    Use the SQLite or Google Cloud Storage versions
    '''

    def __init__(self) -> None:
        self.component_classes: dict[str, type] = dict()
        '''Component classes we have registered'''

    def add_entity(self, entity: Entity) -> int:
        '''
        Adds an entity, returns its ID
        '''
        raise NotImplementedError()

    def new_entity(self, components: list[object]) -> int:
        '''
        Creates a new entity from a list of components.
        Adds it, then returns its ID.
        '''
        return self.add_entity(Entity(components))

    def update_entity(self, entity: Entity) -> bool:
        '''
        Updates the components of an entity that has already been
        saved to the database.
        '''
        raise NotImplementedError()

    def delete_entity(self, entity: Entity) -> None:
        raise NotImplementedError()

    def run(self, system_func: Callable) -> None:
        '''
        Runs a system, parses the function signature & type hints to know what entities to pass in.

        ## Rules
        - All inputs are required systems by default, unless one of the following applies
        - Make params optional by prefixing name with `opt_*`.
        - Exclude components with: `exclude=[MyExcludedComponent, AnotherComponent]`.
        - Get the entity by adding `entity:Entity` to the params.
        - The EntityDB can be passed in by a param annotated with `EntityDB`. Can be used to create new entities or run more systems.
        - The loop index can be passed in by adding an `int` paramater
        '''
        raise NotImplementedError()

    def count_matches(self, system_func: Callable) -> int:
        '''
        Counts how many entities match the system's signature.
        If you were to run the system, this is how many times it will
        be called.

        TODO some code can be shared with EntityDB.run()
        '''
        raise NotImplementedError()

    def load_component(self, entity: Entity, component_type: type) -> bool:
        '''
        Loads a given component onto an entity, by reading its values from the db.
        If the component already exists on the entity, it is overwritten.

        If this returns True, makes some changes to the given entity:
        - Loads a new component on `Entity._components`
        - Removes the component from `Entity._unloaded_components`, if it was there.
        '''
        raise NotImplementedError()

    def _register_component_type(self, component: type) -> bool:
        '''
        Checks if this component has been seen before, if not, it
        registers it and calls the _setup_component_type function.

        Returns False if the component has already been registered
        '''
        component_name = component.__name__
        if component_name not in self.component_classes:
            self.component_classes[component_name] = component
            self._setup_component_type(component)
            return True
        return False

    def _setup_component_type(self, component: type) -> None:
        '''
        Called when a new component is registered. Only called once for each
        component. Always called before a component type is used for the first time.

        Use this as an opportunity to setup any data structures needed to store this component.
        For example, create the tables in an SQL database.
        '''
        pass

    def _run_on_entities(self, system: SystemWrapper, entity_components: dict[str, any]) -> None:
        '''
        entity_components is a dict:
        {
            eid: {
                ComponentA: cid,
                ComponentB: cid
            },
        }

        '''
        index = 0
        for eid in entity_components:
            # TODO optimization: read below
            # Don't bother trying to load every component from DB this entity has,
            # just load the ones that this function calls for
            entity: Entity = self._load_entity_from_cids(
                eid, entity_components[eid])
            commands = system.run(entity, index)

            # * Run the commands

            if SystemCommands.DELETE_ENTITY in commands:
                self.delete_entity(entity)

            elif SystemCommands.SAVE_ENTITY in commands:
                self.update_entity(entity)

            if SystemCommands.BREAK in commands:
                break

            index += 1

    def _create_component_from_data(self, component_type: type, component_data: dict) -> object:
        # Just get the actual values, strip the extra stuff
        component_values: dict = {}
        for varname in component_data:
            # Exclude vars that are in the table but not passed into the constructor
            # This should just be the primary key and entity reference
            if varname not in [PRIMARY_KEY, ENTITY_REFERENCE]:
                component_values[varname] = component_data[varname]
        return component_type(**component_values)

    def _load_entity_from_cids(self, eid: str, components: dict[str, any]) -> Entity:
        '''
        Takes a dict of component_name to cids, and creates an entity from it.
        '''
        raise NotImplementedError()
    
    def _parse_system(self, system_func:Callable) -> SystemWrapper:
        '''
        Analises the passed in system, putting it in a wrapper to easily access
        its properties.
        '''
        result = SystemWrapper(self, system_func)
        # Might be some components in the call signature we haven't seen yet
        for component_type in result.get_components_from_signature():
            self._register_component_type(component_type)
        return result

    @classmethod
    def get_variables_of(cls, o: object) -> dict[str, object]:
        '''
        Returns the public variables of this object.
        Result is a dict of the var name, to its value
        '''
        result = dict()

        var_names = dir(o)
        for var_name in var_names:
            attr = getattr(o, var_name)
            if not var_name.startswith("_"):
                result[var_name] = attr

        return result

    @classmethod
    def get_instance_variables(cls, t: Type) -> dict[str, Type]:
        '''
        Given a type (class), will return the arguments needed to construct it
        '''
        args = inspect.getfullargspec(t.__init__).annotations
        args.pop("return", None)
        return args
