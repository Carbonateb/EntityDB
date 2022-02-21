import entitydb


class Entity():
    '''
    A collection of components
    '''

    def __init__(self, components: list[object]) -> None:
        self.uid: int
        self.db: entitydb.EntityDB

        self._components: dict[type, object] = {}
        for c in components:
            self._components[type(c)] = c

        self._unloaded_components: list[str] = []

    def get_component_types(self) -> list[type]:
        return list(self._components.keys()) + self._unloaded_components

    def get_components(self) -> list[object]:
        '''
        Gets a list of component objects.
        May not return every component this entity possibly has, only gets ones that are loaded!
        '''
        for component in self._unloaded_components:
            print(
                "Warning: get_components called while there is an unloaded component:", component)
        return list(self._components.values())

    def has_components(self, component_types: list[type]) -> bool:
        '''
        Returns true if this entity has all of the given components
        '''
        for t in component_types:
            if t not in self._components:
                # Attempt to load it if it's in the unloaded components

                return False
        return True

    def has_any_matching_components(self, component_types: list[type]) -> bool:
        '''
        Returns true if at least one of the components in the given list is present on this entity
        '''
        for t in component_types:
            if t in self._components:
                return True
        return False

    def get(self, component_type: type) -> object:
        '''
        Gets a component if it exists, returns None otherwise.
        '''
        component_name = component_type.__name__
        if component_name in self._unloaded_components:
            self.db.load_component(self, component_type)
        return self._components.get(component_type, None)
