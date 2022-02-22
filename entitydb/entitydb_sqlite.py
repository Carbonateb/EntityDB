import inspect
import os
import random
import sys
import sqlite3
from typing import Callable, Type
from entitydb.entitydb import EntityDB, PRIMARY_KEY, ENTITY_REFERENCE, ENTITY_TABLE
from entitydb.entity import Entity
from entitydb.system import SystemWrapper, SystemCommands


class EntityDB_SQLite(EntityDB):
    def __init__(self, db_file_name: str) -> None:
        super().__init__()
        self.db_file_name = db_file_name
        # Create database if it does not exist
        if not os.path.isfile(db_file_name):
            self._create_db()
    pass

    def add_entity(self, entity: Entity) -> int:
        new_id = random.randint(0, sys.maxsize)
        entity.uid = new_id
        entity.db = self

        con, cur = self._connect_to_db()

        component_uids = []

        # * Add each component
        for component in entity.get_components():
            component_name = type(component).__name__
            self._register_component_type(type(component))

            # Create a unique ID for this component
            component._uid = random.randint(0, sys.maxsize)
            component_uids.append(component._uid)
            columns = [PRIMARY_KEY, ENTITY_REFERENCE] + \
                list(EntityDB.get_variables_of(component).keys())
            cur.execute(f"INSERT INTO {component_name} ({','.join(columns)}) VALUES ({get_questionmarks(len(columns))})", [
                        component._uid] + [entity.uid] + list(EntityDB.get_variables_of(component).values()))
            con.commit()

        # * Add the entity

        # Get the columns we are inserting. This is the UID plus one for each component
        columns = [PRIMARY_KEY] + \
            [component_type.__name__ for component_type in entity.get_component_types()]

        cur.execute(f"INSERT INTO {ENTITY_TABLE} ({','.join(columns)}) VALUES ({get_questionmarks(len(columns))})", [
                    entity.uid] + component_uids)
        con.commit()
        con.close()
        return new_id

    def update_entity(self, entity: Entity) -> bool:
        con, cur = self._connect_to_db()
        for component in entity.get_components():
            component_name = type(component).__name__
            component_variables = EntityDB.get_variables_of(component)
            cvar_update_strings = []
            for varname in component_variables:
                cvar_update_strings.append(f"{varname} = ?")
            statement = f"UPDATE {component_name} SET {', '.join(cvar_update_strings)} WHERE {ENTITY_REFERENCE} = {entity.uid}"
            cur.execute(statement, list(component_variables.values()))
        con.commit()
        con.close()
        return True

    def run(self, system_func: Callable) -> None:
        system = self._parse_system(system_func)

        # A string like "Comp_A IS NOT NULL AND Comp_N IS NULL"
        where_clause = " AND ".join(
            # Include required components
            [f"{system.include_components[comp].__name__} IS NOT NULL" for comp in system.include_components] +

            # Excluded components
            [f"{comp.__name__} IS NULL" for comp in system.exclude_components]
        )

        con, cur = self._connect_to_db()
        # TODO Don't SELECT * (all), just select the components needed for this query
        cur.execute(f"SELECT * FROM {ENTITY_TABLE} WHERE ({where_clause})")
        found_entities: list[dict[str, str]] = cur.fetchall()
        con.close()
        entity_components: dict[int, dict[str, int]] = dict()
        for entity in found_entities:
            entity_components[entity.pop(PRIMARY_KEY)] = entity
        self._run_on_entities(system, entity_components)

    def count_matches(self, system_func: Callable) -> int:
        # TODO some code can be shared with EntityDB.run()

        system = self._parse_system(system_func)

        # A string like "Comp_A IS NOT NULL AND Comp_N IS NULL"
        where_clause = " AND ".join(
            # Include required components
            [f"{system.include_components[comp].__name__} IS NOT NULL" for comp in system.include_components] +

            # Excluded components
            [f"{comp.__name__} IS NULL" for comp in system.exclude_components]
        )

        con, cur = self._connect_to_db(False)
        cur.execute(
            f"SELECT COUNT(*) FROM {ENTITY_TABLE} WHERE ({where_clause})")
        result = cur.fetchone()[0]
        con.close()
        return result

    def load_component(self, entity: Entity, component_type: type) -> bool:
        self._register_component_type(component_type)
        component_name = component_type.__name__

        con, cur = self._connect_to_db()
        cur.execute(
            f"SELECT * FROM {component_name} WHERE {ENTITY_REFERENCE} = {entity.uid}")
        component_data: dict = cur.fetchone()
        con.close()

        if not component_data:
            return False

        new_component = self._create_component_from_data(
            component_type, component_data)

        entity._components[component_type] = new_component
        if component_name in entity._unloaded_components:  # Clean up
            entity._unloaded_components.remove(component_name)

        return True

    def _connect_to_db(self, fetch_as_dict=True) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        con = sqlite3.connect(self.db_file_name)
        if fetch_as_dict:
            con.row_factory = lambda c, r: {l[0]: r[i]
                                            for i, l in enumerate(c.description)}
        return (con, con.cursor())

    def _create_db(self):
        con, cur = self._connect_to_db()
        cur.execute(
            f"CREATE TABLE {ENTITY_TABLE} ({PRIMARY_KEY} INTEGER PRIMARY KEY)")
        con.commit()

    def _load_entity_from_cids(self, eid: int, components: dict[str, int]) -> Entity:
        '''
        Turns a row from the _entities table into an entity object.
        Only able to create components for ones that have been registered!
        '''
        result = Entity([])
        result.uid = eid
        con, cur = self._connect_to_db()
        for comp_name in components:
            if comp_name in self.component_classes:
                cur.execute(
                    f"SELECT * FROM {comp_name} WHERE {PRIMARY_KEY} == ?", [components[comp_name]])
                component_data: dict = cur.fetchone()

                if not component_data:
                    continue

                component_type = self.component_classes[comp_name]
                new_component = self._create_component_from_data(
                    component_type, component_data)
                result._components[component_type] = new_component
            else:
                result._unloaded_components.append(comp_name)
                #print("load entity: skipping unregistered component:", comp_name)

        con.close()
        return result

    def _setup_component_type(self, component: type) -> None:
        component_name = component.__name__
        con, cur = self._connect_to_db()
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {component_name} ({PRIMARY_KEY} INTEGER PRIMARY KEY, {ENTITY_REFERENCE} INTEGER)")
        variables = EntityDB.get_instance_variables(component)
        for var_name in variables:
            if not does_column_exist(cur, component_name, var_name):
                cur.execute(f"ALTER TABLE {component_name} ADD {var_name}")

        # Create its column in the entities table
        if not does_column_exist(cur, ENTITY_TABLE, component_name):
            cur.execute(
                f"ALTER TABLE {ENTITY_TABLE} ADD {component_name} INTEGER")

        con.commit()
        con.close()


def does_column_exist(cursor: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(
        f"SELECT COUNT(*) FROM pragma_table_info('{table_name}') WHERE name='{column_name}'")
    result = cursor.fetchone()
    if isinstance(result, tuple):
        return result[0] == 1
    elif isinstance(result, dict):
        return result["COUNT(*)"] == 1


def get_questionmarks(amount: int) -> str:
    '''
    Returns a string of question marks with commas, like
    "?,?,?,?" for amount = 4
    '''

    if amount < 1:
        return ""
    elif amount == 1:
        return "?"
    else:
        return "?" + str(",?" * (amount - 1))
