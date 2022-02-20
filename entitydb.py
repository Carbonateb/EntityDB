import os
import random
import sys
import enum
import inspect
import dataclasses
import sqlite3

# Components are actually python dataclasses
# This line is here for faster importing in other files
from dataclasses import dataclass as component
from typing import Callable, Type


# Constants
PRIMARY_KEY = "_uid"
ENTITY_TABLE = "_entities"
ENTITY_REFERENCE = "_entity"


class SystemCommands(enum.Enum):
	'''
	Return values from this enum in Systems
	to tell the EntityDB what to do
	'''
	
	# * Entity operations in group 100
	
	SAVE_ENTITY = 100 # Saves the current entity
	DELETE_ENTITY = 101 # Deletes the current entity
	
	# * Flow control in group 200
	
	BREAK = 200 # Stop running this system
	'''
	Works the same as the `break` keyword in loops
	'''



class Entity():
	'''
	A collection of components
	'''
	def __init__(self, components:list[object]) -> None:
		self.uid: int
		self.db: EntityDB
		
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
			print("Warning: get_components called while there is an unloaded component:", component)
		return list(self._components.values())
	
	
	def has_components(self, component_types:list[type]) -> bool:
		'''
		Returns true if this entity has all of the given components
		'''
		for t in component_types:
			if t not in self._components:
				# Attempt to load it if it's in the unloaded components
				
				return False
		return True
		
		
	def has_any_matching_components(self, component_types:list[type]) -> bool:
		'''
		Returns true if at least one of the components in the given list is present on this entity
		'''
		for t in component_types:
			if t in self._components:
				return True
		return False
	
	
	def get(self, component_type:type) -> object:
		'''
		Gets a component if it exists, returns None otherwise.
		'''
		component_name = component_type.__name__
		if component_name in self._unloaded_components:
			self.db.load_component(self, component_type)
		return self._components.get(component_type, None)
			


class SystemWrapper():
	def __init__(self, edb:'EntityDB', system:Callable) -> None:
		# Which components we are searching for
		components:dict[str, type] = {}
		optional_components:dict[str, type] = {}
		exclude_components:list[type] = []
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
								
				elif annotations[arg] is EntityDB:
					if edb_input:
						raise Exception("Can't have multiple of the same type!")
					edb_input = edb
						
				elif annotations[arg] is Entity:
					if entity_input:
						raise Exception("Can't have multiple of the same type!")
					entity_input = arg
				
				elif annotations[arg] is int:
					if index_input:
						raise Exception("Can't have multiple of the same type!")
					index_input = arg
							
			else:
				if arg == "exclude":
					exclude_components = spec.defaults[0]
		
		self.edb:'EntityDB' = edb
		self.system:Callable = system
		self.include_components = components
		self.optional_components = optional_components
		self.exclude_components = exclude_components
		self.entity_input = entity_input
		self.edb_input = edb_input
		self.index_input = index_input
	
	
	def run(self, entity:Entity, index:int) -> list[SystemCommands]:
		'''
		Runs the system using fields from the object. Assumes that the entity has
		all of the correct fields for this function signature.
		
		Will throw an exception if the entity passed in does not match!
		'''
		args:dict[str, object] = {}
		
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
		result:list[SystemCommands] = []
		
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
		return list(self.include_components.values()) + list(self.optional_components.values()) # + self.exclude_components



class EntityDB():
	'''
	Equivalent to the "world" in ECS terminology.
	Stores entities, can be queried.
	Backed by an SQLite database.
	
	TODO Allow changing which backend is used; Google Cloud Storage?
	'''
	
	def __init__(self, db_file_name:str) -> None:
		self.db_file_name = db_file_name
		
		# Keeps track of the component classes we have registered
		self.component_classes:dict[str, type] = dict()
		
		# Create database if it does not exist
		if not os.path.isfile(db_file_name):
			self._create_db()
	
	
	def add_entity(self, entity:Entity) -> int:
		'''
		Adds an entity, returns its ID
		'''
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
			columns = [PRIMARY_KEY, ENTITY_REFERENCE] + list(get_variables_of(component).keys())
			cur.execute(f"INSERT INTO {component_name} ({','.join(columns)}) VALUES ({get_questionmarks(len(columns))})", [component._uid] + [entity.uid] + list(get_variables_of(component).values()))
			con.commit()
		
		# * Add the entity	
		
		# Get the columns we are inserting. This is the UID plus one for each component
		columns = [PRIMARY_KEY] + [component_type.__name__ for component_type in entity.get_component_types()]
		
		cur.execute(f"INSERT INTO {ENTITY_TABLE} ({','.join(columns)}) VALUES ({get_questionmarks(len(columns))})", [entity.uid] + component_uids)
		con.commit()
		con.close()
		return new_id
	
	
	def new_entity(self, components:list[object]) -> int:
		'''
		Creates a new entity from a list of components.
		Adds it, then returns its ID.
		'''
		return self.add_entity(Entity(components))
	
	
	def update_entity(self, entity:Entity) -> bool:
		'''
		Updates the components of an entity that has already been
		saved to the database.
		'''
		con, cur = self._connect_to_db()
		for component in entity.get_components():
			component_name = type(component).__name__
			component_variables = get_variables_of(component)
			cvar_update_strings = []
			for varname in component_variables:
				cvar_update_strings.append(f"{varname} = ?")
			statement = f"UPDATE {component_name} SET {', '.join(cvar_update_strings)} WHERE {ENTITY_REFERENCE} = {entity.uid}"
			cur.execute(statement, list(component_variables.values()))
		con.commit()
		con.close()
		return True
		
	
	def delete_entity(self, entity:Entity) -> bool:
		return False # TODO implement deleting entities!
	
	
	def run(self, system_func:Callable) -> None:
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
		system = SystemWrapper(self, system_func)
		
		# Might be some components in the call signature we haven't seen yet
		for component_type in system.get_components_from_signature():
			self._register_component_type(component_type)
		
		# A string like "Comp_A IS NOT NULL AND Comp_N IS NULL"
		where_clause = " AND ".join(
			# Include required components
			[f"{system.include_components[comp].__name__} IS NOT NULL" for comp in system.include_components] +
			
			# Excluded components
			[f"{comp.__name__} IS NULL" for comp in system.exclude_components]
		)
		
		con, cur = self._connect_to_db()
		cur.execute(f"SELECT * FROM {ENTITY_TABLE} WHERE ({where_clause})")
		found_entities = cur.fetchall()
		con.close()
		index = 0
		for entity_ids in found_entities:
			# TODO optimization: read below
			# Don't bother trying to load every component from DB this entity has,
			# just load the ones that this function calls for
			entity:Entity = self._load_entity_from_ids(entity_ids)
			commands = system.run(entity, index)
			
			# * Run the commands
			
			if SystemCommands.DELETE_ENTITY in commands:
				self.delete_entity(entity)
			
			elif SystemCommands.SAVE_ENTITY in commands:
				self.update_entity(entity)
			
			if SystemCommands.BREAK in commands:
				break
			
			index += 1
	
	
	def count_matches(self, system_func:Callable) -> int:
		'''
		Counts how many entities match the system's signature.
		If you were to run the system, this is how many times it will
		be called.
		
		TODO some code can be shared with EntityDB.run()
		'''
		system = SystemWrapper(self, system_func)
		
		# Might be some components in the call signature we haven't seen yet
		for component_type in system.get_components_from_signature():
			self._register_component_type(component_type)
		
		# A string like "Comp_A IS NOT NULL AND Comp_N IS NULL"
		where_clause = " AND ".join(
			# Include required components
			[f"{system.include_components[comp].__name__} IS NOT NULL" for comp in system.include_components] +
			
			# Excluded components
			[f"{comp.__name__} IS NULL" for comp in system.exclude_components]
		)
		
		con, cur = self._connect_to_db(False)
		cur.execute(f"SELECT COUNT(*) FROM {ENTITY_TABLE} WHERE ({where_clause})")
		result = cur.fetchone()[0]
		con.close()
		return result
	
	
	
	def load_component(self, entity:Entity, component_type:type) -> bool:
		'''
		Loads a given component onto an entity, by reading its values from the db.
		If the component already exists on the entity, it is overwritten.
		
		If this returns True, makes some changes to the given entity:
		- Loads a new component on `Entity._components`
		- Removes the component from `Entity._unloaded_components`, if it was there.
		'''
		self._register_component_type(component_type)
		component_name = component_type.__name__
		
		con, cur = self._connect_to_db()
		cur.execute(f"SELECT * FROM {component_name} WHERE {ENTITY_REFERENCE} = {entity.uid}")
		component_data:dict = cur.fetchone()
		con.close()
		
		if not component_data:
			return False
		
		new_component = self._create_component_from_data(component_type, component_data)
		
		entity._components[component_type] = new_component
		if component_name in entity._unloaded_components: # Clean up
			entity._unloaded_components.remove(component_name)
		
		return True
	
	
	def _connect_to_db(self, fetch_as_dict=True) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
		con = sqlite3.connect(self.db_file_name)
		if fetch_as_dict:
			con.row_factory = lambda c, r: {l[0]: r[i] for i, l in enumerate(c.description)}
		return (con, con.cursor())
	
	
	def _create_db(self):
		con, cur = self._connect_to_db()
		cur.execute(f"CREATE TABLE {ENTITY_TABLE} ({PRIMARY_KEY} INTEGER PRIMARY KEY)")
		con.commit()
	
	
	def _load_entity_from_ids(self, ids:dict) -> Entity:
		'''
		Turns a row from the _entities table into an entity object.
		Only able to create components for ones that have been registered!
		
		TODO allow specifiying which components to not load, to save performance.
		'''
		result = Entity([])
		con, cur = self._connect_to_db()
		ids.pop(PRIMARY_KEY)
		for comp_name in ids:
			if comp_name in self.component_classes:
				cur.execute(f"SELECT * FROM {comp_name} WHERE {PRIMARY_KEY} == ?", [ids[comp_name]])
				component_data:dict = cur.fetchone()
				
				if not component_data:
					continue
						
				component_type = self.component_classes[comp_name]
				new_component = self._create_component_from_data(component_type, component_data)
				result._components[component_type] = new_component
				result.uid = component_data[ENTITY_REFERENCE]
			else:
				result._unloaded_components.append(comp_name)
				#print("load entity: skipping unregistered component:", comp_name)
				
		con.close()
		return result
	
	
	def _create_component_from_data(self, component_type:type, component_data:dict) -> object:
		# Just get the actual values, strip the extra stuff
		component_values:dict = {}
		for varname in component_data:
			# Exclude vars that are in the table but not passed into the constructor
			# This should just be the primary key and entity reference
			if varname not in [PRIMARY_KEY, ENTITY_REFERENCE]:
				component_values[varname] = component_data[varname]
		return component_type(**component_values)
	
	
	def _register_component_type(self, component:type) -> bool:
		'''
		Checks if this component has been seen before, if not, it
		registers it and creates a table for it (adding any new columns if neccessary).
		
		Returns False if the component has already been registered
		'''
		component_name = component.__name__
		if component_name not in self.component_classes:
			# Register it, and setup its table
			self.component_classes[component_name] = component
			
			con, cur = self._connect_to_db()
			cur.execute(f"CREATE TABLE IF NOT EXISTS {component_name} ({PRIMARY_KEY} INTEGER PRIMARY KEY, {ENTITY_REFERENCE} INTEGER)")
			variables = get_instance_variables(component)
			for var_name in variables:
				if not does_column_exist(cur, component_name, var_name):
					cur.execute(f"ALTER TABLE {component_name} ADD {var_name}")
			
			# Create its column in the entities table
			if not does_column_exist(cur, ENTITY_TABLE, component_name):
				cur.execute(f"ALTER TABLE {ENTITY_TABLE} ADD {component_name} INTEGER")
			
			con.commit()
			con.close()
			return True
		return False




def does_column_exist(cursor:sqlite3.Cursor, table_name:str, column_name:str) -> bool:
	cursor.execute(f"SELECT COUNT(*) FROM pragma_table_info('{table_name}') WHERE name='{column_name}'")
	result = cursor.fetchone()
	if isinstance(result, tuple):
		return result[0] == 1
	elif isinstance(result, dict):
		return result["COUNT(*)"] == 1


def get_questionmarks(amount:int) -> str:
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


def get_variables_of(o:object) -> dict[str, object]:
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

def get_instance_variables(t:Type) -> dict[str, Type]:
	'''
	Given a type (class), will return the arguments needed to construct it
	'''
	args = inspect.getfullargspec(t.__init__).annotations
	args.pop("return", None)
	return args