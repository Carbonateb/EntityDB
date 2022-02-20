from entitydb import SystemCommands, component, EntityDB, Entity


@component
class MyComponent:
	my_int: int


@component
class Person:
	name:str


db = EntityDB("test.db")

# Init database with some basic entities

# db.add_entity(Entity([
# 	MyComponent(1),
# 	Person("Someone")
# ]))



# Example that gets entities that have a Person and MyComponent
def my_system(person:Person, my_component:MyComponent):
	print(f"Name = '{person.name}' and number = '{my_component.my_int}'")
#db.run(my_system)



# Example for entities that have MyComponent but not Person
def exclude_system(my_component:MyComponent, exclude=[Person]):
	print("Exclude persons system. value:", my_component.my_int)
# db.run(exclude_system)



# Example for editing an entity and saving it
def increment_system(my_component:MyComponent):
	my_component.my_int += 1
	return SystemCommands.SAVE_ENTITY
# db.run(increment_system)



# Example for seeing how many entities match a systems signature
num_matches = db.count_matches(increment_system)
print(f"'increment_system matches' {num_matches} entities")