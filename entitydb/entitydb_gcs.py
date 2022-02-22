import random
import sys
import string
from typing import Callable, Type
from entitydb.entitydb import EntityDB, PRIMARY_KEY, ENTITY_REFERENCE, ENTITY_TABLE
from entitydb.entity import Entity
from entitydb.system import SystemWrapper, SystemCommands

from google.cloud import storage
from google.cloud.storage import Bucket, Blob
import google.api_core.exceptions as google_exceptions
from oauth2client.service_account import ServiceAccountCredentials


REGION = "AUSTRALIA-SOUTHEAST1"
BLOBNAME_DELIMITER = "/"
ENTITY_FOLDER = "ent"
COMPONENT_FOLDER = "cmp"
VALUES_FOLDER = "val"
UID_LENGTH = 16


class EntityDB_GCS(EntityDB):
    def __init__(self, bucket_name: str) -> None:
        self.storage_client = storage.Client()
        self.bucket = self._init_bucket(bucket_name)

        # Add a new object
        # blob = self.bucket.blob("empty_file")
        # blob.upload_from_string("")

        # Retrieve the object
        # entityid.
        # blobs = self.storage_client.list_blobs(self.bucket, prefix="nice", delimiter=BLOBNAME_DELIMITER)

        super().__init__()

    def add_entity(self, entity: Entity) -> int:
        new_eid = self._random_eid()
        entity.uid = new_eid
        entity.db = self
        
        component_uids = []
        
        # * Add each component
        for component in entity.get_components():
            component_name = type(component).__name__
            component._uid = self._random_cid()
            component_uids.append(component._uid)
            # Map of entities to components
            self._create_empty_blob(f"{ENTITY_FOLDER}/{entity.uid}/{component_name}-{component._uid}")
            # Map of components to entities
            self._create_empty_blob(f"{COMPONENT_FOLDER}/{component_name}/{entity.uid}-{component._uid}")
            
            # Actual data
            vars = EntityDB.get_variables_of(component)
            for varname in vars:
                new_blob = self.bucket.blob(f"{VALUES_FOLDER}/{component._uid}/{varname}")
                new_blob.upload_from_string(str(vars[varname]))
        
        return new_eid

    def update_entity(self, entity: Entity) -> bool:
        return super().update_entity(entity)

    def delete_entity(self, entity: Entity) -> None:
        if not entity.uid:
            raise Exception("Entity has not been saved yet")
        super().delete_entity(entity)

    def run(self, system_func: Callable) -> None:
        system = SystemWrapper(self, system_func)
        
        # * Find all components matching in the query
        
        # A set of entity IDs that match the query
        matched_eids:set = None
        
        # All found eids, and their dict of component names to cids (only components that exist in the query)
        all_entities:dict[str, dict[str, str]] = dict()
        
        for component in system.include_components:
            component_name = system.include_components[component].__name__
            blobs = self._search_blobs(f"{COMPONENT_FOLDER}/{component_name}/")
            found_components:set[str] = set()
            for b in blobs:
                blob:Blob = b
                eid, cid = blob.name.split(BLOBNAME_DELIMITER)[-1].split("-")
                found_components.add(eid)
                if eid in all_entities:
                    all_entities[eid][component_name] = cid
                else:
                    all_entities[eid] = {component_name: cid}
            
            if matched_eids:
                matched_eids = matched_eids & found_components
            else:
                matched_eids = found_components
        
        # TODO exlude components
        for component in system.exclude_components:
            pass
        
        # TODO optional components
        for component in system.optional_components:
            pass
        
        # * Load the components of the matched eids
        matched_entities: dict[str, dict[str, str]] = dict()
        
        for eid in matched_eids:
            matched_entities[eid] = all_entities[eid]
        
        self._run_on_entities(system, matched_entities)

    def count_matches(self, system_func: Callable) -> int:
        return super().count_matches(system_func)

    def _init_bucket(self, bucket_name: str):
        '''Ensures the bucket exists, creates it if it doesn't'''
        try:
            # Check if the bucket exists, will raise a NotFound exception
            return self.storage_client.get_bucket(bucket_name)
        except google_exceptions.NotFound:
            # Bucket does not exist, create it
            return self._create_bucket(bucket_name)

    def _create_bucket(self, bucket_name) -> Bucket:
        bucket = self.storage_client.bucket(bucket_name)
        new_bucket = self.storage_client.create_bucket(bucket, location=REGION)
        print(f"Bucket {new_bucket.name} created.")
        return new_bucket

    def _list_buckets(self) -> list[Bucket]:
        buckets = self.storage_client.list_buckets()
        result = []
        for bucket in buckets:
            result.append(bucket)
        return result
    
    def _create_empty_blob(self, name:str) -> Blob:
        return self.bucket.blob(name).upload_from_string("")
    
    def _random_id(self, length:int) -> str:
        '''Create a new random ID'''
        return "".join(random.choice(string.ascii_letters + string.digits + string.ascii_letters.upper()) for i in range(length))
    
    def _random_eid(self) -> str:
        '''Create a new random ID for use in entities'''
        return "E" + self._random_id(UID_LENGTH - 1)
    
    def _random_cid(self) -> str:
        '''Create a new random ID for use in components'''
        return "C" + self._random_id(UID_LENGTH - 1)
    
    def _search_blobs(self, prefix:str):
        '''
        Finds blobs that start with a specified prefix.
        
        Note: if searching in a folder, ensure there is a trailing `BLOBNAME_DELIMITER` (`/`)
        '''
        return self.storage_client.list_blobs(self.bucket, prefix=prefix, delimiter=BLOBNAME_DELIMITER)
    
    def _load_entity_from_cids(self, eid:str, components: dict[str, str]) -> Entity:
        result = Entity([])
        result.uid = eid
        for cid in list(components.values()):
            blobs:list[Blob] = self._search_blobs(f"{VALUES_FOLDER}/{cid}/")
            # Iterate over every property in the blob
            for blob in blobs:
                varname = blob.name.split("/")[-1]
                print(varname, blob.download_as_text())
            pass
        raise NotImplementedError()
        return result