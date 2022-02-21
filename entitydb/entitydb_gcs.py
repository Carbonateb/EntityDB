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
        new_id = self._random_id()
        entity.uid = new_id
        entity.db = self
        
        component_uids = []
        
        # * Add each component
        for component in entity.get_components():
            component_name = type(component).__name__
            component._uid = self._random_id()
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
        
        return new_id

    def update_entity(self, entity: Entity) -> bool:
        return super().update_entity(entity)

    def delete_entity(self, entity: Entity) -> bool:
        return super().delete_entity(entity)

    def run(self, system_func: Callable) -> None:
        return super().run(system_func)

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
    
    def _random_id(self) -> str:
        '''Create a new random ID for use in components and entities'''
        return "".join(random.choice(string.ascii_letters + string.digits + string.ascii_letters.upper()) for i in range(UID_LENGTH))