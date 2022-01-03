""" 
Much of this is based on the KF teams ID caching used in the Ingest Library
https://github.com/kids-first/kf-lib-data-ingest/blob/master/kf_lib_data_ingest/etl/load/load_v1.py

The cache is an attempt to ensure that records that have already been loaded
into a given fhir server will be overwritten. To do that, we'll expect that
there is a reliably unique identifier to key on.

The test for pre-existing IDs will be done via an in-memory defaultdict 
(resourceType => unique_key => id) Persistance is enabled by use of a 
simple Sqlite3 database.

"""

import sqlite3
from collections import defaultdict

# Stealing this from the KF ingest library team! :)
from threading import Lock, current_thread, main_thread

count_lock = Lock()
cache_lock = Lock()

def FixTargetSystem(ts):
    return ts.split("//")[-1].replace("/", "_").replace("-", "_").replace(".", "_")
class IdCache:
    db_filename = ".dbcache.sqlite3"
    def __init__(self, study_id, fhir_endpoint):
        """
        :param study_id: Study ID associated with the current work
        :type study_id: string
        :param fhir_endpoint: Endpoint URL associated with the FHIR server
        :type fhir_endpoint: string

        The endpoint and study will be used in the db schema to allow us 
        to use a single database for persistance. 
        """
        self.study_id = study_id
        self.fhir_endpoint = fhir_endpoint
        self.cache = defaultdict(lambda: defaultdict(dict))
        self.db_cache = sqlite3.connect(IdCache.db_filename,
            isolation_level=None,
            check_same_thread=False)

    def prime_cache(self, target_system):
        if target_system not in self.cache:
            # Create table in DB first if necessary
            self.db_cache.execute(
                f"""CREATE TABLE IF NOT EXISTS "{FixTargetSystem(target_system)}"
                        (unique_id TEXT PRIMARY KEY, 
                        study_id TEXT NOT NULL, 
                        entity_type TEXT NOT NULL,
                        fhir_endpoint TEXT NOT NULL, 
                        target_id TEXT NOT NULL)""")

            # I'm currently on the fence about whether this is appropriate
            # The read is a onetime only (per resourceType/run) whereas, 
            # the write is very frequent for loads with new records. So, 
            # the index will slow down writes to benefit a small number of 
            # cases? 
            # self.db_cache.execute(f"CREATE INDEX idx-{entity_type} ON {entity_type}(study_id, fhir_endpoint)")

            # Populate RAM cache from DB
            for unique_id, entity_type, target_id in self.db_cache.execute(
                f"""SELECT 
                        unique_id, 
                        entity_type,
                        target_id 
                    FROM "{FixTargetSystem(target_system)}" 
                    WHERE study_id=? AND fhir_endpoint=?""", (self.study_id, self.fhir_endpoint)):
                self.cache[target_system][unique_id] = (entity_type, target_id)

    def get_id(self, target_system, entity_key):
        """
        Retrieve the target service ID for a given source unique key.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param entity_key: source unique key for this entity
        :type entity_key: str
        """
        with cache_lock:
            self.prime_cache(target_system)
            return self.cache[target_system].get(entity_key)

    def store_id(
        self, entity_type, target_system, entity_key, target_id, no_db=False
    ):
        """
        Cache the relationship between a source unique key and its corresponding
        target service ID.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param entity_key: source unique key for this entity
        :type entity_key: str
        :param target_id: target service ID for this entity
        :type target_id: str
        :param no_db: only store in the RAM cache, not in the db
        :type no_db: bool
        """
        with cache_lock:
            self.prime_cache(target_system)
            if self.cache[target_system].get(entity_key) != (entity_type, target_id):
                self.cache[target_system][entity_key] = (entity_type, target_id)
                if not no_db:
                    self.db_cache.execute(
                        f'INSERT OR REPLACE INTO "{FixTargetSystem(target_system)}"'
                        " (entity_type, unique_id, study_id, fhir_endpoint, target_id)"
                        " VALUES (?, ?,?,?,?);",
                        (entity_type, entity_key, self.study_id, self.fhir_endpoint, target_id),
                    )

