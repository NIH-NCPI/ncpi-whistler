#!/usr/bin/env python

""" 
    To speed ingestion up, we no longer write IDs to the db cache, but we still
    need a way to identify which IDs are associated with a given study in case
    we need to delete them. So, we'll capture them in the simplest way 
    possible and write them to a database once the ingestion is completed

    Key components of the database: 
    * host
    * study name
    * resourceType
    * id

    Each row will be a single ID that was PUT or POSTed to a given server. 

    
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from collections.abc import KeysView
from pathlib import Path
from threading import Lock
from typing import Any

class StudyIDs:
    def __init__(
        self,
        fhir_endpoint: str,
        study_id: str | None = None,
        chunk_size: int = 1000,
        threaded: bool = False,
        thread_count: int = 10,
    ) -> None:
        self.servername = fhir_endpoint
        self.study_id = study_id
        self.chunk_size = chunk_size

        # Working on the assumption that we only have one study/host
        # active at any given time, the in-memory cache is very simple
        # resourceType => [id1, id2, etc]
        self.ids: defaultdict[str, list[str]] = defaultdict(list)

        # I guess we can add the lock to the instance in case
        # we actually do want to maintain more than one study/host
        # at once
        self.lock = Lock()

        self.data: dict[str, Any] | None = None

    def add_id(self, resourceType: str, id: str) -> None:
        with self.lock:
            self.ids[resourceType].append(id)

    def load_from_file(self, filename: str | os.PathLike[str]) -> list[str]:
        if self.data is None:
            print(f"Loading IDs from file: {filename}")

            self.data = {}
            id_file = Path(filename)
            if id_file.exists():
                with id_file.open('rt') as f:
                    self.data = json.load(f)

        return [
            study
            for study in self.data.keys()
            if self.servername in self.data[study]
        ]

    def get_ids(self, study_id: str, resource: str) -> list[str]:
        assert self.data is not None
        return self.data[study_id][self.servername][resource]

    def list_resource_types(self, study_id: str) -> KeysView[str]:
        assert self.data is not None

        return self.data[study_id][self.servername].keys()

    def dump_to_file(self, filename: str | os.PathLike[str]) -> None:
        assert self.study_id is not None
        print(f"dumping IDs to file: {filename}")
        data: dict[str, Any] = {}
        id_file = Path(filename)
        if id_file.exists():
            with id_file.open('rt') as f:
                data = json.load(f)

        if self.study_id not in data:
            data[self.study_id] = {
                self.servername: {}
            }

        if self.servername not in data[self.study_id]:
            data[self.study_id][self.servername] = {}

        for resourceType in self.ids.keys():
            id_list = sorted(list(set(self.ids[resourceType])))
            data[self.study_id][self.servername][resourceType] = id_list 

        with id_file.open('wt') as f:
            json.dump(data, f, indent=2)

    def _dump_to_file(self, filename):
        def commit_to_db(cursor, data_chunk):
            if len(data_chunk) > 0:

                print(f"Committing {len(data_chunk)} to DB: #1=> {data_chunk[0]}")
                cursor.executemany("""
                        INSERT INTO 
                            db_ids(hostname, study_id, resourceType, id) 
                        VALUES(?,?,?,?)""", data_chunk)

            return []

        db = sqlite3.connect(filename, isolation_level=None, check_same_thread=False)
    
        cur = db.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS db_ids
                        (hostname TEXT NOT NULL,
                         study_id TEXT NOT NULL,
                         resourceType TEXT NOT NULL,
                         id TEXT NOT NULL,
                         unique(hostname, study_id, resourceType, id)); """)
        
        cur.execute("DELETE FROM db_ids WHERE hostname=? AND study_id=?", (
                        self.servername, self.study_id))

        stmts = []
        for resourceType in self.ids.keys():
            for id in self.ids[resourceType]:
                stmts.append((self.servername, self.study_id, resourceType, id))

                if len(stmts) >= self.chunk_size:
                    stmts = commit_to_db(cur, stmts)

            stmts = commit_to_db(cur, stmts)
            