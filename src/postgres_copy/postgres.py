from psycopg import Cursor, connect, Connection
from psycopg.rows import class_row, dict_row, DictRow
from typing import List, Any, Optional, Iterator
from pydantic import BaseModel

class SimpleTable:
    headers: List[str]
    records: List[Any]

    def __init__(self, headers, records):
        self.headers = headers
        self.records = records

class ItemList(BaseModel):
    list: List[BaseModel|None]

    def dicts(self, **kws) -> List[dict]:
        """Returns a dict list of ItemList"""
        return [model.dict(**kws) for model in self.list]

    def json(self, **kws) -> str:
        """Returns a json list of the ItemList"""
        from json import dumps
        return dumps(self.dicts(**kws))
    
    def csv(self, **kws):
        data = self.dicts(**kws)
        headers = data[0].keys()
        records = [value.values() for value in data]
        return SimpleTable(headers, records)

class Postgres:
    """Connection Wrapper for psycopg"""
    connection: Connection
    cursor: Optional[Cursor] = None

    def __init__(self, **kw):
        conn_string = ' '.join([f"""{k}={v}""" for k,v in kw.items()])
        self.connection = connect(conn_string)

    def sql_result(self, query='select version()', model=None, persist: bool=False) -> dict|BaseModel:
        if model:
            model = class_row(model)
        else:
            model = dict_row
        if persist:
            if not self.cursor:
                self.cursor = self.connection.cursor(row_factory=model)
            self.cursor.execute(query)
            return self.cursor.fetchone()
        else:
            with self.connection.cursor(row_factory=model) as curs:
                curs.execute(query)
                return curs.fetchone()
    
    def pagination(self, query='select version()', model=None, persist: bool=False):
        """Returns a list of results for the given query"""
        
        if model:
            model = class_row(model)
        else:
            model = dict_row()
        self.cursor = self.connection.cursor(row_factory=model)
        self.cursor.execute(query)
        return self.cursor

    def insert(self, table:str, 
        data: List[dict]|Iterator[dict]|ItemList|BaseModel|dict, 
        persist: bool = False,
        columns: Optional[List[str]]=None,
    ):
        csv = None
        from psycopg.sql import SQL, Identifier
        if isinstance(data, ItemList):
            csv = data.csv()
        elif isinstance(data, list):
            csv = SimpleTable(data[0].keys(), [dct.values() for dct in data])

        if csv:
            query = SQL("COPY {} ({}) FROM STDIN").format(
                Identifier(*table.split('.')),
                SQL(',').join([Identifier(header) for header in csv.headers])
            )
                
            
        elif isinstance(data, BaseModel):
            data = data.dict()
        if isinstance(data, dict):
            headers = data.keys()
            values = data.values()
            query = SQL("INSERT INTO {} ({}) VALUES {}").format(
                Identifier(*table.split('.')),
                SQL(',').join(Identifier(header) for header in headers),
                SQL('({})').format(
                    SQL(',').join(values)
                )
            )
        
        if query:
            if persist:
                if not self.cursor:
                    self.cursor = self.connection.cursor()
                if csv:
                    with self.cursor.copy(query) as copy:
                        for record in csv.records:
                            copy.write_row(record)
                else:
                    self.cursor.execute(query)
                self.connection.commit()
            else:
                with self.connection.cursor() as cursor:
                    if csv:
                        with cursor.copy(query) as copy:
                            for record in csv.records:
                                copy.write_row(record)
                    else:
                        cursor.execute(query)
                    self.connection.commit()


class MultiThread:
    """Connection Wrapper for psycopg"""
    connections: List[Postgres] = []
    workers: int = 1

    def __init__(self, workers: Optional[int] = None, **kw):
        from multiprocessing import cpu_count
        self.workers = workers or cpu_count()
        for worker in range(self.workers):
            self.connections.append(Postgres(**kw))

    def workerInsert(self, conn: Connection,
        table:str, 
        data: List[dict]|ItemList,
        batch_count=10000
    ):
        from math import floor, ceil
        if isinstance(data, list):
            count = len(data)
        elif isinstance(data, ItemList):
            count = len(data.list)
        i = 0
        while i < ceil(count/self.workers/batch_count):
            insert_count = int(batch_count if i < floor(count/self.workers/batch_count) else count/self.workers%batch_count)
            i+=1
            conn.insert('raas.users', data, persist=True)

    def insert(self, table:str,
        data: List[dict]|ItemList
    ):
        from multiprocessing import Process
        from math import ceil
        worker_pool = []
        for worker in range(self.workers):
            p = Process(target=self.workerInsert, 
                args=(
                    self.connections[worker], 
                    table, 
                    data[worker*ceil(len(data)/self.workers):ceil(len(data)/self.workers)*(worker+1)]
                )
            )
            p.start()
            worker_pool.append(p)
        for p in worker_pool:
            p.join()

    def close(self):
        for conn in self.connections:
            conn.connection.close()