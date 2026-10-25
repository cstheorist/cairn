"""Real-client differential tests for atomic distributed data modification."""
import os
import unittest
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
import psycopg

REFERENCE = "host=reference port=5432 user=cairn dbname=cairn password=local-postgres sslmode=disable"

class Mutations(unittest.TestCase):
    def setUp(self):
        self.table = "mutation_" + uuid.uuid4().hex[:10]
        self.connections = [psycopg.connect(dsn, autocommit=True, prepare_threshold=0)
                            for dsn in [os.environ["CAIRN_DSN"], REFERENCE]]
        for connection in self.connections:
            connection.execute(f"CREATE TABLE {self.table} (id BIGINT PRIMARY KEY, label TEXT NOT NULL, score INTEGER)")

    def tearDown(self):
        for connection in self.connections: connection.close()

    def same(self, query, parameters=None):
        results = []
        for connection in self.connections:
            cursor = connection.execute(query, parameters)
            results.append((sorted(cursor.fetchall(), key=repr) if cursor.description else [],
                            [(column.name,column.type_code) for column in cursor.description] if cursor.description else [],
                            cursor.rowcount, cursor.statusmessage))
        self.assertEqual(results[0], results[1], query)

    def state(self):
        self.same(f"SELECT * FROM {self.table} ORDER BY id")

    def seed(self):
        self.same(f"INSERT INTO {self.table} VALUES (0,'zero',10),(1,'one',20),(2,'two',NULL),(3,'three',-1) RETURNING *")

    def test_multirow_insert_expressions_and_returning(self):
        self.same(f"INSERT INTO {self.table} (label,id,score) VALUES ('zero',0,1+2),('one',1,4*5) RETURNING id, UPPER(label) AS label, score+1 AS score")
        self.same(f"INSERT INTO {self.table} (id,label) VALUES (%s,%s),(%s,%s) RETURNING *", (2,"two",3,"three"))
        self.state()

    def test_update_predicate_original_values_and_null(self):
        self.seed()
        self.same(f"UPDATE {self.table} AS a SET score=COALESCE(score,0)+1, label=label||'!' WHERE a.id >= %s RETURNING *", (1,))
        self.same(f"UPDATE {self.table} SET label=NULL WHERE id=-1 RETURNING *")
        self.same(f"UPDATE {self.table} SET label=%s WHERE id=%s RETURNING *", (None,-1))
        self.same(f"UPDATE {self.table} SET score=5 WHERE id=NULL RETURNING *")
        self.state()

    def test_cross_shard_key_move_and_delete(self):
        self.seed()
        self.same(f"UPDATE {self.table} SET id=9, score=score+1 WHERE id=0 RETURNING *")
        self.same(f"DELETE FROM {self.table} AS a WHERE a.score >= 10 RETURNING id,label,score")
        self.same(f"DELETE FROM {self.table} RETURNING *")
        self.state()

    def test_late_errors_roll_back_all_rows(self):
        self.seed()
        for query in [
            f"INSERT INTO {self.table} VALUES (10,'new',1),(1,'duplicate',2) RETURNING *",
            f"UPDATE {self.table} SET score=10/(3-id) RETURNING *",
            f"UPDATE {self.table} SET label=NULL WHERE id>=0 RETURNING *",
        ]:
            states = []
            for connection in self.connections:
                with self.assertRaises(psycopg.Error) as error: connection.execute(query)
                states.append(error.exception.sqlstate)
            self.assertEqual(states[0], states[1], query)
            self.state()
        self.same(f"SELECT COUNT(*) FROM {self.table} WHERE id=10")

    def test_explicit_transaction_rollback(self):
        self.seed()
        for connection in self.connections:
            connection.execute("BEGIN")
        self.same(f"UPDATE {self.table} SET score=score+100 RETURNING *")
        self.same(f"DELETE FROM {self.table} WHERE score IS NULL RETURNING *")
        self.same(f"INSERT INTO {self.table} VALUES (10,'ten',10),(11,'eleven',11) RETURNING *")
        self.state()
        for connection in self.connections:
            connection.execute("ROLLBACK")
        self.state()

    def test_typed_point_increments(self):
        table='increment_'+uuid.uuid4().hex[:8]
        for connection in self.connections:
            connection.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY,b BIGINT,i INTEGER,s SMALLINT,n BIGINT NOT NULL)')
            connection.execute(f'INSERT INTO {table} VALUES (0,0,0,0,1),(1,NULL,NULL,NULL,1)')
        for original,delta in [(2**63-2,1),(-2**63+1,-1),(2**63-1,-2**63),(-2**63,2**63-1),(2**53+11,7),(-2**53-11,-7)]:
            self.same(f'UPDATE {table} SET b=%s WHERE id=0',(original,))
            self.same(f'UPDATE {table} SET b=b+%s WHERE id=%s',(delta,0))
            self.same(f'SELECT * FROM {table} WHERE id=0')
        for query,parameters in [
            (f'UPDATE {table} SET b=b+%s WHERE id=%s',(None,0)),
            (f'UPDATE {table} SET b=b+%s WHERE id=%s',(5,1)),
            (f'UPDATE {table} SET n=n+%s WHERE id=%s',(None,999)),
            (f'UPDATE {table} SET n=n+%s WHERE id=%s',(1,None)),
            (f'UPDATE {table} SET n=n+%s WHERE id=0 AND n<0',(7,)),
            (f'UPDATE {table} SET n=n+%s WHERE id=0 RETURNING *',(3,)),
        ]:
            self.same(query,parameters)
        self.same(f'SELECT * FROM {table} ORDER BY id')
        for column,original,delta in [('b',2**63-1,1),('b',-2**63,-1),('b',-2**63,-2**63),('i',2**31-1,1),('s',32767,1)]:
            self.same(f'UPDATE {table} SET {column}=%s WHERE id=0',(original,))
            states=[]
            for connection in self.connections:
                with self.assertRaises(psycopg.Error) as error:
                    connection.execute(f'UPDATE {table} SET {column}={column}+%s WHERE id=0',(delta,))
                states.append(error.exception.sqlstate)
            self.assertEqual(states,['22003','22003'])
            self.same(f'SELECT * FROM {table} ORDER BY id')
        for connection in self.connections:
            with self.assertRaises(psycopg.errors.NotNullViolation):
                connection.execute(f'UPDATE {table} SET n=n+%s WHERE id=0',(None,))
            connection.execute('BEGIN')
        self.same(f'UPDATE {table} SET n=n+%s WHERE id=0',(7,))
        self.same(f'SELECT n FROM {table} WHERE id=0')
        for connection in self.connections: connection.execute('ROLLBACK')
        # A strict scan checks both rollback values and release of OPEN claims.
        self.same(f'SELECT * FROM {table} ORDER BY id')

    def test_contended_increment_has_no_lost_updates(self):
        table='counter_'+uuid.uuid4().hex[:8]
        self.connections[0].execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY,value BIGINT NOT NULL)')
        self.connections[0].execute(f'INSERT INTO {table} VALUES (0,0)')
        def worker(index):
            with psycopg.connect(os.environ['CAIRN_DSN'],autocommit=True,prepare_threshold=0) as connection:
                for _ in range(25):
                    deadline=time.monotonic()+10
                    while True:
                        try:
                            cursor=connection.execute(f'UPDATE {table} SET value=value+%s WHERE id=%s',(1,0))
                            self.assertEqual(cursor.rowcount,1)
                            break
                        except psycopg.errors.SerializationFailure:
                            if time.monotonic()>deadline: raise
                            time.sleep(.0005*(index+1))
        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(worker,range(8)))
        self.assertEqual(self.connections[0].execute(f'SELECT value FROM {table} WHERE id=0').fetchone(),(200,))

if __name__ == "__main__": unittest.main(verbosity=2)
