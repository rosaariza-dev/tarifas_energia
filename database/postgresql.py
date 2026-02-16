import logging
from psycopg_pool import ConnectionPool
from contextlib import contextmanager
from typing import Optional, List, Tuple, Any


class Database:

    def __init__(self):
        self._pool = None

    def initialize(self, database_url: str, min_size: int = 2, max_size: int = 10):
        if self._pool is None:
            try:
                self._pool = ConnectionPool(
                    conninfo=database_url,
                    min_size=min_size,
                    max_size=max_size,
                    open=True
                )
                logger.info(f"✅ Conexión exitosa a la base de datos")
            except Exception as e:
                logger.error(f"❌ Error al conectar a la base de datos: {e}")
                raise SystemExit(f"No se pudo conectar a la base de datos: {e}")
    

    @property
    def pool(self):
        if self._pool is None:
            raise RuntimeError("Database no ha sido inicializado. Llama a db.initialize() primero.")
        return self._pool
    

    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    result = cur.fetchone()
                    logger.debug(f"fetch_one ejecutado - Resultado: {result is not None}")
                    return result
        except Exception as e:
            logger.error(f"Error en fetch_one: {e}")
            raise
    
    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    results = cur.fetchall()
                    logger.debug(f"fetch_all ejecutado - Filas: {len(results)}")
                    return results
        except Exception as e:
            logger.error(f"Error en fetch_all: {e}")
            raise
    
    def execute(self, query: str, params: Optional[Tuple] = None) -> int:
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rowcount = cur.rowcount
                    conn.commit()
                    logger.debug(f"execute completado - Filas afectadas: {rowcount}")
                    return rowcount
        except Exception as e:
            logger.error(f"Error en execute: {e}")
            raise
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, params_list)
                    rowcount = cur.rowcount
                    conn.commit()
                    logger.info(f"execute_many completado - Filas afectadas: {rowcount}")
                    return rowcount
        except Exception as e:
            logger.error(f"Error en execute_many: {e}")
            raise
    
    @contextmanager
    def transaction(self):
        conn = None
        try:
            conn = self.pool.getconn()
            logger.debug("Transacción iniciada")
            
            with conn.transaction():
                yield conn
            
            logger.debug("Transacción completada (commit)")
            
        except Exception as e:
            logger.warning(f"Transacción revertida (rollback): {e}")
            raise
        
        finally:
            if conn:
                self.pool.putconn(conn)
    
    
    def close(self):
        if self._pool:
            try:
                self._pool.close()
                logger.info("✅ Pool de conexiones cerrado")
            except Exception as e:
                logger.error(f"❌ Error al cerrar pool: {e}")
                raise

db = Database()