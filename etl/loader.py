import pandas as pd
from config.logger import get_logger
from database.postgresql import db
import time

logger = get_logger(__name__)

class Loader:
    def load_database(self, df:pd.DataFrame)-> pd.DataFrame:
        logger.info("Iniciando cargue de informacion a base de datos")
        start_time = time.time()
        try:
            logger.info("Consultando columnas del dataframe %s", df.columns )
            columns = list(df.columns)
            columns_sql = ", ".join(columns)

            query = f"""
                COPY tarifas_energia ({columns_sql})
                FROM STDIN
            """
            with db.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute("CALL PROCE_TARIFAS_ENERGIA_DELETE();")
                    with cur.copy(query) as copy:
                        for row in df.itertuples(index=False, name=None):
                            copy.write_row(row)
        except Exception as e:
            logger.exception("Ocurrio un error al cargar la informacion a la base de datos")
            raise
        finally:
            duracion = time.time() - start_time
            logger.info("Tiempo de duracion del cargue de informacion en segundos : %s", duracion)
            
