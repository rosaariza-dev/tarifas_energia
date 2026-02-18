from etl.extractor import Extractor
from etl.transform import Transform
from etl.loader import Loader
import pandas as pd
from config.logger import get_logger
import time

logger = get_logger(__name__)

class Pipeline:

    async def run_etl(self):
        logger.info("Iniciando ejecución de etl..")
        start_time = time.time()
        try:
            # consultamos el dataset
            extractor = Extractor()
            data = await extractor.extract()
            
            # normalizar y limpiar datos
            transform = Transform()
            df = pd.DataFrame(data)
            df_new = transform.transform(df)

            #cargar la informacion a la base de datos
            loader = Loader()
            loader.load_database(df_new)

            # retornar los datos cargados a la base de datos
            logger.info("ETL ejecutado exitosamente")
            return df_new.to_dict(orient="records")
        except Exception as e:
            logger.exception("Ocurrio un error al ejecutar el etl")
            raise
        finally:
            duracion = time.time() - start_time
            logger.info("Tiempo de ejecucion del etl en segundos: %s", duracion)

       