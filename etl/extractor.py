import pandas as pd
from api.api import api_tarifas
from config.env import settings
from config.logger import get_logger
import time

logger = get_logger(__name__)

class Extractor:
    async def extract(self, params:dict | None = None):
        logger.info("Iniciando extracción de datos de api")
        start_time = time.time()
        try:
            response = await api_tarifas.get(f"/api/v3/views/{settings.ETL_DATASET_CODE}/query.json", params=params)
            logger.info("Datos consultados exitosamente")
            return response.json()
        except Exception as e:
            logger.exception("Ocurrio un error al extraer la informacion del etl")
            raise
        finally:
            duracion= time.time() - start_time
            logger.info("Tiempo de extraccion en segundos : %s", duracion)