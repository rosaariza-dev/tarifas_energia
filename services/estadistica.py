from database.postgresql import db
from config.logger import get_logger

logger = get_logger(__name__)

class Estadistica:
    
    def get_promedio_costo_total_kwh(self, p_anio: int | None = None, p_periodo: str | None = None, p_mes: int | None = None, p_region:str|None=None, p_operador:str|None=None, p_nivel:str|None=None):
        params = {
            "p_anio" : p_anio,
            "p_periodo": p_periodo,
            "p_mes":p_mes,
            "p_region":p_region,
            "p_operador": p_operador,
            "p_nivel": p_nivel
        }
        query= """
            SELECT fnc_tarifas_energia_promedio_costo_total_kwh(
                p_anio := %(p_anio)s,
                p_periodo := %(p_periodo)s,
                p_mes := %(p_mes)s,
                p_region := %(p_region)s,
                p_operador := %(p_operador)s,
                p_nivel := %(p_nivel)s
            )
        """
        promedio =  db.fetch_one(query, params=params)
        logger.info("Respuesta obtenida de procedimiento %s", promedio[0])
        return promedio[0]
    
    def get_total_operadores(self):
        query = """
            SELECT fnc_tarifas_energia_total_operadores()
        """
        total = db.fetch_one(query)
        return total[0]
        
    def fechas_actualizacion(self):
        query= """
            SELECT * from fnc_tarifas_energia_fechas_actualizacion()
        """
        fila = db.fetch_one(query)
        if fila:
            ultima_actualizacion, periodo = fila
            return {
                "ultima_actualizacion": ultima_actualizacion,
                "periodo": periodo
            }
        return None