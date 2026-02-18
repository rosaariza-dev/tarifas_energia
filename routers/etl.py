from fastapi import APIRouter
from config.env import settings
from etl.pipeline import Pipeline
from fastapi.responses import JSONResponse
from config.logger import get_logger
from services.estadistica import Estadistica

logger = get_logger(__name__)

router = APIRouter(
    prefix="/etl", tags=["etl"]
)


@router.get("/run")
async def run():
    try:
        pipeline = Pipeline()
        datos = await pipeline.run_etl()
        return JSONResponse(
            status_code=200,
            content={
                "is_success": True,
                "message": "ETL ejecutado exitosamente",
                "data": datos
            }
        )
    except Exception as e:
        logger.error(f"Ocurrio un error al ejecutar el etl: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "is_success": False,
                "error": "Ocurrio un error al ejecutar el etl"
            }
        )
    

@router.get("/promedio")
def promedio():
    est = Estadistica()
    costo_promedio = est.get_promedio_costo_total_kwh()
    total_operadores = est.get_total_operadores()
    fechas_actualizacion = est.fechas_actualizacion()
    return fechas_actualizacion




