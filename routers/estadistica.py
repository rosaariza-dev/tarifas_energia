from fastapi import APIRouter
from config.env import settings
from fastapi.responses import JSONResponse
from config.logger import get_logger
from services.estadistica import Estadistica

logger = get_logger(__name__)

router = APIRouter(
    prefix="/estadisticas", tags=["estadisticas"]
)


@router.get("/")
async def get_estadisticas():
    try:
        est = Estadistica()
        costo_promedio = est.get_promedio_costo_total_kwh()
        total_operadores = est.get_total_operadores()
        fechas_actualizacion = est.fechas_actualizacion()
        return JSONResponse(
            status_code=200,
            content={
                "is_success": 200,
                "message": "Estadisticas consultadas exitosamete",
                "data": {
                    "promedio_costo_kwh": float(costo_promedio),
                    "total_operadores": total_operadores,
                    "ultima_fecha_dataset": fechas_actualizacion["ultima_actualizacion"],
                    "ultimo_periodo": fechas_actualizacion["periodo"]
                }
            }
        )
    except Exception as e:
        logger.error(f"Ocurrio un error al consultar las estadisticas: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "is_success": False,
                "error": "Ocurrio un error al consultar las estadisticas"
            }
        )
    

@router.get("/promedio")
def promedio():
    est = Estadistica()
    costo_promedio = est.get_promedio_costo_total_kwh()
    total_operadores = est.get_total_operadores()
    fechas_actualizacion = est.fechas_actualizacion()
    return fechas_actualizacion




