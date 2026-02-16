from fastapi import FastAPI
from contextlib import asynccontextmanager
from database.postgresql import db
from config.env import settings
from config.logger import setup_logging, get_logger


setup_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Iniciando aplicación...")
    db.initialize(settings.DB_URI)
    logger.info("✅ Base de datos inicializada")
    
    yield 
    logger.info("🛑 Cerrando aplicación...")
    db.close()
    logger.info("✅ Pool de conexiones cerrado")


# Crear app con lifespan
app = FastAPI(
    title="TarifasApi",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
def read_root():
    return {"Hello": "World"}