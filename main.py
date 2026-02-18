from fastapi import FastAPI
from contextlib import asynccontextmanager
from database.postgresql import db
from config.env import settings
from config.logger import setup_logging, get_logger
from routers import etl, estadistica
from api.api import api_tarifas
from fastapi.middleware.cors import CORSMiddleware


setup_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Iniciando aplicación...")
    db.initialize(settings.DB_URI)
    
    yield 
    logger.info("🛑 Cerrando aplicación...")
    api_tarifas.close()
    db.close()
    logger.info("✅ Pool de conexiones cerrado")


# Crear app con lifespan
app = FastAPI(
    title="TarifasApi",
    version="1.0.0",
    lifespan=lifespan
)

# Agregar middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONT_URL],
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"], 
)

app.include_router(etl.router)
app.include_router(estadistica.router)

@app.get("/")
def read_root():
    return {"Hello": "World"}