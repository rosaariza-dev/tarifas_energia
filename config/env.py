from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_URI:str
    ETL_DATASET_CODE:str
    ETL_URL:str
    ETL_API:str
    APP_TOKEN:str
    FRONT_URL:str
    EMAIL_PASSWORD:str
    ACCOUNT_EMAIL:str

    class Config:
        env_file = ".env"

settings = Settings()