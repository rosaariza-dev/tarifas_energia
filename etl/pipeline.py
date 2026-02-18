from etl.extractor import Extractor
from etl.transform import Transform
from etl.loader import Loader
import pandas as pd
from config.logger import get_logger
import time
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config.env import settings

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
            duracion = round(time.time() - start_time, 2)

            logger.info("ETL ejecutado exitosamente en %s segundos", duracion)

            await self._enviar_correo(
                asunto="✅ ETL ejecutado exitosamente",
                cuerpo=f"""
                    El ETL se ejecutó correctamente.

                    📊 Registros procesados: {len(df_new)}
                    ⏱ Duración: {duracion} segundos
                    """)
           
            return df_new.to_dict(orient="records")
        except Exception as e:
            duracion = round(time.time() - start_time, 2)
            logger.exception("Ocurrió un error al ejecutar el ETL")
            await self._enviar_correo(
                    asunto="❌ Error al ejecutar ETL",
                    cuerpo=f"""
                        Ocurrió un error durante la ejecución del ETL.

                        ⏱ Duración antes del fallo: {duracion} segundos

                        Error:
                        {str(e)}

                        """)
            raise
        finally:
            duracion = time.time() - start_time
            logger.info("Tiempo de ejecucion del etl en segundos: %s", duracion)


    async def _enviar_correo(self, asunto: str, cuerpo: str):
        mensaje = MIMEMultipart()
        mensaje["From"] = settings.ACCOUNT_EMAIL
        mensaje["To"] = settings.ACCOUNT_EMAIL
        mensaje["Subject"] = asunto
        mensaje.attach(MIMEText(cuerpo, "plain"))

   
        try:
            await aiosmtplib.send(
                mensaje,
                hostname="smtp.gmail.com",
                port=587,
                start_tls=True,
                username=settings.ACCOUNT_EMAIL,
                password=settings.EMAIL_PASSWORD,
            )

            logger.info("Correo enviado correctamente")

        except Exception:
            logger.exception("Error enviando correo de notificación")


        