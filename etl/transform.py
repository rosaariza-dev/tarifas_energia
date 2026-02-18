from config.logger import get_logger
import pandas as pd
import time

logger = get_logger(__name__)

class Transform:

    def transform(self, df:pd.DataFrame)-> pd.DataFrame:
        logger.info("Iniciando transformacion de datos")
        start_time = time.time()
        try:
            logger.info("Consultando columnas del dataframe  %s", df.columns)

            # eliminando las columnas de creado por , actualizado por , version e id
            df_normalized = df.drop(columns=[":id", ":version", ":created_at"], errors="ignore")

            # cambiar nombres de columnas
            df_normalized = df_normalized.rename(columns={
                "a_o": "anio",
                "cu_total": "costo_total_kwh",
                "margen_comercializaci_n_cvm": "margen_comercializacion_cvm",
                "costo_g_t_p_rdidas_prn_m": "costo_g_t_perdidas_prn_m",
                "cot": "costo_operacion_transporte",
                "cfm_j_fact": "costo_fijo_mensual_j_fact",
                ":updated_at": "actualizacion_dataset_at"
            })

            #limpiar datos
            # consultar tipos de datos
            logger.info("Consultando tipo de datos del dataframe %s", df_normalized.dtypes )

            # eliminar espacios adicionales en valores string incluidos parentesis
            df_normalized = df_normalized.apply(
                lambda col: col.str.strip()
                            .str.replace(r"\s+", " ", regex=True)
                            .str.replace(r"\(\s*", "(", regex=True)   # quita espacio después de (
                            .str.replace(r"\s*\)", ")", regex=True)   # quita espacio antes de )
                            .str.upper()
            )

            # dividir operador de red de departamento y eliminar espacios extra
            df_normalized[["operador", "region"]] = (
                df_normalized["operador_de_red"]
                    .str.split(r"\s*-\s*", n=1, expand=True)
            )

            df_normalized["operador"] = df_normalized["operador"].str.strip().str.upper()
            df_normalized["region"] = df_normalized["region"].str.strip().str.upper()

            # normalizar nombre de operadores
            operadores = {
                "CELSIA": "CELSIA",
                "CELSIA COLOMBIA": "CELSIA",
                "ENEL BOGOTÁ": "ENEL",
                "ENEL": "ENEL"
            }

            df_normalized["operador"] = df_normalized["operador"].map(operadores).fillna(df_normalized["operador"])

            #eliminar columna de operador_red
            df_normalized = df_normalized.drop(columns=["operador_de_red"])

            # convertir columnas con valores numericos a tipo numerico
            colums_to_convert_numeric = [
                "anio",
                "costo_total_kwh", 
                "costo_compra_gm_i", 
                "cargo_transporte_stn_tm", 
                "cargo_transporte_sdl_dn_m", 
                "margen_comercializacion_cvm",
                "costo_g_t_perdidas_prn_m",
                "restricciones_rm",
                "costo_operacion_transporte",
                "costo_fijo_mensual_j_fact"
            ]

            df_normalized[colums_to_convert_numeric] = df_normalized[colums_to_convert_numeric].apply(pd.to_numeric, errors="coerce")

            logger.info("Consultando tipo de datos del dataframe %s", df_normalized.dtypes )

            logger.info("Consultando valores vacios por columnas \n %s", df_normalized.isna().sum())

            # convertir periodo/mes a numero
            meses = {
                "ENERO": 1,
                "FEBRERO": 2,
                "MARZO": 3,
                "ABRIL": 4,
                "MAYO": 5,
                "JUNIO": 6,
                "JULIO": 7,
                "AGOSTO": 8,
                "SEPTIEMBRE": 9,
                "OCTUBRE": 10,
                "NOVIEMBRE": 11,
                "DICIEMBRE":12
            }

            df_normalized["mes"] = df_normalized["periodo"].map(meses)

            logger.info("Consultando duplicados \n %s", df_normalized.duplicated().sum())

            logger.info("Datos del ETL transformador exitosamente")

            return df_normalized
        except Exception as e:
            logger.exception("Ocurrio un error al transformar los datos del etl")
            raise
        finally:
            duracion = time.time() -start_time
            logger.info("Tiempo de transformacion etl: %s", duracion)