import libs.config as config
import tkinter as tk
import botocore
import json
import tqdm
import uuid
import time
import os
import subprocess
import sys
from datetime import datetime
from IPython.core.magic import register_line_magic
from libs.ImpactosFunx import *


def main():
    # Accede a las variables desde config
    try:
        year = config.year
        data_dict = config.data_dict
        periodo = str(data_dict["periodo"])
        cartera = data_dict["cartera"]
        user = data_dict["user"]
        output_version = data_dict["output_version"]
        pesos = [float(peso) for peso in data_dict["pesos"]]
        umbrales = list(data_dict["umbrales"].values())
        fwl_table_v = data_dict["fwl_table_v"]

        # Acceder al diccionario desde el contexto principal
        print(" ")
        print("Resultados:")
        for key, value in data_dict.items():
            if key == "umbrales":
                print(f"{key}: {[umbral for umbral in data_dict[key].values() if umbral != None]}")
            else:
                print(f"{key}: {value}")
        print(" ")
        

        # Usa las variables
        try:
            pretty_json, params_sql = transform_txt_to_json(periodo, 
                                                cartera, 
                                                user, 
                                                output_version)
        except:
            print("No se encuentra el archivo .txt para la cartera y periodo indicados")
        
        print("Realizando ejecución oficial en athena. Espere....")
        response_result = ejecucion_oficial_athena(periodo,
                                                params_sql,
                                                cartera,
                                                output_version)

        eo_athena_download(response_result, 
                    periodo, 
                    cartera,
                    output_version)
        
        validacion_CODEJECUCION(periodo, cartera, output_version)

        path_s3_json = f"{carpet_prefix}/Calibracion-{year}/Impactos/Inputs_json/{periodo}" 

        upload_json_to_s3(s3_client, 
                        s3_bucket, 
                        path_s3_json, 
                        periodo, 
                        cartera, 
                        output_version)
        
        he_json = param_json_HE(user, 
                                cartera, 
                                periodo, 
                                output_version, 
                                pesos,
                                umbrales,
                                fwl_table_v)

        print("Se genero el archivo params")
    
        ## SUBA EL PARAMS A S3

        ## EJECUTAR NOTEBOOKS EN GLUE (CARTERA, PERIODO, VERSION)
        
    except:
        pass
if __name__ == "__main__":
    main()


