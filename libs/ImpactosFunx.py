import re
import copy
import botocore
import json
import tqdm
import uuid
from datetime import datetime
import pandas as pd
import time
import sys
import os
import libs.config as config
from libs.visualization import *

sys.stdout.reconfigure(encoding='utf-8')


s3_bucket = config.s3_bucket 
carpet_prefix = config.carpet_prefix
year = config.year
athena_client = config.athena_client
s3_client = config.s3_client
glue_client = config.glue_client

try:
    current_dir = os.path.abspath(os.path.dirname(__file__))
except NameError:
    # Si __file__ no está definido, usar el directorio de trabajo actual
    current_dir = os.path.abspath(os.getcwd())

def cal_year(year):
    return f"Calibracion-{year}"


def s3_download_file(s3_client, s3_bucket, carpet_prefix, local_key, array_files ):
    track_download = {"file": [], "downloaded": []}
    for file_name in array_files:
        s3_key = f'{carpet_prefix}/{file_name}'
        track_download["file"].append(file_name)  # Utiliza append para agregar a la lista
        try:
            s3_client.download_file(s3_bucket, s3_key, f"{local_key}/{file_name}")
            #print(f'El cuaderno {file_name} se ha descargado exitosamente.')
            track_download["downloaded"].append(True)  # Utiliza append para agregar a la lista
        except botocore.exceptions.ClientError as e:
            track_download["downloaded"].append(False)  # Utiliza append para agregar a la lista
            #print(f'Error: {e}')
    return track_download

def eo_athena_download(response_result, periodo, cartera, output_version):
    athena_to_s3 = f"{carpet_prefix}/Calibracion-2024/Impactos/Notebooks/{periodo}/{cartera.upper()}/output_v{output_version}"
    local_athena_results = f'results/{periodo}/{cartera}/output_v{output_version}'
    response_name = [f'E_oficial_{cartera}_{periodo}_v{output_version}.csv']

    if not os.path.exists(local_athena_results):
        os.makedirs(local_athena_results)
        
    track_result_athena = None
    start_time = time.time()

    while not track_result_athena or track_result_athena["downloaded"] == [False]:

        try:
            copy_source = {
                'Bucket': s3_bucket,
                'Key': f'{athena_to_s3}/{response_result}.csv'
                            }

            s3_client.copy(copy_source, Bucket=s3_bucket, Key= f"{athena_to_s3}/{response_name[0]}")

            lisr_response = s3_client.list_objects_v2(Bucket = s3_bucket, 
                                                    Prefix = athena_to_s3)

            del_files = [lisr_response["Contents"][i]["Key"] for i in range(len(lisr_response["Contents"]))
                        if lisr_response["Contents"][i]["Key"].endswith((".txt", ".csv.metadata", f"{response_result}.csv"))]

            response = [s3_client.delete_object(
                                            Bucket = copy_source["Bucket"],
                                            Key = key) for key in del_files] 
        except:
            continue


        track_result_athena = s3_download_file(s3_client, 
                                                s3_bucket, 
                                                athena_to_s3, 
                                                local_athena_results, 
                                                response_name)
        
        if track_result_athena["downloaded"] == [True]:  # Verifica si es verdadero
            sys.stdout.write(f"Ejecucion oficial: {response_name[0]} descargado en: {local_athena_results}\n")
            sys.stdout.flush()
            break  # Termina el bucle si el archivo se descargó

        else:
            elapsed_time = time.time() - start_time
            sys.stdout.write("\rAthena sigue ejecutando, Tiempo transcurrido: {:.2f} segundos".format(elapsed_time))
            sys.stdout.flush()
            time.sleep(0.1)
            continue

    return

def transform_txt_to_json(periodo, cartera , user, output_version ):
    json_prefix = f"{carpet_prefix}/{cal_year(year)}/Impactos"
    INPUT_JSON = "raw_txt"
    # Crear el directorio de salida si no existe
    OUTPUT_JSON = f"json_s3/{periodo}/{cartera}"
    if not os.path.exists(OUTPUT_JSON):
        os.makedirs(OUTPUT_JSON)

    # Lista de archivos en el directorio
    file_json = os.listdir(INPUT_JSON+"/")

    # Diccionario con las regex para cada cartera
    carteras_regex = {
        "tarjetas": "tarjet(as?|a|ah|s|e)?", 
        "rappi": "rappi|rap(p)?i|rrapi",
        "consumo": "cons(u|o)mo(s)?|cunsumo",
        "bpe": "bpe|bepe|bpee",
        "vehicular": "veh(i|í)cular(es)?|vehicul(ar|ares)",
        "convenios": "conv(e|é)nio(s)?|convenioz",
        "empresas": "empres(a|as|az|s|e)?|empr(e|é)sa",
        "hipotecas": "hipotec(a|as|az|os)?|hipote(c|k)as",
        "corporativa": "corpor(ativa|ativo|atiba|tiva|tivo)s?",
        "institucional": "institucion(a|al|ales|als|l)?|insti(tucional|tutional)",
        "inmobiliarias": "inmobi(l|li)aria(s|z)?|inmobil(iaria|iarias)",
    }

    # Regex que busca la presencia del período y la cartera con variaciones
    regex_cartera = carteras_regex[cartera]
    pattern = f".*({regex_cartera}).*({periodo}).*\\.txt$"

    # Filtrar archivos usando regex
    file_txt = [file for file in file_json if re.search(pattern, file, re.IGNORECASE)]
    if file_txt:
        file_txt = file_txt[0]  # Tomar el primer archivo que coincida
        print("Archivos txt:", file_txt)
    else:
        print("No se encontró ningún archivo que coincida con los criterios.")

    json_file_path = os.path.join(current_dir, '..', INPUT_JSON, file_txt)
    json_file_path = os.path.abspath(json_file_path)

    with open(json_file_path) as f:
        lines = json.loads(f.read())

        with open(f'{OUTPUT_JSON}/JSON_{cartera.upper()}_{periodo}_v{output_version}.json', 'w') as fr:
            fr.write(lines)  

        with open(f'{OUTPUT_JSON}/JSON_{cartera.upper()}_{periodo}_v{output_version}.json', 'r') as fr:

            pretty_json = json.load(fr)

            pretty_json['confPython']['folderS3'] = f"s3://{s3_bucket}/{json_prefix}/{user}/athena_results/"

            pretty_json['confPython']['folderS3Destino'] = f"s3://{s3_bucket}/{json_prefix}/{user}/resultados_he/{periodo}/carteras/{cartera}/output_v{output_version}/"

        with open(f'{OUTPUT_JSON}/JSON_{cartera.upper()}_{periodo}_v{output_version}.json', 'w') as fr:
            json.dump(pretty_json, fr, indent=4) 
        
        print(F"TANFORMASCIÓN DE TXT A JSON COMPLETA PARA: JSON_{cartera}_{periodo}_v{output_version}.json")

        with open(f'{OUTPUT_JSON}/JSON_{cartera.upper()}_{periodo}_v{output_version}.json', 'r') as fr:
            pretty_json = json.load(fr)

    with open('libs/params/ejecucion_oficial_parametros.sql', 'r') as archivo_sql:
        sql_script = archivo_sql.read()

    CODBASE = str(pretty_json["escenarios"][0]["n_cod_ejecucion"])
    CODPESIMISTA = str(pretty_json["escenarios"][1]["n_cod_ejecucion"])
    CODOPTIMISTA = str(pretty_json["escenarios"][2]["n_cod_ejecucion"])

    peso_base = str(pretty_json["escenarios"][0]["n_peso"])
    peso_pesimista = str(pretty_json["escenarios"][1]["n_peso"])
    peso_optimista = str(pretty_json["escenarios"][2]["n_peso"])

    sql_script = sql_script.replace('CODBASE', CODBASE)
    sql_script = sql_script.replace('CODPESIMISTA', CODPESIMISTA)
    sql_script = sql_script.replace('CODOPTIMISTA', CODOPTIMISTA)

    sql_script = sql_script.replace('peso_base', peso_base)
    sql_script = sql_script.replace('peso_pesimista', peso_pesimista)
    sql_script = sql_script.replace('peso_optimista', peso_optimista)

    # sql_output = f'sql_files/{periodo}/{cartera}'

    # if not os.path.exists(sql_output):
    #     os.makedirs(sql_output)

    # with open(f'{sql_output}/eo_params_{cartera}_{periodo}_v{output_version}.sql', 'w') as archivo_sql:
    #     archivo_sql.write(sql_script)

    # print('')
    # print("Linea 11", pretty_json['confPython']['folderS3'])
    # print("Linea 13", pretty_json['confPython']['folderS3Destino'])

    print(" ")
    print("Codigo de ejecución base:",CODBASE)
    print("Codigo de ejecución pesimista:",CODPESIMISTA)
    print("Codigo de ejecución optimista:",CODOPTIMISTA)

    return pretty_json, sql_script

def start_query_execution(client, query, output_path):
    response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": output_path}
        )
    return response["QueryExecutionId"]

def ejecucion_oficial_athena(periodo, sql_script,  cartera, output_version):

    
    
    athena_to_s3 = f"{carpet_prefix}/Calibracion-2024/Impactos/Notebooks/{periodo}/{cartera.upper()}/output_v{output_version}"
    OutputLocation = f's3://{s3_bucket}/{athena_to_s3}/'

    with open("libs/params/ejecucion_oficial_base.sql", "r") as archivo:
            base_sql = archivo.read()

    # with open(f"sql_files/{periodo}/{cartera}/eo_params_{cartera}_{periodo}_v{output_version}.sql", "r") as archivo:
    #         parametros_sql = archivo.read()

    response_base = start_query_execution(athena_client, base_sql, OutputLocation)
    response_result = start_query_execution(athena_client, sql_script, OutputLocation)


    return response_result


def upload_json_to_s3(s3_client, s3_bucket, path_s3_json, periodo, cartera,output_version ):
    
    OUTPUT_JSON = f"json_s3/{periodo}/{cartera}"

    json_name = f"{path_s3_json}/JSON_{cartera.upper()}_{periodo}_v{output_version}.json"
    path_local = f"{OUTPUT_JSON}/JSON_{cartera}_{periodo}_v{output_version}.json"

    s3_client.upload_file(path_local, 
                      s3_bucket, 
                      json_name)
    
    print(f"Se cargo en s3 el json: JSON_{cartera.upper()}_{periodo}_v{output_version}.json")
    
    return 


def validacion_CODEJECUCION(periodo, cartera, output_version):

    df = pd.read_csv(f"results/{periodo}/{cartera}/output_v{output_version}/E_oficial_{cartera}_{periodo}_v{output_version}.csv", sep=",")

    if (df["VCHCARTERA"].str.lower() == cartera.lower()).any() and df["NUMFECHAREFERENCIA"].unique()[0]== int(periodo):

        print(f"{bcolors.OKGREEN} VALIDACION APROBADA{bcolors.ENDC}\n")
        print(f"Los CODEJECUCIÓN de la ejecución oficial coinciden con la cartera: '{cartera}' y periodo: '{periodo}' definidos para el json\n")
       
    else:
        
        print(f"{bcolors.BOLD_RED}VALIDACIÓN NO APROBADA{bcolors.ENDC}\n")
        print(f"Los CODEJECUCIÓN de la ejecución oficial no coinciden con la cartera: '{cartera}' y periodo: '{periodo}' definidos para el json\n")
        

def param_json_HE(user, cartera, periodo, output_version, pesos, umbrales, fwl_table_v):
    path_json_he = 'libs/params/params.json'

    with open(f'{path_json_he}') as f:
          json_he_orginal = json.loads(f.read())
    
    json_he = copy.deepcopy(json_he_orginal)

    json_he["params_variables"]["registro"] = user
    json_he["params_variables"]["registro_ejecucion"] = user
    json_he["params_variables"]["cartera"] = cartera
    json_he["params_variables"]["periodo"] = periodo
    json_he["params_variables"]["cartera"] = cartera
    json_he["params_variables"]["version"] = output_version
    json_he["params_variables"]["fwl_table_v"] = fwl_table_v
    json_he["params_variables"]["pesos"]["n_peso_b"] = pesos[0]
    json_he["params_variables"]["pesos"]["n_peso_o"] = pesos[1]
    json_he["params_variables"]["pesos"]["n_peso_p"] = pesos[2]
    json_he["rutas"]["carpet_prefix"]= f"ifrs/discovery/discovery_riesgos_calibracion/{cal_year(year)}/Impactos"

    number_of_thresholds = [number for number, list_portfolio in json_he["threshold_per_portfolio"].items() 
                                    if cartera in list_portfolio][0]
    
    if number_of_thresholds == "4":
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)]["Umbral_1"] = umbrales[0]
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)]["Umbral_2"] = umbrales[1]
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)]["Umbral_3"] = umbrales[2]
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)]["Umbral_4"] = umbrales[3]

    elif number_of_thresholds == "2":
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)]["Umbral_1"] = umbrales[0]
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)]["Umbral_2"] = umbrales[1]

    elif number_of_thresholds == "0":
        json_he["params_variables"]["umbrales"][str(number_of_thresholds)] = {}

    tabla = [key for key in list(json_he['params_variables']["tbl_names_per_portfolio"].keys()) 
            if key in json_he['file_group'][cartera]]
    
    #print(tabla)
    if fwl_table_v != "0":
        json_he['params_variables']["tbl_names_per_portfolio"][tabla[0]]['tbl_stress_pd'][1] = 1

    OUTPUT_JSON = f'results/{periodo}/{cartera}/output_v{output_version}'
    if not os.path.exists(OUTPUT_JSON):
        os.makedirs(OUTPUT_JSON)

    with open(f'{OUTPUT_JSON}/params_{cartera}_{periodo}_v{output_version}.json', 'w') as fr:
        json.dump(json_he, fr, indent=4)

    with open(path_json_he, 'w') as fr:
        json.dump(json_he_orginal, fr, indent=4)

    return json_he


def dict_arguments(periodo, cartera, output_version):
    OUTPUT_JSON = f'params_json/{periodo}/{cartera}'
    with open(f'{OUTPUT_JSON}/params_{cartera}_{periodo}_v{output_version}.json') as f:
            json_he = json.loads(f.read())

    OUTPUT_JSON2 = f'json_files/{periodo}'
    with open(f'{OUTPUT_JSON2}/Json_{cartera.upper()}_{periodo}_v{output_version}.json') as f:
            output_he = json.loads(f.read())["confPython"]["folderS3Destino"]

    dict_argument = {
    '--READ_PATH': output_he,
    '--PERIODO': periodo,
    '--CARTERA': cartera,
    '--OUTPUT_VERSION': output_version,
    '--PESOS': str(list(json_he["params_variables"]["pesos"].values()))
            }

    return dict_argument





# def calcular_previsiones_glue(periodo, cartera, output_version):
#     impactos_file = f'impactos_he_{periodo}_{cartera}_v{output_version}'
#     prefix_s3 = f'{carpet_prefix}/{cal_year(year)}/Impactos/Notebooks/{periodo}/{cartera.upper()}/output_v{output_version}/{impactos_file}'
#     local_athena_results = f'results/{periodo}/{cartera}/output_v{output_version}'

#     prefix_JSON = f'{carpet_prefix}/{cal_year(year)}/Impactos/Inputs_json/{periodo}'
#     local_JSON_results = f'json_files\{periodo}'
#     JSON_file = f'JSON_{cartera.upper()}_{periodo}_v{output_version}.json'

#     try:
#         name_json = s3_download_file(s3_client, 
#                         s3_bucket, 
#                         prefix_JSON, 
#                         local_JSON_results, 
#                         [JSON_file] )["file"][0]
#     except TypeError:
#         print("El archivo JSON no existe")


#     response = glue_client.start_job_run(
#         JobName='ifrs-backtest-23-impactos-banco',
#         Arguments= dict_arguments(periodo, cartera, output_version),
#         Timeout=90,
#         WorkerType='G.2X',
#         NumberOfWorkers=20,
#         ExecutionClass='STANDARD'
#     )

#     response_id = None

#     start_time = time.time()

#     sys.stdout.flush()
#     while response_id != 'SUCCEEDED':
#         response_id = glue_client.get_job_runs(
#                                                 JobName='ifrs-backtest-23-impactos-banco'
#                                             )["JobRuns"][0]["JobRunState"]
#         sys.stdout.flush()
#         if response_id == 'SUCCEEDED':  # Verifica si es verdadero
#             sys.stdout.write("\rSe termino de calcular la provision: SUCCEEDED\n")
#             sys.stdout.flush()
#             break  # Termina el bucle si el archivo se descargó

#         elif response_id == 'FAILED':
#             sys.stdout.write("\rFallo la ejecución: FAILED\n")
#             sys.stdout.flush()
#             break

#         else:
#             elapsed_time = time.time() - start_time
#             sys.stdout.write("\rCalculando la provision... Time: {:.2f}s".format(elapsed_time))
#             sys.stdout.flush()
#             time.sleep(0.1)
#             continue


#         file = list_files_carpet_prefix(s3_client, s3_bucket, prefix_s3)[0].split(impactos_file+'/')[1]

#         s3_download_file(s3_client, 
#                         s3_bucket, 
#                         prefix_s3, 
#                         local_athena_results, 
#                         [file] )

#         nueva_ruta = f'{local_athena_results}/{impactos_file}.csv'
#         ruta_actual = f'{local_athena_results}/{file}'

#         os.replace(ruta_actual, nueva_ruta)
