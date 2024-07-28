import tkinter as tk
from tkinter import messagebox
from datetime import datetime
import json
import os
import sys
import boto3
from botocore.config import Config
import botocore


try:
    current_dir = os.path.abspath(os.path.dirname(__file__))
except NameError:
    # Si __file__ no está definido, usar el directorio de trabajo actual
    current_dir = os.path.abspath(os.getcwd())

# Configuración de codificación para la consola
sys.stdout.reconfigure(encoding='utf-8')

# Ruta al archivo de configuración
TOKEN_FILE = 'libs/params/aws_tokens.json'

# Configuración de boto3
my_config = Config(
    region_name='us-east-1',
    signature_version='v4',
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)

# Leer tokens desde el archivo
def load_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r', encoding='utf-8') as file:
            tokens = json.load(file)
            return tokens.get('aws_access_key_id'), tokens.get('aws_secret_access_key'), tokens.get('aws_session_token')
    return None, None, None

# Guardar tokens en el archivo
def save_tokens(aws_access_key_id, aws_secret_access_key, aws_session_token):
    tokens = {
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'aws_session_token': aws_session_token
    }
    with open(TOKEN_FILE, 'w', encoding='utf-8') as file:
        json.dump(tokens, file)

def connect(client):
    return boto3.client(client,
                        config=my_config,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token
                        )

def resource(client):
    return boto3.resource(client, config=my_config)

def test_connection():
    try:
        client = connect('s3')
        client.list_buckets()
        return True
    except Exception as e:
        return False

def request_new_tokens():
    aws = tk.Tk()
    aws.title("AWS Credentials")

    tk.Label(aws, text="AWS Access Key ID:").grid(row=0)
    tk.Label(aws, text="AWS Secret Access Key:").grid(row=1)
    tk.Label(aws, text="AWS Session Token:").grid(row=2)

    aws_access_key_id_entry = tk.Entry(aws)
    aws_secret_access_key_entry = tk.Entry(aws)
    aws_session_token_entry = tk.Entry(aws)

    aws_access_key_id_entry.grid(row=0, column=1)
    aws_secret_access_key_entry.grid(row=1, column=1)
    aws_session_token_entry.grid(row=2, column=1)

    def submit():
        global aws_access_key_id, aws_secret_access_key, aws_session_token
        aws_access_key_id = aws_access_key_id_entry.get()
        aws_secret_access_key = aws_secret_access_key_entry.get()
        aws_session_token = aws_session_token_entry.get()
        save_tokens(aws_access_key_id, aws_secret_access_key, aws_session_token)
        aws.destroy()

    def on_closing():
        if messagebox.askokcancel("Quit", "No se ingresaron nuevos tokens. ¿Deseas salir?"):
            aws.destroy()
            raise SystemExit

    aws.protocol("WM_DELETE_WINDOW", on_closing)
    tk.Button(aws, text="Submit", command=submit).grid(row=3, column=1, sticky=tk.W, pady=4)

    aws.mainloop()

    return aws_access_key_id, aws_secret_access_key, aws_session_token




# Cargar tokens desde el archivo
aws_access_key_id, aws_secret_access_key, aws_session_token = load_tokens()

try:
    while not test_connection():
        aws_access_key_id, aws_secret_access_key, aws_session_token = request_new_tokens()
    # messagebox.showinfo("AWS Credentials", "Conexión exitosa con los tokens proporcionados.")
except SystemExit:
    print("El usuario decidió salir del programa sin ingresar nuevos tokens.")



s3_bucket = "interbank-datalake-us-east-1-428938305480-discovery" # No modificar 
carpet_prefix = "ifrs/discovery/discovery_riesgos_calibracion" # No modificar
year = datetime.now().year

athena_client = connect("athena") # No modificar
s3_client = connect("s3") # No modificar
glue_client = connect("glue") # No modificar
##############################

def list_files_carpet_prefix(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [item['Key'] for item in response.get('Contents', []) if item['Key'] != prefix]
    return files

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

def dowload_files_he(periodo, cartera, output_version):
        prefix_files = f'{carpet_prefix}/Calibracion-{year}/Impactos/Notebooks/{periodo}/{cartera.upper()}/output_v{output_version}'
        
        local_JSON_results = f'results\{periodo}\{cartera}\output_v{output_version}'

        if not os.path.exists(local_JSON_results):
            os.makedirs(local_JSON_results)
            
        file_array = list_files_carpet_prefix(s3_client, s3_bucket, prefix_files)
        file_array = [file for file in file_array if "$" not in file]
        file = [files.split(f'output_v{output_version}')[1][1:] for files in file_array
                if len(files.split(f'output_v{output_version}')[1][1:]) < 50]

        s3_download_file(s3_client, 
                        s3_bucket, 
                        prefix_files, 
                        local_JSON_results, 
                        file)
        return 
# Función para crear la pestaña Submit
def create_submit_tab(root,tab):
    data_dict = {}

    # Función para validar que fwl_table_v solo contiene números y tiene hasta 4 dígitos
    def validate_fwl_table_v(event):
        value = entry_fwl_table_v.get()
        if not value.isdigit():
            value = ''.join(filter(str.isdigit, value))
        if len(value) > 4:
            value = value[:4]
        entry_fwl_table_v.delete(0, tk.END)
        entry_fwl_table_v.insert(0, value)

    # Función para actualizar los pesos según el período ingresado
    def update_pesos(event):
        try:
            period_value = int(entry_periodo.get())
            if period_value >= 202403:
                entry_pesos_b.config(state=tk.NORMAL)
                entry_pesos_o.config(state=tk.NORMAL)
                entry_pesos_p.config(state=tk.NORMAL)
                entry_pesos_b.delete(0, tk.END)
                entry_pesos_o.delete(0, tk.END)
                entry_pesos_p.delete(0, tk.END)
                entry_pesos_b.insert(0, '0.58')
                entry_pesos_o.insert(0, '0.07')
                entry_pesos_p.insert(0, '0.35')
                entry_pesos_b.config(state=tk.DISABLED)
                entry_pesos_o.config(state=tk.DISABLED)
                entry_pesos_p.config(state=tk.DISABLED)
            else:
                entry_pesos_b.config(state=tk.NORMAL)
                entry_pesos_o.config(state=tk.NORMAL)
                entry_pesos_p.config(state=tk.NORMAL)
                entry_pesos_b.delete(0, tk.END)
                entry_pesos_o.delete(0, tk.END)
                entry_pesos_p.delete(0, tk.END)
                entry_pesos_b.insert(0, '0.49')
                entry_pesos_o.insert(0, '0.07')
                entry_pesos_p.insert(0, '0.44')
                entry_pesos_b.config(state=tk.DISABLED)
                entry_pesos_o.config(state=tk.DISABLED)
                entry_pesos_p.config(state=tk.DISABLED)
        except ValueError:
            entry_pesos_b.config(state=tk.NORMAL)
            entry_pesos_o.config(state=tk.NORMAL)
            entry_pesos_p.config(state=tk.NORMAL)
            entry_pesos_b.delete(0, tk.END)
            entry_pesos_o.delete(0, tk.END)
            entry_pesos_p.delete(0, tk.END)
            entry_pesos_b.config(state=tk.DISABLED)
            entry_pesos_o.config(state=tk.DISABLED)
            entry_pesos_p.config(state=tk.DISABLED)

    # Función para validar los umbrales
    def validate_umbrales():
        global data_dict  # Usar la variable global
        try:
            abs_trad = rel_trad = abs_miv = rel_miv = None
            
            if umbral1.winfo_ismapped():
                abs_trad = float(entry_umbral1.get())
                rel_trad = float(entry_umbral2.get())
            if umbral11.winfo_ismapped():
                abs_miv = float(entry_umbral11.get())
                rel_miv = float(entry_umbral22.get())

            periodo = int(entry_periodo.get())
            
            data_dict = {
                "cartera": cartera_var.get(),
                "periodo": periodo,
                "user": entry_user.get(),
                "output_version": entry_output_version.get(),
                "fwl_table_v": entry_fwl_table_v.get(),
                "pesos": [
                    entry_pesos_b.get(),
                    entry_pesos_o.get(),
                    entry_pesos_p.get()
                ],
                "umbrales": {
                    "abs_trad": abs_trad,
                    "rel_trad": rel_trad,
                    "abs_miv": abs_miv,
                    "rel_miv": rel_miv
                }
            }

            root.destroy()  # Cerrar la ventana después de la validación exitosa

        except ValueError as e:
            messagebox.showerror("Error", str(e))

    # Función para actualizar los campos de umbrales según la cartera seleccionada
    def update_umbrales(*args):
        clear_umbrales()  # Limpiar umbrales antes de actualizarlos
        cartera = cartera_var.get()
        
        # Mostrar/ocultar umbrales según la cartera seleccionada
        if cartera == 'hipotecas':
            umbral1.config(text="Umbral Abs Trad:")
            umbral2.config(text="Umbral Rel Trad:")
            umbral1.grid(row=13, column=0, padx=10, pady=5)
            entry_umbral1.grid(row=13, column=1, padx=10, pady=5)
            umbral2.grid(row=14, column=0, padx=10, pady=5)
            entry_umbral2.grid(row=14, column=1, padx=10, pady=5)
            umbral11.grid(row=15, column=0, padx=10, pady=5)
            entry_umbral11.grid(row=15, column=1, padx=10, pady=5)
            umbral22.grid(row=16, column=0, padx=10, pady=5)
            entry_umbral22.grid(row=16, column=1, padx=10, pady=5)
        elif cartera in ['tarjetas', 'consumo', 'bpe', 'vehicular', 'convenios', 'empresas']:
            umbral1.config(text="Umbral Abs:")
            umbral2.config(text="Umbral Rel:")
            umbral1.grid(row=13, column=0, padx=10, pady=5)
            entry_umbral1.grid(row=13, column=1, padx=10, pady=5)
            umbral2.grid(row=14, column=0, padx=10, pady=5)
            entry_umbral2.grid(row=14, column=1, padx=10, pady=5)
            umbral11.grid_remove()
            entry_umbral11.grid_remove()
            umbral22.grid_remove()
            entry_umbral22.grid_remove()
        elif cartera in ['corporativa', 'institucional', 'inmobiliarias']:
            umbral1.grid_remove()
            entry_umbral1.grid_remove()
            umbral2.grid_remove()
            entry_umbral2.grid_remove()
            umbral11.grid_remove()
            entry_umbral11.grid_remove()
            umbral22.grid_remove()
            entry_umbral22.grid_remove()
        
        # Actualizar los umbrales vigentes si la casilla está marcada
        if umbrales_vigentes_var.get() == 1:
            set_umbrales_vigentes()
            disable_umbrales()
        else:
            enable_umbrales()

    # Función para habilitar o deshabilitar la entrada de FWL Table V
    def toggle_fwl_table_v():
        if fwl_check_var.get() == 1:
            entry_fwl_table_v.config(state=tk.NORMAL)
            entry_fwl_table_v.delete(0, tk.END)
        else:
            entry_fwl_table_v.config(state=tk.NORMAL)
            entry_fwl_table_v.delete(0, tk.END)
            entry_fwl_table_v.insert(0, "0")
            entry_fwl_table_v.config(state=tk.DISABLED)

    # Función para establecer los umbrales vigentes
    def set_umbrales_vigentes():
        enable_umbrales()
        clear_umbrales()
        cartera = cartera_var.get()
        if cartera == 'tarjetas':
            entry_umbral1.insert(0, '0.053')
            entry_umbral2.insert(0, '0.642')
        elif cartera == 'consumo':
            entry_umbral1.insert(0, '0.052')
            entry_umbral2.insert(0, '0.79')
        elif cartera == 'vehicular':
            entry_umbral1.insert(0, '0.132')
            entry_umbral2.insert(0, '1.000')
        elif cartera == 'bpe':
            entry_umbral1.insert(0, '0.150')
            entry_umbral2.insert(0, '0.237')
        elif cartera == 'hipotecas':
            entry_umbral1.insert(0, '0.018')
            entry_umbral2.insert(0, '26.587')
            entry_umbral11.insert(0, '0.024')
            entry_umbral22.insert(0, '23.199')
        else:
            entry_umbral1.insert(0, '0')
            entry_umbral2.insert(0, '0')
            entry_umbral11.insert(0, '0')
            entry_umbral22.insert(0, '0')

    # Función para limpiar los umbrales
    def clear_umbrales():
        entry_umbral1.delete(0, tk.END)
        entry_umbral2.delete(0, tk.END)
        entry_umbral11.delete(0, tk.END)
        entry_umbral22.delete(0, tk.END)

    # Función para deshabilitar los campos de umbrales
    def disable_umbrales():
        entry_umbral1.config(state=tk.DISABLED)
        entry_umbral2.config(state=tk.DISABLED)
        entry_umbral11.config(state=tk.DISABLED)
        entry_umbral22.config(state=tk.DISABLED)

    # Función para habilitar los campos de umbrales
    def enable_umbrales():
        entry_umbral1.config(state=tk.NORMAL)
        entry_umbral2.config(state=tk.NORMAL)
        entry_umbral11.config(state=tk.NORMAL)
        entry_umbral22.config(state=tk.NORMAL)

    # Función para activar/desactivar los umbrales vigentes
    def toggle_umbrales_vigentes():
        if umbrales_vigentes_var.get() == 1:
            set_umbrales_vigentes()
            disable_umbrales()
        else:
            enable_umbrales()
            clear_umbrales()

    # # Crear la ventana principal
    # tab = tk.Tk()
    # tab.title("Entrada de datos")
    # tab.geometry("300x470")  # Ajustar el tamaño de la ventana

    # Crear los widgets de entrada
    tk.Label(tab, text="Período (YYYYMM):").grid(row=0, column=0, padx=10, pady=5, sticky='w')
    entry_periodo = tk.Entry(tab)
    entry_periodo.grid(row=0, column=1, padx=10, pady=5)
    entry_periodo.bind('<KeyRelease>', update_pesos)

    tk.Label(tab, text="Cartera:").grid(row=1, column=0, padx=10, pady=5, sticky='w')
    carteras = ['tarjetas', 'hipotecas', 'consumo', 'vehicular', 'bpe', 'empresas', 'corporativa', 'institucional', 'inmobiliarias', 'convenios']
    cartera_var = tk.StringVar(tab)
    cartera_var.set(carteras[0])  # valor por defecto
    entry_cartera = tk.OptionMenu(tab, cartera_var, *carteras)
    entry_cartera.grid(row=1, column=1, padx=10, pady=5)
    cartera_var.trace('w', update_umbrales)

    tk.Label(tab, text="User:").grid(row=2, column=0, padx=10, pady=5, sticky='w')
    entry_user = tk.Entry(tab)
    entry_user.grid(row=2, column=1, padx=10, pady=5)

    tk.Label(tab, text="Output Version:").grid(row=3, column=0, padx=10, pady=5, sticky='w')
    entry_output_version = tk.Entry(tab)
    entry_output_version.grid(row=3, column=1, padx=10, pady=5)

    # FWL Table V con casilla de verificación
    fwl_check_var = tk.IntVar()
    fwl_check = tk.Checkbutton(tab, text="FWL Table V:", variable=fwl_check_var, command=toggle_fwl_table_v)
    fwl_check.grid(row=4, column=0, padx=10, pady=5, sticky='w')
    entry_fwl_table_v = tk.Entry(tab)
    entry_fwl_table_v.grid(row=4, column=1, padx=10, pady=5)
    entry_fwl_table_v.bind('<KeyRelease>', validate_fwl_table_v)
    toggle_fwl_table_v()  # Inicializar el estado

    tk.Label(tab, text="Peso Base:").grid(row=5, column=0, padx=10, pady=5, sticky='w')
    entry_pesos_b = tk.Entry(tab, state='disabled')
    entry_pesos_b.grid(row=5, column=1, padx=10, pady=5)

    tk.Label(tab, text="Peso Optimista:").grid(row=6, column=0, padx=10, pady=5, sticky='w')
    entry_pesos_o = tk.Entry(tab, state='disabled')
    entry_pesos_o.grid(row=6, column=1, padx=10, pady=5)

    tk.Label(tab, text="Peso Pesimista:").grid(row=7, column=0, padx=10, pady=5, sticky='w')
    entry_pesos_p = tk.Entry(tab, state='disabled')
    entry_pesos_p.grid(row=7, column=1, padx=10, pady=5)

    # Umbrales
    umbral1 = tk.Label(tab, text="Umbral Abs Trad:")
    entry_umbral1 = tk.Entry(tab)
    umbral2 = tk.Label(tab, text="Umbral Rel Trad:")
    entry_umbral2 = tk.Entry(tab)
    umbral11 = tk.Label(tab, text="Umbral Abs Mi Vi:")
    entry_umbral11 = tk.Entry(tab)
    umbral22 = tk.Label(tab, text="Umbral Rel Mi Vi:")
    entry_umbral22 = tk.Entry(tab)

    # Casilla de verificación para umbrales vigentes
    umbrales_vigentes_var = tk.IntVar(value=1)  # Marcada por defecto
    umbrales_vigentes_check = tk.Checkbutton(tab, text="Umbrales Vigentes", variable=umbrales_vigentes_var, command=toggle_umbrales_vigentes)
    umbrales_vigentes_check.grid(row=8, column=0, padx=10, pady=5, sticky='w')

    # Inicialmente, mostrar/ocultar campos de umbrales según la cartera seleccionada
    update_umbrales()

    # Crear y mostrar el botón de envío
    submit_button = tk.Button(tab, text="Submit", command=validate_umbrales)
    submit_button.grid(row=18, column=0, columnspan=2, pady=10)


def list_files_carpet_prefix(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [item['Key'] for item in response.get('Contents', []) if item['Key'] != prefix]
    return files

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



def dowload_files_he(periodo, cartera, output_version):
        prefix_files = f'{carpet_prefix}/Calibracion-{year}/Impactos/Notebooks/{periodo}/{cartera.upper()}/output_v{output_version}'
        INPUT_PARAMS = f'results\{periodo}\{cartera}\output_v{output_version}'

        local_JSON_results = os.path.join(current_dir, '..', INPUT_PARAMS)
        local_JSON_results = os.path.abspath(local_JSON_results)

        if not os.path.exists(local_JSON_results):
            os.makedirs(local_JSON_results)
            
        file_array = list_files_carpet_prefix(s3_client, s3_bucket, prefix_files+"/")
        file_array = [file for file in file_array if "$" not in file]
        file = [files.split(f'output_v{output_version}')[1][1:] for files in file_array
                if len(files.split(f'output_v{output_version}')[1][1:]) < 70]
        track = s3_download_file(s3_client, 
                        s3_bucket, 
                        prefix_files, 
                        local_JSON_results, 
                        file)
        return track



def create_download_tab(root, tab):
    # Crear los widgets de entrada para download
    tk.Label(tab, text="Período (YYYYMM):").grid(row=0, column=0, padx=10, pady=5, sticky='w')
    entry_periodo_dl = tk.Entry(tab)
    entry_periodo_dl.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(tab, text="Cartera:").grid(row=1, column=0, padx=10, pady=5, sticky='w')
    carteras = ['tarjetas', 'hipotecas', 'consumo', 'vehicular', 'bpe', 'empresas', 'corporativa', 'institucional', 'inmobiliarias', 'convenios']
    cartera_var_dl = tk.StringVar(tab)
    cartera_var_dl.set(carteras[0])  # valor por defecto
    entry_cartera_dl = tk.OptionMenu(tab, cartera_var_dl, *carteras)
    entry_cartera_dl.grid(row=1, column=1, padx=10, pady=5)

    tk.Label(tab, text="Output Version:").grid(row=2, column=0, padx=10, pady=5, sticky='w')
    entry_output_version_dl = tk.Entry(tab)
    entry_output_version_dl.grid(row=2, column=1, padx=10, pady=5)

    # Función para descargar archivos
    def download_files():
        periodo = entry_periodo_dl.get()
        cartera = cartera_var_dl.get()
        output_version = entry_output_version_dl.get()
        track = dowload_files_he(periodo, cartera, output_version)
        if track == {}:
            messagebox.showinfo("Parametros mal ingresados, revisar archivos en s3")
        else:
            messagebox.showinfo("Download", "Archivos descargados exitosamente.")
        root.destroy()  # Cerrar la ventana después de la descarga exitosa

    # Crear y mostrar el botón de descarga
    download_button = tk.Button(tab, text="Download", command=download_files)
    download_button.grid(row=3, column=0, columnspan=2, pady=10)


