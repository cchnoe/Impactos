import tkinter as tk
from tkinter import ttk
from libs.configFunx import *
import libs.configFunx as config
from datetime import datetime

################################################################################

##############################################################################################################
# Crear la ventana principal
root = tk.Tk()
root.title("Entrada de datos")
root.geometry("300x510")  # Ajustar el tamaño de la ventana

# Crear el Notebook (pestañas)
notebook = ttk.Notebook(root)
notebook.pack(pady=10, expand=True)

# Crear las pestañas
submit_tab = ttk.Frame(notebook)
download_tab = ttk.Frame(notebook)

notebook.add(submit_tab, text='Submit')
notebook.add(download_tab, text='Download')

# Crear el contenido de cada pestaña
create_submit_tab(root, submit_tab)
create_download_tab(root, download_tab)
root.mainloop()

try:
    data_dict = config.data_dict
    s3_bucket = config.s3_bucket 
    carpet_prefix = config.carpet_prefix
    year = config.year

    athena_client = config.athena_client
    s3_client = config.s3_client
    glue_client = config.glue_client
except:
    pass

