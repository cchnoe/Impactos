import os
import subprocess
import sys

# Función para ejecutar comandos del sistema
def run_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error ejecutando el comando: {command}")
        print(result.stderr)
    else:
        print(result.stdout)


# Verificar si pip está instalado y actualizarlo
try:
    import pip
    print("pip ya está instalado. Actualizando pip...")
    run_command("python -m pip install --upgrade pip")
except ImportError:
    print("pip no está instalado. Instalando pip...")
    # Descargar get-pip.py e instalar pip
    run_command("curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py")
    run_command("python get-pip.py")
    os.remove("get-pip.py")

# Instalar tqdm si no está instalado
try:
    from tqdm import tqdm
except ImportError:
    run_command("pip install tqdm")
    from tqdm import tqdm

# Función para instalar paquetes con barra de progreso
def install_packages(env_name, requirements_file):
    with open(requirements_file, 'r') as f:
        packages = f.read().splitlines()

    activate_script = os.path.join(env_name, 'Scripts', 'activate') if os.name == 'nt' else os.path.join(env_name, 'bin', 'activate')
    
    for package in tqdm(packages, desc="Instalando paquetes", unit="paquete"):
        run_command(f"{activate_script} && pip install {package}")

# Crear entorno virtual
env_name = "env_impactos"
print(f"Creando entorno virtual: {env_name}")
run_command(f"python -m venv {env_name}")

# Instalar requerimientos con barra de progreso
requirements_file = 'requirements.txt'
print("Activando entorno virtual e instalando requerimientos...")
install_packages(env_name, requirements_file)

print("Configuración completa.")
