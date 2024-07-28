$WScriptShell = New-Object -ComObject WScript.Shell

# Ruta del escritorio
$DesktopPath = [System.Environment]::GetFolderPath("Desktop")

# Crear acceso directo para impactos.bat
$ImpactosTargetPath = Join-Path $PSScriptRoot "impactos.bat"
$ImpactosShortcutPath = Join-Path $DesktopPath "impactos.lnk"
Write-Host "Creating shortcut for impactos.bat: $ImpactosTargetPath -> $ImpactosShortcutPath"
$ImpactosShortcut = $WScriptShell.CreateShortcut($ImpactosShortcutPath)
$ImpactosShortcut.TargetPath = $ImpactosTargetPath
$ImpactosShortcut.IconLocation = Join-Path $PSScriptRoot "libs\params\icon_impactos.ico"
$ImpactosShortcut.Save()

# Crear acceso directo para la carpeta results
$ResultsFolderPath = Join-Path $PSScriptRoot "results"
if (-Not (Test-Path -Path $ResultsFolderPath)) {
    Write-Host "Creating results folder: $ResultsFolderPath"
    New-Item -ItemType Directory -Path $ResultsFolderPath
}

$ResultsShortcutPath = Join-Path $DesktopPath "results.lnk"
Write-Host "Creating shortcut for results folder: $ResultsFolderPath -> $ResultsShortcutPath"
$ResultsShortcut = $WScriptShell.CreateShortcut($ResultsShortcutPath)
$ResultsShortcut.TargetPath = $ResultsFolderPath
$ResultsShortcut.IconLocation = "shell32.dll,3"  # Cambia esto si tienes un icono específico
$ResultsShortcut.Save()

# Crear acceso directo para la carpeta raw_txt
$Raw_txtFolderPath = Join-Path $PSScriptRoot "raw_txt"
if (-Not (Test-Path -Path $Raw_txtFolderPath)) {
    Write-Host "Creating results folder: $Raw_txtFolderPath"
    New-Item -ItemType Directory -Path $Raw_txtFolderPath
}

$Raw_txtShortcutPath = Join-Path $DesktopPath "raw_txt.lnk"
Write-Host "Creating shortcut for results folder: $Raw_txtFolderPath -> $Raw_txtShortcutPath"
$Raw_txtShortcut = $WScriptShell.CreateShortcut($Raw_txtShortcutPath)
$Raw_txtShortcut.TargetPath = $Raw_txtFolderPath
$Raw_txtShortcut.IconLocation = "shell32.dll,3"  # Cambia esto si tienes un icono específico
$Raw_txtShortcut.Save()



Write-Host "Shortcuts created successfully."
