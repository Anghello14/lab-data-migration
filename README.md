# lab-data-migration

## Guía completa para reproducir los ambientes: `load_postgres` y `write_csv`

Esta guía (en español) describe un **paso a paso profesional y detallado** para levantar y ejecutar los dos ambientes del repositorio:

- **`load_postgres/`**: ambiente para ejecutar el pipeline que carga/procesa datos hacia PostgreSQL.
- **`write_csv/`**: ambiente para ejecutar el pipeline que genera/escribe archivos CSV.

Cada ambiente tiene su propio `requirements.txt` dentro de su carpeta y su propio ejecutor `main.py`.

> **Convención usada en esta guía**: se crearán **dos entornos virtuales** separados (uno por ambiente) para evitar conflictos de dependencias.

---

## 1) Requisitos previos

Asegúrate de tener instalado:

- **Git**
- **Python 3.10+** (recomendado 3.10 o 3.11)

Verifica:

```bash
python3 --version
git --version
```

---

## 2) Clonar el repositorio

```bash
git clone https://github.com/Anghello14/lab-data-migration.git
cd lab-data-migration
```

---

## 3) Ambiente 1: `load_postgres`

### 3.1 Crear el entorno virtual

Desde la **raíz** del repositorio:

```bash
python3 -m venv .venv-load-postgres
```

### 3.2 Activar el entorno virtual

**macOS / Linux (bash/zsh):**

```bash
source .venv-load-postgres/bin/activate
```

**Windows (PowerShell):**

```powershell
.\.venv-load-postgres\Scripts\Activate.ps1
```

Verifica que el entorno está activo:

```bash
which python
python --version
```

### 3.3 Actualizar pip e instalar librerías (dependencias)

Actualiza herramientas base:

```bash
python -m pip install --upgrade pip setuptools wheel
```

Instala las dependencias del ambiente usando el `requirements.txt` que está dentro de `load_postgres/`:

```bash
pip install -r load_postgres/requirements.txt
```

Verifica lo instalado:

```bash
pip freeze
```

### 3.4 Ejecutar el pipeline (main.py)

Ejecuta el script principal del ambiente:

```bash
python load_postgres/main.py
```

> Si el pipeline requiere variables de entorno (por ejemplo credenciales de base de datos), revisa si el repositorio incluye un `.env.example` o documentación adicional. En caso contrario, crea un `.env` y asegúrate de que el código lo cargue (por ejemplo con `python-dotenv`).

### 3.5 Salir del entorno

```bash
deactivate
```

---

## 4) Ambiente 2: `write_csv`

### 4.1 Crear el entorno virtual

Desde la **raíz** del repositorio:

```bash
python3 -m venv .venv-write-csv
```

### 4.2 Activar el entorno virtual

**macOS / Linux (bash/zsh):**

```bash
source .venv-write-csv/bin/activate
```

**Windows (PowerShell):**

```powershell
.\.venv-write-csv\Scripts\Activate.ps1
```

### 4.3 Actualizar pip e instalar librerías (dependencias)

```bash
python -m pip install --upgrade pip setuptools wheel
```

Instala las dependencias del ambiente usando el `requirements.txt` que está dentro de `write_csv/`:

```bash
pip install -r write_csv/requirements.txt
```

Verifica lo instalado:

```bash
pip freeze
```

### 4.4 Ejecutar el pipeline (main.py)

```bash
python write_csv/main.py
```

### 4.5 Salir del entorno

```bash
deactivate
```

---

## 5) Recomendaciones de buenas prácticas

- Mantén cada entorno aislado: usa **`.venv-load-postgres`** únicamente para `load_postgres` y **`.venv-write-csv`** únicamente para `write_csv`.
- Si cambias dependencias, recuerda regenerar/actualizar el `requirements.txt` del ambiente correspondiente.
- Para evitar errores por versión, puedes fijar la versión de Python (ej. 3.11.x) en tu equipo.

---

## 6) Solución de problemas

### 6.1 En Windows: error al activar entorno (ExecutionPolicy)

En PowerShell, si no te deja ejecutar el script de activación:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 6.2 Problemas instalando dependencias de PostgreSQL

Algunas librerías pueden requerir compiladores o dependencias del sistema. Si el proyecto usa `psycopg2`, normalmente `psycopg2-binary` simplifica la instalación. La fuente de verdad siempre será `load_postgres/requirements.txt`.