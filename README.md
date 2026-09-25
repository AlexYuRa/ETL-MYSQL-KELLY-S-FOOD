# ETL Kelly's Food

Proceso ETL (Extracción, Transformación y Carga) para los registros mensuales del
servicio de menús **Kelly's Food**. Toma los Excel mensuales de ventas, menú y gastos,
los limpia y normaliza, los separa en las entidades del modelo físico y los carga en
una base de datos MySQL **sin valores NULL**.

> Módulo N°03 — Base de Datos Avanzada — Universidad Nacional de Trujillo (UNT)

## Pipeline

| Paso | Script | Qué hace |
|------|--------|----------|
| 1 | `proceso1_elementos_nulos.py` | Elimina filas resumen y columnas 100 % vacías, imputa raciones (`0`), montos (`0.0`) y método de pago (moda). |
| 2 | `proceso2_agregacion.py` | Asigna `id_trabajador` (estable entre meses mediante un maestro acumulado), `Estado` e `id_compra` con prefijo de período. |
| 3 | `proceso3_normalizacion.py` | Tipos correctos (enteros, decimales, fechas), teléfonos de 9 dígitos, texto en Tipo Título, catálogo de métodos de pago, corrección de precios atípicos. |
| 4 | `proceso4_adaptabilidad.py` | Separa la tabla plana en 20 entidades, verifica que no haya vacíos y genera `kellys_food_entidades_<AAAAMM>.xlsx` y `kellys_food_transformado_<AAAAMM>.sql`. |
| 5 | `carga_mysql.py` | Valida que ningún `INSERT` contenga `NULL` y ejecuta el `.sql` en MySQL dentro de una transacción (ROLLBACK si algo falla). |

`interfaz_etl.py` ofrece una interfaz web (Streamlit) que ejecuta los cinco pasos sobre
uno o varios Excel a la vez, con vista previa y descarga de cada resultado.

## Requisitos

- Python 3.10+
- MySQL 8 (solo para el paso de carga)

```bash
py -m pip install -r requirements.txt
```

## Uso

### Interfaz web

```bash
py -m streamlit run interfaz_etl.py
```

### Línea de comandos

Coloque los archivos `KellysFood_*.xlsx` en la carpeta del proyecto y ejecute en orden:

```bash
py proceso1_elementos_nulos.py [ruta_excel]
py proceso2_agregacion.py
py proceso3_normalizacion.py
py proceso4_adaptabilidad.py
py carga_mysql.py [ruta_sql]
```

### Conexión a MySQL

Las credenciales se leen de variables de entorno (si no se definen se usan los valores
por defecto entre paréntesis):

| Variable | Por defecto |
|----------|-------------|
| `MYSQL_HOST` | `localhost` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASSWORD` | *(vacía)* |
| `MYSQL_DATABASE` | `kellys_food` |

PowerShell:

```powershell
$env:MYSQL_PASSWORD = "tu_contraseña"
py carga_mysql.py
```

## Formato esperado de los Excel

Cada archivo mensual debe contener las hojas **Registro Ventas**, **Menú del día** y
**Gastos**, con el título en la fila 1 y los encabezados en la fila 2. En *Registro
Ventas* y *Gastos* las columnas de días deben ser fechas reales.

## Datos

Por privacidad, este repositorio **no incluye** los Excel fuente, el maestro de
trabajadores (`maestro_trabajadores.csv`, que contiene nombres y teléfonos) ni las
salidas generadas. Todos se crean localmente al ejecutar el pipeline y están excluidos
en `.gitignore`.
# ETL-MYSQL-KELLY-S-FOOD
