# -*- coding: utf-8 -*-
"""
ETL Kelly's Food - Fase de Transformación
PROCESO 4: ADAPTABILIDAD (corregido)
Módulo N°03 - Base de Datos Avanzada - UNT

Separa la tabla plana (ya limpia, completa y normalizada) en las
entidades del modelo físico y entrega el dataset transformado en dos
formatos equivalentes:
  - kellys_food_entidades_<AAAAMM>.xlsx   -> inspección visual
  - kellys_food_transformado_<AAAAMM>.sql -> DDL + DML (lo que consume
    la Fase de Carga)

Correcciones clave:
1. Ya NO existe la conversión "Sin_Dato -> NULL": los Procesos 1-3
   garantizan valores reales y el DDL es NOT NULL. Ningún INSERT lleva NULL.
2. Trabajador sin DNI; Teléfono VARCHAR(9) NOT NULL.
3. Ración: Cantidad_Raciones INT, solo días con pedido (> 0).
4. Pago REDISEÑADO: una fila por pago real (id_trabajador, nro_pago,
   Fecha_pago, Método, Monto). Quien no pagó no genera filas: nunca hay
   fecha vacía ni centinelas.
5. Compra: solo días con gasto (> 0).
6. Verificación de calidad: si algún valor a insertar estuviera vacío,
   ALERTA y NO se genera el .sql.

Escenario multi-archivo (44+ meses hacia una misma BD):
- Período_Cobro se deriva del rango de fechas del archivo (202304Q1...).
- Catálogos/maestros (Trabajador, Metodo_Pago, Periodo_Cobro, Menu) van
  como INSERT IGNORE: cargar varios meses no duplica ni falla.
- Los archivos de salida llevan el período para no sobrescribirse.

Entrada : etl_paso3.pkl (generado por proceso3_normalizacion.py)
Salida  : kellys_food_entidades_<AAAAMM>.xlsx
          kellys_food_transformado_<AAAAMM>.sql

Uso:
    py proceso4_adaptabilidad.py
"""

import datetime
import math

import pandas as pd

from proceso1_elementos_nulos import columnas_fecha, periodo_del_archivo

def _columnas_fecha_pago(df: pd.DataFrame) -> list:
    return sorted(
        (c for c in df.columns if str(c).startswith("Fecha de pago")),
        key=lambda c: int(str(c).split()[-1]),
    )

def proceso_adaptabilidad(hojas: dict) -> dict:
    """
    Construye las entidades del modelo físico a partir de las hojas
    transformadas en los procesos anteriores.
    """
    ventas = hojas["Registro Ventas"]
    gastos = hojas["Gastos"]
    menu = hojas["Menu del dia"]
    periodo = periodo_del_archivo(hojas)

    entidades = {}

    entidades["Trabajador"] = (
        ventas[["id_trabajador", "Nombre", "Apellido", "Teléfono", "Estado"]]
        .drop_duplicates(subset="id_trabajador")
        .reset_index(drop=True)
    )

    racion = ventas.melt(
        id_vars=["id_trabajador"], value_vars=columnas_fecha(ventas),
        var_name="Fecha", value_name="Cantidad_Raciones",
    )
    racion = racion[racion["Cantidad_Raciones"] > 0].reset_index(drop=True)
    entidades["Ración"] = racion

    pagos = []
    for col in _columnas_fecha_pago(ventas):
        nro = int(str(col).split()[-1])
        con_pago = ventas[ventas[col].notna()]
        pagos.append(pd.DataFrame({
            "id_trabajador": con_pago["id_trabajador"],
            "nro_pago": nro,
            "Fecha_pago": con_pago[col],
            "Método_Pago": con_pago["Método de pago"],
            "Monto": con_pago["Pago (S/)"],
        }))
    entidades["Pago"] = (
        pd.concat(pagos, ignore_index=True) if pagos
        else pd.DataFrame(columns=["id_trabajador", "nro_pago", "Fecha_pago", "Método_Pago", "Monto"])
    )

    metodos = ventas["Método de pago"].dropna().unique().tolist()
    entidades["Método_Pago"] = pd.DataFrame({"Método_Pago": metodos})

    fechas = sorted(pd.Timestamp(f) for f in columnas_fecha(ventas))
    inicio, fin = fechas[0], fechas[-1]
    mitad = inicio + pd.Timedelta(days=13)
    entidades["Período_Cobro"] = pd.DataFrame({
        "id_periodo": [f"{periodo}Q1", f"{periodo}Q2"],
        "Descripción": [
            f"Quincena 1 ({inicio:%d/%m}-{mitad:%d/%m})",
            f"Quincena 2 ({mitad + pd.Timedelta(days=1):%d/%m}-{fin:%d/%m})",
        ],
    })

    entidades["Menú"] = menu.rename(columns={"Fecha": "Fecha_Menu"}).reset_index(drop=True)

    compra = gastos.melt(
        id_vars=["id_compra", "Concepto"], value_vars=columnas_fecha(gastos),
        var_name="Fecha", value_name="Monto",
    )
    compra = compra[compra["Monto"] > 0].reset_index(drop=True)
    entidades["Compra"] = compra

    esquema_vacio = {
        "Proveedor": ["id_proveedor", "RUC", "Primer_Nombre_Proveedor", "Nombre_Contacto"],
        "Tel_Proveedor": ["id_proveedor", "Teléfono"],
        "Urbanización": ["id_urbanizacion", "Nombre_Urbanización"],
        "Distrito": ["id_distrito", "Nombre_Distrito"],
        "Temporada": ["id_temporada", "Fecha_Inicio", "Fecha_Fin"],
        "Clasi_Temporada": ["id_clasificacion", "Descripción"],
        "Insumo": ["id_insumo", "Nombre_Insumo", "id_tipo_insumo", "id_unidad_medida"],
        "Tipo_Insumo": ["id_tipo_insumo", "Nombre_Tipo"],
        "Unidad_Medida": ["id_unidad_medida", "Nombre_Unidad"],
        "Receta": ["id_receta", "id_insumo", "Cantidad_Requerida"],
        "Detallecompra": ["id_compra", "id_insumo", "Cantidad", "Precio_Unitario"],
        "Tel_Trabajador": ["id_trabajador", "Teléfono_Secundario"],
        "Estado_Trabajador": ["id_estado", "Descripción"],
    }
    for entidad, columnas in esquema_vacio.items():
        entidades[entidad] = pd.DataFrame(columns=columnas)
        print(f"[LOG] Entidad sin fuente de datos disponible: {entidad}")

    entidades_con_datos = 7  
    total_esperado = entidades_con_datos + len(esquema_vacio)
    print(f"\nEntidades generadas: {len(entidades)} / esperadas: {total_esperado}")
    if len(entidades) != total_esperado:
        print("[ALERTA] Esquema incompleto: revisar entidades faltantes.")

    return entidades

def verificar_sin_vacios(entidades: dict) -> list:
    """
    Verificación de calidad: la BD debe entregarse LIMPIA, sin vacíos.
    Devuelve la lista de problemas encontrados (vacía si todo está bien).
    """
    problemas = []
    for nombre, df in entidades.items():
        if df.empty:
            continue  
        for col in df.columns:
            n = int(df[col].isna().sum())
            n += int((df[col].astype(str).str.strip() == "").sum())
            if n:
                problemas.append(f"{nombre}.{col}: {n} valores vacíos")
    return problemas


# =====================================================================
# SALIDA DE LA TRANSFORMACIÓN EN FORMATO SQL
# =====================================================================
DDL_TABLAS = {
    "Trabajador": """
        CREATE TABLE IF NOT EXISTS Trabajador (
            id_trabajador VARCHAR(10)  PRIMARY KEY,
            Nombre        VARCHAR(60)  NOT NULL,
            Apellido      VARCHAR(60)  NOT NULL,
            Telefono      VARCHAR(9)   NOT NULL,
            Estado        VARCHAR(20)  NOT NULL
        );""",
    "Metodo_Pago": """
        CREATE TABLE IF NOT EXISTS Metodo_Pago (
            Metodo_Pago VARCHAR(30) PRIMARY KEY
        );""",
    "Periodo_Cobro": """
        CREATE TABLE IF NOT EXISTS Periodo_Cobro (
            id_periodo  VARCHAR(10) PRIMARY KEY,
            Descripcion VARCHAR(60) NOT NULL
        );""",
    "Racion": """
        CREATE TABLE IF NOT EXISTS Racion (
            id_racion          INT AUTO_INCREMENT PRIMARY KEY,
            id_trabajador      VARCHAR(10) NOT NULL,
            Fecha              DATE        NOT NULL,
            Cantidad_Raciones  INT         NOT NULL,
            FOREIGN KEY (id_trabajador) REFERENCES Trabajador(id_trabajador)
        );""",
    "Pago": """
        CREATE TABLE IF NOT EXISTS Pago (
            id_pago        INT AUTO_INCREMENT PRIMARY KEY,
            id_trabajador  VARCHAR(10)   NOT NULL,
            nro_pago       INT           NOT NULL,
            Fecha_pago     DATE          NOT NULL,
            Metodo_Pago    VARCHAR(30)   NOT NULL,
            Monto          DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (id_trabajador) REFERENCES Trabajador(id_trabajador),
            FOREIGN KEY (Metodo_Pago)   REFERENCES Metodo_Pago(Metodo_Pago)
        );""",
    "Menu": """
        CREATE TABLE IF NOT EXISTS Menu (
            Fecha_Menu    DATE PRIMARY KEY,
            Plato_del_dia VARCHAR(120) NOT NULL
        );""",
    "Compra": """
        CREATE TABLE IF NOT EXISTS Compra (
            id_registro INT AUTO_INCREMENT PRIMARY KEY,
            id_compra   VARCHAR(15)   NOT NULL,
            Concepto    VARCHAR(120)  NOT NULL,
            Fecha       DATE          NOT NULL,
            Monto       DECIMAL(10,2) NOT NULL
        );""",
    # --- Entidades sin fuente de datos en el dataset actual: esquema vacío ---
    "Proveedor": """
        CREATE TABLE IF NOT EXISTS Proveedor (
            id_proveedor            VARCHAR(15) PRIMARY KEY,
            RUC                     VARCHAR(11) NOT NULL,
            Primer_Nombre_Proveedor VARCHAR(60) NOT NULL,
            Nombre_Contacto         VARCHAR(60) NOT NULL
        );""",
    "Tel_Proveedor": """
        CREATE TABLE IF NOT EXISTS Tel_Proveedor (
            id_proveedor VARCHAR(15),
            Telefono     VARCHAR(15),
            PRIMARY KEY (id_proveedor, Telefono),
            FOREIGN KEY (id_proveedor) REFERENCES Proveedor(id_proveedor)
        );""",
    "Urbanizacion": """
        CREATE TABLE IF NOT EXISTS Urbanizacion (
            id_urbanizacion    INT AUTO_INCREMENT PRIMARY KEY,
            Nombre_Urbanizacion VARCHAR(80) NOT NULL
        );""",
    "Distrito": """
        CREATE TABLE IF NOT EXISTS Distrito (
            id_distrito    INT AUTO_INCREMENT PRIMARY KEY,
            Nombre_Distrito VARCHAR(80) NOT NULL
        );""",
    "Temporada": """
        CREATE TABLE IF NOT EXISTS Temporada (
            id_temporada INT AUTO_INCREMENT PRIMARY KEY,
            Fecha_Inicio DATE NOT NULL,
            Fecha_Fin    DATE NOT NULL
        );""",
    "Clasi_Temporada": """
        CREATE TABLE IF NOT EXISTS Clasi_Temporada (
            id_clasificacion INT AUTO_INCREMENT PRIMARY KEY,
            Descripcion      VARCHAR(80) NOT NULL
        );""",
    "Insumo": """
        CREATE TABLE IF NOT EXISTS Insumo (
            id_insumo         INT AUTO_INCREMENT PRIMARY KEY,
            Nombre_Insumo     VARCHAR(80) NOT NULL,
            id_tipo_insumo    INT,
            id_unidad_medida  INT,
            FOREIGN KEY (id_tipo_insumo)   REFERENCES Tipo_Insumo(id_tipo_insumo),
            FOREIGN KEY (id_unidad_medida) REFERENCES Unidad_Medida(id_unidad_medida)
        );""",
    "Tipo_Insumo": """
        CREATE TABLE IF NOT EXISTS Tipo_Insumo (
            id_tipo_insumo INT AUTO_INCREMENT PRIMARY KEY,
            Nombre_Tipo    VARCHAR(60) NOT NULL
        );""",
    "Unidad_Medida": """
        CREATE TABLE IF NOT EXISTS Unidad_Medida (
            id_unidad_medida INT AUTO_INCREMENT PRIMARY KEY,
            Nombre_Unidad    VARCHAR(30) NOT NULL
        );""",
    "Receta": """
        CREATE TABLE IF NOT EXISTS Receta (
            id_receta          INT AUTO_INCREMENT PRIMARY KEY,
            id_insumo           INT,
            Cantidad_Requerida  DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (id_insumo) REFERENCES Insumo(id_insumo)
        );""",
    "Detallecompra": """
        CREATE TABLE IF NOT EXISTS Detallecompra (
            id_compra       VARCHAR(15),
            id_insumo       INT,
            Cantidad        DECIMAL(10,2) NOT NULL,
            Precio_Unitario DECIMAL(10,2) NOT NULL,
            PRIMARY KEY (id_compra, id_insumo),
            FOREIGN KEY (id_insumo) REFERENCES Insumo(id_insumo)
        );""",
    "Tel_Trabajador": """
        CREATE TABLE IF NOT EXISTS Tel_Trabajador (
            id_trabajador       VARCHAR(10),
            Telefono_Secundario VARCHAR(15),
            PRIMARY KEY (id_trabajador, Telefono_Secundario),
            FOREIGN KEY (id_trabajador) REFERENCES Trabajador(id_trabajador)
        );""",
    "Estado_Trabajador": """
        CREATE TABLE IF NOT EXISTS Estado_Trabajador (
            id_estado   INT AUTO_INCREMENT PRIMARY KEY,
            Descripcion VARCHAR(40) NOT NULL
        );""",
}

ORDEN_CREACION = [
    "Trabajador", "Metodo_Pago", "Periodo_Cobro", "Racion", "Pago", "Menu", "Compra",
    "Proveedor", "Tel_Proveedor", "Urbanizacion", "Distrito", "Temporada",
    "Clasi_Temporada", "Tipo_Insumo", "Unidad_Medida", "Insumo", "Receta",
    "Detallecompra", "Tel_Trabajador", "Estado_Trabajador",
]

TABLAS_INSERT_IGNORE = {"Trabajador", "Metodo_Pago", "Periodo_Cobro", "Menu"}

NOMBRE_TABLA_SQL = {
    "Trabajador": "Trabajador", "Ración": "Racion", "Pago": "Pago",
    "Método_Pago": "Metodo_Pago", "Período_Cobro": "Periodo_Cobro",
    "Menú": "Menu", "Compra": "Compra",
    "Proveedor": "Proveedor", "Tel_Proveedor": "Tel_Proveedor",
    "Urbanización": "Urbanizacion", "Distrito": "Distrito", "Temporada": "Temporada",
    "Clasi_Temporada": "Clasi_Temporada", "Insumo": "Insumo",
    "Tipo_Insumo": "Tipo_Insumo", "Unidad_Medida": "Unidad_Medida",
    "Receta": "Receta", "Detallecompra": "Detallecompra",
    "Tel_Trabajador": "Tel_Trabajador", "Estado_Trabajador": "Estado_Trabajador",
}

RENOMBRAR_COLUMNAS = {
    "Trabajador": {"id_trabajador": "id_trabajador", "Nombre": "Nombre", "Apellido": "Apellido",
                   "Teléfono": "Telefono", "Estado": "Estado"},
    "Ración": {"id_trabajador": "id_trabajador", "Fecha": "Fecha", "Cantidad_Raciones": "Cantidad_Raciones"},
    "Pago": {"id_trabajador": "id_trabajador", "nro_pago": "nro_pago", "Fecha_pago": "Fecha_pago",
             "Método_Pago": "Metodo_Pago", "Monto": "Monto"},
    "Método_Pago": {"Método_Pago": "Metodo_Pago"},
    "Período_Cobro": {"id_periodo": "id_periodo", "Descripción": "Descripcion"},
    "Menú": {"Fecha_Menu": "Fecha_Menu", "Plato del día": "Plato_del_dia"},
    "Compra": {"id_compra": "id_compra", "Concepto": "Concepto", "Fecha": "Fecha", "Monto": "Monto"},
}


def preparar_para_sql(entidades: dict) -> dict:
    """Renombra cada entidad/columna a su nombre SQL. Ya no existe la
    conversión 'Sin_Dato -> NULL': los datos llegan completos."""
    preparadas = {}
    for nombre, df in entidades.items():
        tabla_sql = NOMBRE_TABLA_SQL.get(nombre, nombre)
        df2 = df.copy()
        if nombre in RENOMBRAR_COLUMNAS:
            df2 = df2.rename(columns=RENOMBRAR_COLUMNAS[nombre])
            columnas_sql = list(RENOMBRAR_COLUMNAS[nombre].values())
            df2 = df2[[c for c in columnas_sql if c in df2.columns]]
        preparadas[tabla_sql] = df2
    return preparadas


def _literal_sql(valor):
    """Convierte un valor de Python/pandas al literal SQL correspondiente."""
    if valor is None or (isinstance(valor, float) and math.isnan(valor)):
        raise ValueError("Valor vacío detectado al generar SQL: la verificación "
                         "de calidad debió impedirlo (revisar Procesos 1-3).")
    if isinstance(valor, (pd.Timestamp, datetime.datetime, datetime.date)):
        return f"'{pd.Timestamp(valor).strftime('%Y-%m-%d')}'"
    if isinstance(valor, (int, float)):
        if isinstance(valor, float) and valor.is_integer():
            return str(int(valor))
        return str(valor)
    texto = str(valor).replace("'", "''")
    return f"'{texto}'"


def generar_dataset_transformado_sql(entidades: dict, periodo: str, ruta_sql: str | None = None) -> str:
    """
    Serializa el dataset ya transformado como un único script SQL:
    DDL (CREATE DATABASE/TABLE) + DML (INSERT / INSERT IGNORE).
    Este archivo ES la salida de la Transformación en formato de carga;
    la Fase de Carga no lo genera, solo lo ejecuta contra MySQL.
    """
    if ruta_sql is None:
        ruta_sql = f"kellys_food_transformado_{periodo}.sql"
    preparadas = preparar_para_sql(entidades)
    lineas = [
        "-- =====================================================================",
        f"-- Kelly's Food - Dataset transformado del período {periodo}",
        "-- Salida de la Fase de Transformación: sin marcas ni valores NULL",
        "-- =====================================================================",
        "CREATE DATABASE IF NOT EXISTS kellys_food;",
        "USE kellys_food;",
        "",
    ]
    for tabla in ORDEN_CREACION:
        lineas.append(DDL_TABLAS[tabla].strip())
        lineas.append("")
    for tabla in ORDEN_CREACION:
        df = preparadas.get(tabla)
        if df is None or df.empty:
            continue
        verbo = "INSERT IGNORE INTO" if tabla in TABLAS_INSERT_IGNORE else "INSERT INTO"
        lineas.append(f"-- Datos de {tabla} ({len(df)} filas)")
        columnas = ", ".join(df.columns)
        for _, fila in df.iterrows():
            valores = ", ".join(_literal_sql(v) for v in fila)
            lineas.append(f"{verbo} {tabla} ({columnas}) VALUES ({valores});")
        lineas.append("")

    with open(ruta_sql, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))
    return ruta_sql


def main():
    print("1) Cargando resultado del Proceso 3 ('etl_paso3.pkl')...")
    hojas = pd.read_pickle("etl_paso3.pkl")
    periodo = periodo_del_archivo(hojas)

    print("2) Proceso: Adaptabilidad (separación en entidades)...\n")
    entidades = proceso_adaptabilidad(hojas)

    print("\n3) Verificación de calidad (BD limpia, sin vacíos)...")
    problemas = verificar_sin_vacios(entidades)
    if problemas:
        for p in problemas:
            print(f"  [ALERTA] {p}")
        raise SystemExit("Hay valores vacíos: revisar Procesos 1-3. NO se genera el .sql")
    print("  OK: ninguna entidad contiene valores vacíos")

    ruta_xlsx = f"kellys_food_entidades_{periodo}.xlsx"
    print(f"\n4) Exportando '{ruta_xlsx}' (una hoja por entidad)...")
    with pd.ExcelWriter(ruta_xlsx) as writer:
        for nombre, df in entidades.items():
            # Excel limita nombres de hoja a 31 caracteres
            df.to_excel(writer, sheet_name=nombre[:31], index=False)

    print("5) Generando el dataset transformado en formato SQL...")
    ruta_sql = generar_dataset_transformado_sql(entidades, periodo)
    print(f"   -> {ruta_sql}")

    print("\nListo. Filas por entidad:")
    for nombre, df in entidades.items():
        print(f"  - {nombre}: {len(df)} filas")

    print(f"\nProceso 4 completado. Ahora puede ejecutar: py carga_mysql.py {ruta_sql}")


if __name__ == "__main__":
    main()
