# -*- coding: utf-8 -*-
"""
Kelly's Food - Fase de Carga (corregida)
Este módulo NO genera datos ni SQL: eso ya lo entrega la Fase de
Transformación (proceso4_adaptabilidad.py -> kellys_food_transformado_<AAAAMM>.sql).
Aquí solo se valida ese script y se ejecuta contra un servidor MySQL.

Arquitectura (3 componentes):
    1. Cliente de Carga (este script, Python)
    2. Conector MySQL-Python (mysql-connector-python)
    3. Servidor MySQL (host:puerto) -> base de datos kellys_food

CORRECCIÓN: antes este proceso convertía las marcas 'Sin_Dato' en NULL,
entregando una BD sucia. Ahora incluye una VERIFICACIÓN DE CALIDAD: si
algún INSERT contiene NULL, la carga se ABORTA y el problema se devuelve
a la fase de Transformación. La BD entregada queda limpia: 0 NULL.

ESCENARIO MULTI-ARCHIVO: la carga se ejecuta una vez por cada mes
transformado (44+ archivos), todos hacia la MISMA base de datos. Los
catálogos van como INSERT IGNORE, así repetir o encadenar meses no
duplica maestros. Cada archivo se carga en UNA transacción (ROLLBACK
si algo falla).

Requisitos:
    py -m pip install mysql-connector-python

Configuración (variables de entorno, opcionales):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

Uso:
    py carga_mysql.py [ruta_del_sql]
    (sin argumento usa el kellys_food_transformado_*.sql más reciente)
"""

import glob
import os
import re
import sys

import mysql.connector

# Credenciales tomadas de variables de entorno (nunca escribirlas aquí).
SERVIDOR_MYSQL = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "puerto": int(os.getenv("MYSQL_PORT", "3306")),
    "usuario": os.getenv("MYSQL_USER", "root"),
    "contraseña": os.getenv("MYSQL_PASSWORD", ""),
    "base_datos": os.getenv("MYSQL_DATABASE", "kellys_food"),
}

CARGAR_A_MYSQL = True

def dataset_por_defecto() -> str:
    """El kellys_food_transformado_<AAAAMM>.sql más reciente de la carpeta."""
    candidatos = sorted(glob.glob("kellys_food_transformado_*.sql"))
    if not candidatos:
        raise SystemExit("No hay ningún 'kellys_food_transformado_*.sql'. "
                         "Ejecute primero proceso4_adaptabilidad.py")
    if len(candidatos) > 1:
        print(f"Hay {len(candidatos)} datasets transformados; se usará el último: {candidatos[-1]}")
    return candidatos[-1]

def leer_sentencias_sql(ruta_sql: str) -> list:
    """
    Lee el archivo .sql que entrega la Transformación y lo separa en
    sentencias individuales (por ';'), descartando comentarios y líneas
    vacías.
    """
    with open(ruta_sql, "r", encoding="utf-8") as f:
        contenido = f.read()

    sentencias = []
    for bloque in contenido.split(";"):
        lineas_utiles = [
            linea for linea in bloque.splitlines()
            if linea.strip() and not linea.strip().startswith("--")
        ]
        sentencia = "\n".join(lineas_utiles).strip()
        if sentencia:
            sentencias.append(sentencia)
    return sentencias

def verificar_sin_null(sentencias: list) -> int:
    """
    Verificación de calidad de la BD a entregar: ningún INSERT debe
    contener el literal NULL. Devuelve cuántos lo contienen.
    """
    return sum(
        1 for s in sentencias
        if s.upper().startswith("INSERT") and re.search(r"\bNULL\b", s)
    )

def cargar_a_mysql(ruta_sql: str, servidor: dict = SERVIDOR_MYSQL):
    """
    Valida el dataset transformado (cero NULL) y ejecuta todas sus
    sentencias dentro de UNA única transacción contra el Servidor MySQL.
    Si alguna sentencia falla, se revierte todo (ROLLBACK).
    """
    sentencias = leer_sentencias_sql(ruta_sql)
    print(f"{len(sentencias)} sentencias leídas desde {ruta_sql}")

    con_null = verificar_sin_null(sentencias)
    if con_null:
        raise ValueError(
            f"{con_null} INSERT contienen NULL: la BD NO se carga. "
            "Corregir la Fase de Transformación (Procesos 1-3)."
        )

    cn = mysql.connector.connect(
        host=servidor["host"],
        port=servidor["puerto"],
        user=servidor["usuario"],
        password=servidor["contraseña"],
    )
    cur = cn.cursor()
    try:
        for sentencia in sentencias:
            cur.execute(sentencia)
        cn.commit()
        print(f"Carga completada en la base de datos '{servidor['base_datos']}'. "
              "BD entregada limpia: 0 valores NULL.")
    except mysql.connector.Error as error:
        cn.rollback()
        print(f"[ALERTA] Carga fallida, se revirtió la transacción: {error}")
        raise
    finally:
        cur.close()
        cn.close()

def main():
    ruta = sys.argv[1] if len(sys.argv) > 1 else dataset_por_defecto()
    sentencias = leer_sentencias_sql(ruta)
    con_null = verificar_sin_null(sentencias)
    print(f"Dataset transformado: {ruta} ({len(sentencias)} sentencias).")
    if con_null:
        raise SystemExit(f"[ALERTA] {con_null} INSERT contienen NULL: la BD NO se "
                         "carga. Corregir la Fase de Transformación (Procesos 1-3).")
    print("Verificación de calidad OK: 0 INSERT con NULL.")

    if CARGAR_A_MYSQL:
        cargar_a_mysql(ruta)
    else:
        print("CARGAR_A_MYSQL=False -> no se conectó a ningún servidor. "
              "Cambia la bandera y completa SERVIDOR_MYSQL para cargar de verdad.")


if __name__ == "__main__":
    main()
