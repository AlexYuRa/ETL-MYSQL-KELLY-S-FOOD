# -*- coding: utf-8 -*-
"""
ETL Kelly's Food - Fase de Transformación
PROCESO 1: ELEMENTOS NULOS (corregido)
Módulo N°03 - Base de Datos Avanzada - UNT

Un ETL de limpieza debe TRATAR el dato faltante, no marcarlo. Cada vacío
se maneja según lo que representa (ver proceso1_elementos_nulos.psc):
  a) Fila resumen ("TOTAL", "TOTAL GASTOS", "Total Ingresos...",
     "Ganancia Neta...") o completamente vacía -> se ELIMINA (los
     totales son derivables con SQL).
  b) Columna 100% vacía -> se ELIMINA (regla dinámica: cubre Quincenas,
     Total Mensual, Costo de envío, Cuenta por pagar, Deuda, fechas de
     pago sin uso y los días sin reparto, p.ej. domingos).
  c) Fila de menú sin plato (día sin atención) -> se ELIMINA.
  d) Ración vacía (cliente no pidió ese día) -> 0.
  e) Monto vacío (pago/gasto sin registro) -> 0.0.
  f) Método de pago vacío -> moda de la columna (del archivo en proceso).
  g) Fecha de pago vacía -> SE DEJA VACÍA: es un pago inexistente y el
     Proceso 4 no generará fila para ella (nunca llega NULL a la BD).
  h) Identidad (Nombre, Apellido, Teléfono) -> se valida; la fuente
     garantiza que está completa (ya no existe la columna DNI).

GENERALIDAD: el ETL se ejecuta una vez por cada Excel mensual (44+
archivos, misma estructura, distinta data); nada depende de un mes
concreto.

Entrada : KellysFood_*.xlsx (ruta como argumento, o el primero de la carpeta)
Salida  : etl_paso1.pkl (para el Proceso 2)
          etl_paso1_elementos_nulos_<AAAAMM>.xlsx (evidencia)

Uso:
    py proceso1_elementos_nulos.py [ruta_excel]
"""

import datetime
import glob
import pickle
import sys

import pandas as pd


# =====================================================================
# FASE DE EXTRACCIÓN
# =====================================================================
def cargar_dataset(ruta_excel) -> dict:
    """
    Carga las 3 hojas del Excel fuente. La fila 1 es un título, por eso el encabezado real está en la fila 2 (header=1 en pandas, base 0). Devuelve un diccionario {nombre_hoja: DataFrame}.
    """
    xls = pd.ExcelFile(ruta_excel)
    return {
        "Registro Ventas": pd.read_excel(xls, sheet_name="Registro Ventas", header=1),
        "Menu del dia": pd.read_excel(xls, sheet_name="Menú del día", header=1),
        "Gastos": pd.read_excel(xls, sheet_name="Gastos", header=1),
    }

def columnas_fecha(df: pd.DataFrame) -> list:
    """Devuelve las columnas del DataFrame cuyo nombre es una fecha real,
    excluyendo columnas resumen como 'Quincena 1', 'Total Mensual', etc."""
    return [c for c in df.columns if isinstance(c, (pd.Timestamp, datetime.datetime, datetime.date))]

def periodo_del_archivo(hojas: dict) -> str:
    """Período AAAAMM del archivo en proceso, derivado de la primera de sus columnas de fecha (nunca de una constante)."""
    fechas = columnas_fecha(hojas["Registro Ventas"])
    inicio = min(pd.Timestamp(f) for f in fechas)
    return f"{inicio.year:04d}{inicio.month:02d}"

def _es_fila_resumen(valor) -> bool:
    """¿La primera columna indica una fila de totales y no un dato?"""
    t = str(valor).strip().upper()
    return t.startswith("TOTAL") or t.startswith("GANANCIA NETA")

def proceso_elementos_nulos(hojas: dict):
    """
    Aplica las reglas (a)-(h) a cada hoja. Devuelve (hojas, resumen),
    donde resumen registra por hoja las filas/columnas eliminadas y las
    celdas imputadas (evidencia para el informe).
    """
    resumen = {}
    for nombre, df in hojas.items():
        filas_antes, cols_antes = df.shape
        df = df.copy()

        df = df[~df.iloc[:, 0].apply(_es_fila_resumen)]
        df = df.dropna(how="all")

        if nombre == "Menu del dia":
            df = df.dropna(subset=["Plato del día"])

        df = df.dropna(axis=1, how="all")
        df = df.reset_index(drop=True)

        imputadas = 0
        if nombre == "Registro Ventas":
            for col in columnas_fecha(df):
                imputadas += int(df[col].isna().sum())
                df[col] = df[col].fillna(0)         
            if "Pago (S/)" in df.columns:
                imputadas += int(df["Pago (S/)"].isna().sum())
                df["Pago (S/)"] = df["Pago (S/)"].fillna(0.0)   
            if "Precio menú (S/)" in df.columns and df["Precio menú (S/)"].isna().any():
                moda_precio = df["Precio menú (S/)"].mode().iloc[0]
                imputadas += int(df["Precio menú (S/)"].isna().sum())
                df["Precio menú (S/)"] = df["Precio menú (S/)"].fillna(moda_precio)
            if "Método de pago" in df.columns and df["Método de pago"].isna().any():
                moda_metodo = df["Método de pago"].mode().iloc[0]      
                imputadas += int(df["Método de pago"].isna().sum())
                df["Método de pago"] = df["Método de pago"].fillna(moda_metodo)
            for col in ["Nombre", "Apellido", "Teléfono"]:
                n = int(df[col].isna().sum())
                if n:
                    print(f"  [ALERTA] {n} registros sin {col} en 'Registro Ventas'")
        elif nombre == "Gastos":
            for col in columnas_fecha(df):
                imputadas += int(df[col].isna().sum())
                df[col] = df[col].fillna(0.0)     

        hojas[nombre] = df
        resumen[nombre] = {
            "filas_eliminadas": filas_antes - df.shape[0],
            "columnas_eliminadas": cols_antes - df.shape[1],
            "celdas_imputadas": imputadas,
        }
        print(f"  - '{nombre}': {resumen[nombre]['filas_eliminadas']} filas y "
              f"{resumen[nombre]['columnas_eliminadas']} columnas eliminadas, "
              f"{imputadas} celdas imputadas")
    return hojas, resumen


def excel_por_defecto() -> str:
    candidatos = sorted(glob.glob("KellysFood_*.xlsx"))
    if not candidatos:
        raise SystemExit("No se encontró ningún 'KellysFood_*.xlsx' en la carpeta. "
                         "Indique la ruta: py proceso1_elementos_nulos.py <ruta_excel>")
    if len(candidatos) > 1:
        print(f"Hay {len(candidatos)} Excels; se procesará el primero: {candidatos[0]}")
    return candidatos[0]


def main():
    ruta = sys.argv[1] if len(sys.argv) > 1 else excel_por_defecto()
    print(f"1) Extracción de '{ruta}'...")
    hojas = cargar_dataset(ruta)

    print("2) Proceso: Elementos Nulos...")
    hojas, _ = proceso_elementos_nulos(hojas)

    periodo = periodo_del_archivo(hojas)
    print(f"\n3) Guardando resultado intermedio en 'etl_paso1.pkl' (período {periodo})...")
    with open("etl_paso1.pkl", "wb") as f:
        pickle.dump(hojas, f)

    ruta_evidencia = f"etl_paso1_elementos_nulos_{periodo}.xlsx"
    print(f"4) Exportando evidencia a '{ruta_evidencia}'...")
    with pd.ExcelWriter(ruta_evidencia) as writer:
        for nombre, df in hojas.items():
            df.to_excel(writer, sheet_name=nombre[:31], index=False)

    print("\nListo. Proceso 1 completado. Ahora puede ejecutar proceso2_agregacion.py")


if __name__ == "__main__":
    main()
