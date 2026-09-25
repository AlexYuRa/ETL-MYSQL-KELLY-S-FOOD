# -*- coding: utf-8 -*-
"""
ETL Kelly's Food - Fase de Transformación
PROCESO 3: NORMALIZACIÓN (corregido)
Módulo N°03 - Base de Datos Avanzada - UNT

Lleva cada valor a un formato único con el TIPO DE DATO correcto:
- Raciones diarias          -> ENTERO (la fuente trae 2.0)
- Precios y montos (S/)     -> DECIMAL con 2 decimales
- Teléfono                  -> texto de 9 dígitos (la fuente trae 951059762.0)
- Nombres/platos/conceptos  -> Tipo Título; conceptos sin sufijo " (S/)"
- Método de pago            -> contra catálogo (Efectivo, Yape, ...)
- Fechas de pago            -> tipo DATE

Suciedad real tratada (reglas genéricas, valen para los 44+ archivos):
1. Fechas de pago en TEXTO con formatos mezclados ('5/3', '5 mar',
   '05-03-26') -> se interpretan como día-mes y el AÑO se toma del
   período del propio archivo.
2. Precio menú con atípicos por dígito de más (70 cuando la moda del
   mes es 7): todo precio igual a 10x la moda se corrige a la moda.
3. Teléfono como decimal: se quita el '.0' ANTES de limpiar no-dígitos
   (si no, '951059762.0' -> '9510597620') y se valida longitud 9.
4. Montos no convertibles a número -> 0 (no se dejan como texto).

Entrada : etl_paso2.pkl (generado por proceso2_agregacion.py)
Salida  : etl_paso3.pkl (para el Proceso 4)
          etl_paso3_normalizacion_<AAAAMM>.xlsx (evidencia)

Uso:
    py proceso3_normalizacion.py
"""

import datetime
import re
import unicodedata

import pandas as pd

from proceso1_elementos_nulos import columnas_fecha, periodo_del_archivo

MESES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
}

def _a_numero(valor) -> float:
    """Convierte a número; si no es convertible devuelve 0 (corrección:
    antes se dejaba el texto tal cual y terminaba como NULL en la BD)."""
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0

def _titulo(texto) -> str:
    if not isinstance(texto, str):
        return texto
    return texto.strip().title()


def _normalizar_metodo_pago(valor) -> str:
    if not isinstance(valor, str):
        return valor
    v = unicodedata.normalize("NFKD", valor.strip().lower())
    v = "".join(c for c in v if not unicodedata.combining(c))
    equivalencias = {
        "efectivo": "Efectivo", "yape": "Yape", "plin": "Plin",
        "transferencia": "Transferencia", "deposito": "Depósito",
    }
    return equivalencias.get(v, valor.strip().title())

def _normalizar_telefono(valor) -> str:
    """Deja solo los dígitos. Si el valor viene como decimal entero
    (951059762.0) primero se quita el '.0' y luego los no-dígitos."""
    if pd.isna(valor):
        return valor
    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)
    return re.sub(r"\D", "", str(valor))

def _normalizar_fecha_pago(valor, anio_periodo: int):
    """
    Normaliza una fecha de pago a tipo DATE
    """
    if pd.isna(valor):
        return valor
    if isinstance(valor, (pd.Timestamp, datetime.datetime, datetime.date)):
        return pd.Timestamp(valor)
    texto = str(valor).strip().lower()

    if re.search(r"\d{4}", texto):
        es_iso = re.match(r"^\s*\d{4}[-/]", texto) is not None
        fecha = pd.to_datetime(texto, dayfirst=not es_iso, errors="coerce")
        if not pd.isna(fecha):
            print(f"  [LOG] Fecha de pago en texto '{valor}' normalizada a {fecha.date()}")
            return pd.Timestamp(fecha)

    numeros = [int(n) for n in re.findall(r"\d+", texto) if 1 <= int(n) <= 31]
    mes_nombre = next((n for nombre, n in MESES.items() if nombre in texto), None)
    candidatos = []
    if mes_nombre is not None and numeros:
        candidatos.append((numeros[0], mes_nombre))        
    elif len(numeros) >= 2:
        candidatos.append((numeros[0], numeros[1]))       
        candidatos.append((numeros[1], numeros[0]))        
    for dia, mes in candidatos:
        if 1 <= mes <= 12:
            try:
                fecha = pd.Timestamp(anio_periodo, mes, dia)
            except ValueError:
                continue
            print(f"  [LOG] Fecha de pago en texto '{valor}' normalizada a {fecha.date()}")
            return fecha

    print(f"  [ALERTA] Fecha de pago no interpretable: '{valor}' (se descarta el pago)")
    return pd.NaT

def _columnas_fecha_pago(df: pd.DataFrame) -> list:
    return [c for c in df.columns if str(c).startswith("Fecha de pago")]

def proceso_normalizacion(hojas: dict) -> dict:
    """
    Aplica las reglas de normalización a cada hoja.
    """
    ventas = hojas["Registro Ventas"].copy()
    anio_periodo = min(pd.Timestamp(f) for f in columnas_fecha(ventas)).year

    ventas["Nombre"] = ventas["Nombre"].apply(_titulo)
    ventas["Apellido"] = ventas["Apellido"].apply(_titulo)

    ventas["Teléfono"] = ventas["Teléfono"].apply(_normalizar_telefono)
    invalidos = ventas.loc[ventas["Teléfono"].astype(str).str.len() != 9, "Teléfono"]
    for tel in invalidos:
        print(f"  [ALERTA] Teléfono con longitud distinta de 9: '{tel}'")

    precios = ventas["Precio menú (S/)"].apply(_a_numero)
    moda_precio = precios.mode().iloc[0]
    atipicos = int((precios == moda_precio * 10).sum())
    if atipicos:
        print(f"  [LOG] {atipicos} precios atípicos ({moda_precio * 10:g}) corregidos a la moda {moda_precio:g}")
    ventas["Precio menú (S/)"] = precios.replace(moda_precio * 10, moda_precio).round(2)

    ventas["Método de pago"] = ventas["Método de pago"].apply(_normalizar_metodo_pago)
    ventas["Pago (S/)"] = ventas["Pago (S/)"].apply(_a_numero).round(2)

    for col in columnas_fecha(ventas):
        ventas[col] = ventas[col].apply(_a_numero).astype(int)

    for col in _columnas_fecha_pago(ventas):
        ventas[col] = ventas[col].apply(_normalizar_fecha_pago, anio_periodo=anio_periodo)

    hojas["Registro Ventas"] = ventas
    print("  - 'Registro Ventas': identidad, teléfono, precio, método de pago, "
          "montos, raciones y fechas de pago normalizados")

    menu = hojas["Menu del dia"].copy()
    menu["Plato del día"] = menu["Plato del día"].apply(_titulo)
    hojas["Menu del dia"] = menu
    print("  - 'Menu del dia': nombre del plato normalizado")

    gastos = hojas["Gastos"].copy()
    # 'Insumos perecibles (S/)' -> 'Insumos Perecibles'
    gastos["Concepto"] = gastos["Concepto"].apply(
        lambda v: _titulo(re.sub(r"\s*\(S/\)\s*$", "", str(v))) if not pd.isna(v) else v
    )
    for col in columnas_fecha(gastos):
        gastos[col] = gastos[col].apply(_a_numero).round(2)
    hojas["Gastos"] = gastos
    print("  - 'Gastos': conceptos (sin sufijo ' (S/)') y montos normalizados")

    return hojas


def main():
    print("1) Cargando resultado del Proceso 2 ('etl_paso2.pkl')...")
    hojas = pd.read_pickle("etl_paso2.pkl")

    print("2) Proceso: Normalización...")
    hojas = proceso_normalizacion(hojas)

    periodo = periodo_del_archivo(hojas)
    print("\n3) Guardando resultado intermedio en 'etl_paso3.pkl'...")
    pd.to_pickle(hojas, "etl_paso3.pkl")

    ruta_evidencia = f"etl_paso3_normalizacion_{periodo}.xlsx"
    print(f"4) Exportando evidencia a '{ruta_evidencia}'...")
    with pd.ExcelWriter(ruta_evidencia) as writer:
        for nombre, df in hojas.items():
            df.to_excel(writer, sheet_name=nombre[:31], index=False)

    print("\nListo. Proceso 3 completado. Ahora puede ejecutar proceso4_adaptabilidad.py")


if __name__ == "__main__":
    main()
