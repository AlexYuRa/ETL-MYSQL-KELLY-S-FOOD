# -*- coding: utf-8 -*-
"""
ETL Kelly's Food - Fase de Transformación
PROCESO 2: AGREGACIÓN DE ELEMENTOS FALTANTES (corregido)
Módulo N°03 - Base de Datos Avanzada - UNT

- 'Registro Ventas': asigna id_trabajador (uno por persona, reutilizado
  en todas sus filas) y Estado (por defecto 'Activo').
- 'Menu del dia': no se realiza ninguna agregación.
- 'Gastos': asigna id_compra con el período como prefijo.

La fuente ya NO trae DNI: la identidad de una persona es la combinación
Nombre + Apellido + Teléfono.

ESCENARIO MULTI-ARCHIVO (44+ Excels hacia una misma BD):
- Los id_trabajador NO se reinician por archivo: se mantiene un MAESTRO
  acumulado (maestro_trabajadores.csv) con los ids ya emitidos en meses
  anteriores; el mismo cliente conserva su id y el contador continúa.
- Los id_compra llevan el período AAAAMM como prefijo (C202304-0001)
  para no colisionar entre meses.

Entrada : etl_paso1.pkl (generado por proceso1_elementos_nulos.py)
Salida  : etl_paso2.pkl (para el Proceso 3)
          etl_paso2_agregacion_<AAAAMM>.xlsx (evidencia)
          maestro_trabajadores.csv (acumulado entre corridas)

Uso:
    py proceso2_agregacion.py
"""

import os
import pickle
import re

import pandas as pd

from proceso1_elementos_nulos import periodo_del_archivo

RUTA_MAESTRO = "maestro_trabajadores.csv"

def _clave_persona(nombre, apellido, telefono) -> str:
    """Identidad normalizada de una persona (Nombre|Apellido|Teléfono),
    estable entre archivos aunque el formato crudo varíe."""
    if isinstance(telefono, float) and telefono.is_integer():
        telefono = int(telefono)
    tel = re.sub(r"\D", "", str(telefono))
    return f"{str(nombre).strip().title()}|{str(apellido).strip().title()}|{tel}"


def _generar_id_trabajador(nombre: str, apellido: str, contador: int) -> str:
    """
    Genera un id con el patrón de la Figura N°12 del informe:
    3 dígitos secuenciales + 2 primeras letras del nombre + 2 primeras
    letras del apellido, en mayúsculas. Ej: persona 14, 'Doris Marín'
    -> '014DOMA'. El contador es GLOBAL (continúa entre archivos).
    """
    ini_nombre = str(nombre).strip()[:2].upper()
    ini_apellido = str(apellido).strip()[:2].upper()
    return f"{contador:03d}{ini_nombre}{ini_apellido}"


def _cargar_maestro(ruta: str) -> dict:
    """Mapa clave_persona -> id_trabajador acumulado de meses anteriores."""
    if not os.path.exists(ruta):
        return {}
    maestro = pd.read_csv(ruta, dtype=str)
    return dict(zip(maestro["clave_persona"], maestro["id_trabajador"]))


def _guardar_maestro(mapa: dict, ruta: str):
    pd.DataFrame(
        {"clave_persona": list(mapa.keys()), "id_trabajador": list(mapa.values())}
    ).to_csv(ruta, index=False, encoding="utf-8")


def proceso_agregacion(hojas: dict, ruta_maestro: str = RUTA_MAESTRO) -> dict:
    """
    Agrega los identificadores y campos que la fuente no trae.
    """
    ventas = hojas["Registro Ventas"].copy()

    mapa_ids = _cargar_maestro(ruta_maestro)
    contador = max((int(i[:3]) for i in mapa_ids.values()), default=0)
    print(f"  - Maestro: {len(mapa_ids)} trabajadores ya registrados en meses anteriores")

    nuevos = 0
    ids_generados = []
    for _, fila in ventas.iterrows():
        clave = _clave_persona(fila["Nombre"], fila["Apellido"], fila["Teléfono"])
        if clave not in mapa_ids:
            contador += 1
            mapa_ids[clave] = _generar_id_trabajador(fila["Nombre"], fila["Apellido"], contador)
            nuevos += 1
        ids_generados.append(mapa_ids[clave])

    ventas.insert(0, "id_trabajador", ids_generados)
    ventas["Estado"] = "Activo"
    hojas["Registro Ventas"] = ventas
    _guardar_maestro(mapa_ids, ruta_maestro)
    print(f"  - 'Registro Ventas': {nuevos} id_trabajador nuevos "
          f"({len(set(ids_generados))} personas en este mes) + Estado='Activo'")

    periodo = periodo_del_archivo(hojas)
    gastos = hojas["Gastos"].copy()
    gastos.insert(0, "id_compra", [f"C{periodo}-{i+1:04d}" for i in range(len(gastos))])
    hojas["Gastos"] = gastos
    print(f"  - 'Gastos': {len(gastos)} id_compra generados (prefijo C{periodo}-)")
    print("  - 'Menu del dia': sin agregación (no requiere)")

    return hojas

def main():
    print("1) Cargando resultado del Proceso 1 ('etl_paso1.pkl')...")
    hojas = pd.read_pickle("etl_paso1.pkl")

    print("2) Proceso: Agregación de elementos faltantes...")
    hojas = proceso_agregacion(hojas)

    periodo = periodo_del_archivo(hojas)
    print("\n3) Guardando resultado intermedio en 'etl_paso2.pkl'...")
    with open("etl_paso2.pkl", "wb") as f:
        pickle.dump(hojas, f)

    ruta_evidencia = f"etl_paso2_agregacion_{periodo}.xlsx"
    print(f"4) Exportando evidencia a '{ruta_evidencia}'...")
    with pd.ExcelWriter(ruta_evidencia) as writer:
        for nombre, df in hojas.items():
            df.to_excel(writer, sheet_name=nombre[:31], index=False)

    print("\nListo. Proceso 2 completado. Ahora puede ejecutar proceso3_normalizacion.py")


if __name__ == "__main__":
    main()
