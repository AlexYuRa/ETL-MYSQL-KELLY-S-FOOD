# -*- coding: utf-8 -*-
"""
ETL Kelly's Food - Interfaz web (corregida, multi-archivo)
Módulo N°03 - Base de Datos Avanzada - UNT

Permite subir UNO O VARIOS Excels mensuales (44+ disponibles, misma
estructura) y ejecutar los procesos de la fase de transformación uno a
uno sobre todos ellos, viendo y descargando el resultado de cada paso.
Reutiliza las funciones de los scripts proceso1..proceso4 y carga_mysql.

Uso:
    py -m streamlit run interfaz_etl.py
"""

import io
import zipfile

import pandas as pd
import streamlit as st

from proceso1_elementos_nulos import periodo_del_archivo, proceso_elementos_nulos
from proceso2_agregacion import proceso_agregacion
from proceso3_normalizacion import proceso_normalizacion
from proceso4_adaptabilidad import (
    generar_dataset_transformado_sql, proceso_adaptabilidad, verificar_sin_vacios,
)
from carga_mysql import SERVIDOR_MYSQL, cargar_a_mysql, leer_sentencias_sql

st.set_page_config(page_title="ETL Kelly's Food", page_icon="🍽️", layout="wide")


# =====================================================================
# UTILITARIOS
# =====================================================================
def cargar_desde_subida(archivo) -> dict:
    """
    Versión de cargar_dataset() para un archivo subido por la interfaz.
    El encabezado real está en la fila 2 (header=1 en pandas, base 0).
    """
    xls = pd.ExcelFile(io.BytesIO(archivo.getvalue()))
    return {
        "Registro Ventas": pd.read_excel(xls, sheet_name="Registro Ventas", header=1),
        "Menu del dia": pd.read_excel(xls, sheet_name="Menú del día", header=1),
        "Gastos": pd.read_excel(xls, sheet_name="Gastos", header=1),
    }


def vista(df: pd.DataFrame, filas: int = 100) -> pd.DataFrame:
    """Copia apta para mostrar en pantalla: nombres de columna y celdas
    como texto (evita problemas con columnas de tipos mezclados)."""
    d = df.head(filas).copy()
    d.columns = [str(c) for c in d.columns]
    return d.astype(str)


def hojas_a_excel(hojas: dict) -> bytes:
    """Convierte un diccionario {nombre: DataFrame} a un Excel en memoria."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        for nombre, df in hojas.items():
            # Excel limita nombres de hoja a 31 caracteres
            df.to_excel(writer, sheet_name=nombre[:31], index=False)
    return buffer.getvalue()


def crear_zip_entidades(entidades_todos: dict, sqls: dict) -> bytes:
    """Empaqueta en un único .zip el Excel de entidades y el .sql de cada
    archivo procesado en el Proceso 4 (omite los que fallaron la verificación
    de calidad, ya que para esos no se generó .sql)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for nombre, entidades in entidades_todos.items():
            info = sqls[nombre]
            if info["problemas"]:
                continue
            zf.writestr(f"kellys_food_entidades_{info['periodo']}.xlsx", hojas_a_excel(entidades))
            zf.writestr(info["ruta"], info["contenido"])
    return buffer.getvalue()


def mostrar_hojas(hojas: dict, clave: str):
    """Muestra cada hoja/entidad en una pestaña."""
    pestanas = st.tabs(list(hojas.keys()))
    for pestana, (nombre, df) in zip(pestanas, hojas.items()):
        with pestana:
            st.caption(f"{len(df)} filas × {len(df.columns)} columnas (se muestran hasta 100)")
            st.dataframe(vista(df), use_container_width=True, key=f"{clave}_{nombre}")


def seleccionar_archivo(clave: str) -> str:
    """Selector del Excel a previsualizar cuando hay varios subidos."""
    nombres = st.session_state["nombres_archivos"]
    if len(nombres) == 1:
        return nombres[0]
    return st.selectbox("Excel a previsualizar", nombres, key=f"sel_{clave}")


CLAVES_PASOS = {
    1: ["p1", "res1"],
    2: ["p2"],
    3: ["p3"],
    4: ["entidades", "sqls"],
    5: ["carga_ok"],
}


def limpiar_desde(paso: int):
    """Invalida los resultados desde el paso indicado en adelante."""
    for n in range(paso, 6):
        for clave in CLAVES_PASOS[n]:
            st.session_state.pop(clave, None)


# =====================================================================
# BARRA LATERAL: ESTADO DEL PIPELINE
# =====================================================================
st.sidebar.title("🍽️ ETL Kelly's Food")
st.sidebar.markdown("**Fase de Transformación + Carga**")
for etiqueta, clave in [
    ("1. Elementos Nulos", "p1"),
    ("2. Agregación de faltantes", "p2"),
    ("3. Normalización", "p3"),
    ("4. Adaptabilidad", "entidades"),
    ("5. Carga (MySQL)", "carga_ok"),
]:
    icono = "✅" if clave in st.session_state else "⬜"
    st.sidebar.markdown(f"{icono} {etiqueta}")
st.sidebar.divider()
if st.sidebar.button("🔄 Reiniciar todo"):
    limpiar_desde(1)
    st.rerun()

st.title("ETL Kelly's Food — Transformación y Carga")

# =====================================================================
# PASO 0: SUBIR LOS EXCELS FUENTE
# =====================================================================
st.header("📂 Excels fuente")
st.markdown(
    "Suba **uno o varios** archivos mensuales `KellysFood_*.xlsx` (hay 44+, "
    "misma estructura). Cada uno debe contener las hojas *Registro Ventas*, "
    "*Menú del día* y *Gastos*."
)
archivos = st.file_uploader("Archivos Excel", type=["xlsx"], accept_multiple_files=True)
archivos = sorted(archivos or [], key=lambda a: a.name)
nombres = [a.name for a in archivos]

# Si cambia el conjunto de archivos, se invalidan los resultados anteriores
if st.session_state.get("nombres_archivos") != nombres:
    limpiar_desde(1)
    st.session_state["nombres_archivos"] = nombres

if not archivos:
    st.info("⬆️ Suba al menos un Excel para habilitar el Proceso 1.")
else:
    st.success(f"{len(archivos)} archivo(s) listos para procesar.")

# =====================================================================
# PROCESO 1: ELEMENTOS NULOS
# =====================================================================
st.header("1. Elementos Nulos")
st.markdown(
    "**Necesita:** los Excels fuente. Trata cada vacío según lo que representa: "
    "elimina filas resumen (TOTAL) y columnas 100% vacías (quincenas, domingos, "
    "derivables), elimina los días de menú sin plato, imputa raciones a `0`, "
    "montos a `0.0` y método de pago a la moda. Las fechas de pago vacías se "
    "quedan vacías (pago inexistente: no generará fila). **Nada se marca con "
    "`Sin_Dato` ni terminará como NULL.**"
)

if st.button("▶ Ejecutar Proceso 1", disabled=not archivos):
    limpiar_desde(1)
    p1, res1 = {}, {}
    for archivo in archivos:
        hojas = cargar_desde_subida(archivo)
        p1[archivo.name], res1[archivo.name] = proceso_elementos_nulos(hojas)
    st.session_state["p1"], st.session_state["res1"] = p1, res1

if "p1" in st.session_state:
    st.success(f"Proceso 1 completado para {len(st.session_state['p1'])} archivo(s).")
    nombre_sel = seleccionar_archivo("p1")
    res = st.session_state["res1"][nombre_sel]
    cols = st.columns(3)
    cols[0].metric("Filas eliminadas (resumen/vacías)",
                   sum(r["filas_eliminadas"] for r in res.values()))
    cols[1].metric("Columnas eliminadas (100% vacías)",
                   sum(r["columnas_eliminadas"] for r in res.values()))
    cols[2].metric("Celdas imputadas (0 / 0.0 / moda)",
                   sum(r["celdas_imputadas"] for r in res.values()))
    mostrar_hojas(st.session_state["p1"][nombre_sel], f"p1_{nombre_sel}")
    st.download_button(
        "⬇️ Descargar evidencia (Proceso 1)",
        data=hojas_a_excel(st.session_state["p1"][nombre_sel]),
        file_name=f"etl_paso1_elementos_nulos_{periodo_del_archivo(st.session_state['p1'][nombre_sel])}.xlsx",
    )

# =====================================================================
# PROCESO 2: AGREGACIÓN DE ELEMENTOS FALTANTES
# =====================================================================
st.header("2. Agregación de elementos faltantes")
st.markdown(
    "**Necesita:** el resultado del Proceso 1. Agrega `id_trabajador` y `Estado` "
    "en *Registro Ventas* e `id_compra` (con el período como prefijo) en *Gastos*. "
    "La identidad es Nombre+Apellido+Teléfono (ya no existe DNI) y los ids se "
    "mantienen entre meses mediante el maestro acumulado `maestro_trabajadores.csv`."
)

if st.button("▶ Ejecutar Proceso 2", disabled="p1" not in st.session_state):
    limpiar_desde(2)
    p2 = {}
    for nombre in st.session_state["nombres_archivos"]:
        hojas = {k: v.copy() for k, v in st.session_state["p1"][nombre].items()}
        p2[nombre] = proceso_agregacion(hojas)
    st.session_state["p2"] = p2

if "p2" in st.session_state:
    st.success("Proceso 2 completado.")
    nombre_sel = seleccionar_archivo("p2")
    hojas_sel = st.session_state["p2"][nombre_sel]
    c1, c2 = st.columns(2)
    c1.metric("Personas en este mes", hojas_sel["Registro Ventas"]["id_trabajador"].nunique())
    c2.metric("id_compra generados", len(hojas_sel["Gastos"]))
    mostrar_hojas(hojas_sel, f"p2_{nombre_sel}")
    st.download_button(
        "⬇️ Descargar evidencia (Proceso 2)",
        data=hojas_a_excel(hojas_sel),
        file_name=f"etl_paso2_agregacion_{periodo_del_archivo(hojas_sel)}.xlsx",
    )

# =====================================================================
# PROCESO 3: NORMALIZACIÓN
# =====================================================================
st.header("3. Normalización")
st.markdown(
    "**Necesita:** el resultado del Proceso 2. Deja cada valor con su tipo "
    "correcto: raciones **enteras**, precios y montos **decimales**, teléfono de "
    "9 dígitos (corrige el `.0`), nombres/platos/conceptos en Tipo Título (sin "
    "sufijo ` (S/)`), método de pago contra catálogo, precios atípicos (10× la "
    "moda) corregidos y fechas de pago en texto (`'5 mar'`) convertidas a fecha "
    "real con el año del período del archivo."
)

if st.button("▶ Ejecutar Proceso 3", disabled="p2" not in st.session_state):
    limpiar_desde(3)
    p3 = {}
    for nombre in st.session_state["nombres_archivos"]:
        hojas = {k: v.copy() for k, v in st.session_state["p2"][nombre].items()}
        p3[nombre] = proceso_normalizacion(hojas)
    st.session_state["p3"] = p3

if "p3" in st.session_state:
    st.success("Proceso 3 completado.")
    nombre_sel = seleccionar_archivo("p3")
    hojas_sel = st.session_state["p3"][nombre_sel]
    metodos = hojas_sel["Registro Ventas"]["Método de pago"].unique()
    precios = hojas_sel["Registro Ventas"]["Precio menú (S/)"].unique()
    c1, c2 = st.columns(2)
    c1.metric("Métodos de pago tras normalizar", len(metodos))
    c2.metric("Precios de menú distintos", len(precios))
    st.caption("Métodos: " + ", ".join(str(m) for m in metodos) +
               " | Precios: " + ", ".join(f"{p:g}" for p in sorted(precios)))
    mostrar_hojas(hojas_sel, f"p3_{nombre_sel}")
    st.download_button(
        "⬇️ Descargar evidencia (Proceso 3)",
        data=hojas_a_excel(hojas_sel),
        file_name=f"etl_paso3_normalizacion_{periodo_del_archivo(hojas_sel)}.xlsx",
    )

# =====================================================================
# PROCESO 4: ADAPTABILIDAD
# =====================================================================
st.header("4. Adaptabilidad")
st.markdown(
    "**Necesita:** el resultado del Proceso 3. Separa la tabla plana en las 20 "
    "entidades del modelo físico: Ración y Compra solo con días con movimiento "
    "(> 0), **Pago con una fila por pago real** (sin fechas vacías), quincenas "
    "derivadas del período del archivo. Verifica que **ningún valor quede vacío** "
    "y entrega el dataset transformado por período: `.xlsx` y `.sql` (DDL `NOT "
    "NULL` + `INSERT`; catálogos con `INSERT IGNORE` para acumular los meses)."
)

if st.button("▶ Ejecutar Proceso 4", disabled="p3" not in st.session_state):
    limpiar_desde(4)
    entidades_todos, sqls = {}, {}
    for nombre in st.session_state["nombres_archivos"]:
        hojas = st.session_state["p3"][nombre]
        periodo = periodo_del_archivo(hojas)
        entidades = proceso_adaptabilidad(dict(hojas))
        problemas = verificar_sin_vacios(entidades)
        info = {"periodo": periodo, "problemas": problemas}
        if not problemas:
            ruta = generar_dataset_transformado_sql(entidades, periodo)
            with open(ruta, "r", encoding="utf-8") as f:
                info["ruta"] = ruta
                info["contenido"] = f.read()
        entidades_todos[nombre] = entidades
        sqls[nombre] = info
    st.session_state["entidades"] = entidades_todos
    st.session_state["sqls"] = sqls

if "entidades" in st.session_state:
    entidades_todos = st.session_state["entidades"]
    sqls_todos = st.session_state["sqls"]
    listos = [n for n, i in sqls_todos.items() if not i["problemas"]]

    if len(entidades_todos) > 1:
        st.download_button(
            f"📦 Descargar todos ({len(listos)}/{len(entidades_todos)} archivo(s) listos) — .zip",
            data=crear_zip_entidades(entidades_todos, sqls_todos),
            file_name="kellys_food_entidades_todos.zip",
            type="primary",
            disabled=not listos,
        )
        if len(listos) < len(entidades_todos):
            st.caption(
                "Los archivos que fallaron la verificación de calidad no se "
                "incluyen en el .zip (no tienen .sql generado)."
            )

    nombre_sel = seleccionar_archivo("p4")
    entidades = entidades_todos[nombre_sel]
    info = sqls_todos[nombre_sel]

    if info["problemas"]:
        st.error("Verificación de calidad FALLÓ para este archivo — no se generó el .sql:")
        for p in info["problemas"]:
            st.write(f"- {p}")
    else:
        con_datos = {n: df for n, df in entidades.items() if len(df) > 0}
        vacias = [n for n, df in entidades.items() if len(df) == 0]
        st.success(
            f"Proceso 4 completado (período {info['periodo']}): {len(entidades)} entidades, "
            "verificación de calidad OK (0 valores vacíos)."
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Entidades con datos", len(con_datos))
        c2.metric("Entidades vacías (sin fuente)", len(vacias))
        c3.metric("INSERT generados", info["contenido"].count("INSERT"))

        st.subheader("Filas por entidad")
        st.dataframe(
            pd.DataFrame({"Entidad": list(entidades.keys()),
                          "Filas": [len(df) for df in entidades.values()]}),
            use_container_width=True,
        )

        st.subheader("Vista de entidades con datos")
        mostrar_hojas(con_datos, f"p4_{nombre_sel}")

        with st.expander(f"👁️ Vista previa de {info['ruta']} (primeras 120 líneas)"):
            st.code("\n".join(info["contenido"].splitlines()[:120]), language="sql")

        c1, c2 = st.columns(2)
        c1.download_button(
            f"⬇️ kellys_food_entidades_{info['periodo']}.xlsx",
            data=hojas_a_excel(entidades),
            file_name=f"kellys_food_entidades_{info['periodo']}.xlsx",
            type="primary",
        )
        c2.download_button(
            f"⬇️ {info['ruta']}",
            data=info["contenido"].encode("utf-8"),
            file_name=info["ruta"],
            type="primary",
        )

# =====================================================================
# PASO 5: FASE DE CARGA (MySQL)
# =====================================================================
st.header("5. Fase de Carga (MySQL)")
st.markdown(
    "**Necesita:** los datasets transformados (`.sql`) del Proceso 4. Esta fase "
    "**no genera datos ni SQL**: valida que ningún INSERT contenga NULL y ejecuta "
    "cada script contra el servidor MySQL en una transacción por archivo (ROLLBACK "
    "si algo falla). Todos los meses se cargan a la **misma** base de datos; los "
    "catálogos van con `INSERT IGNORE`, por lo que acumular meses no duplica."
)

sqls_listos = {
    n: i for n, i in st.session_state.get("sqls", {}).items() if not i.get("problemas")
}
if not sqls_listos:
    st.info("Ejecute el Proceso 4 para obtener los datasets transformados.")
else:
    total_sentencias = sum(len(leer_sentencias_sql(i["ruta"])) for i in sqls_listos.values())
    c1, c2 = st.columns(2)
    c1.metric("Archivos .sql listos", len(sqls_listos))
    c2.metric("Sentencias totales a ejecutar", total_sentencias)
    st.caption(
        "Alternativa: descargue los .sql en el Proceso 4 y ejecútelos en MySQL "
        "Workbench, o complete los datos del servidor y cargue directo desde aquí."
    )
    with st.form("form_mysql"):
        c1, c2, c3 = st.columns(3)
        host = c1.text_input("Host", value=SERVIDOR_MYSQL["host"])
        puerto = c2.number_input("Puerto", min_value=1, max_value=65535,
                                 value=SERVIDOR_MYSQL["puerto"])
        base_datos = c3.text_input("Base de datos", value=SERVIDOR_MYSQL["base_datos"])
        c4, c5 = st.columns(2)
        usuario = c4.text_input("Usuario", value=SERVIDOR_MYSQL["usuario"])
        contrasena = c5.text_input("Contraseña", type="password",
                                   value=SERVIDOR_MYSQL["contraseña"])
        if st.form_submit_button("🚀 Ejecutar carga de todos los archivos"):
            servidor = {
                "host": host, "puerto": int(puerto), "usuario": usuario,
                "contraseña": contrasena, "base_datos": base_datos,
            }
            exitos, fallos = [], []
            for nombre, info in sorted(sqls_listos.items(), key=lambda x: x[1]["periodo"]):
                try:
                    cargar_a_mysql(info["ruta"], servidor)
                    exitos.append(info["periodo"])
                except Exception as e:
                    fallos.append(f"{info['periodo']}: {e}")
            if exitos:
                st.session_state["carga_ok"] = True
                st.success(
                    f"Carga completada para {len(exitos)} período(s) "
                    f"({', '.join(exitos)}) en '{base_datos}' de {host}:{int(puerto)}. "
                    "BD entregada limpia: 0 valores NULL."
                )
            for f in fallos:
                st.error(f"Carga fallida (se hizo ROLLBACK) — {f}")
