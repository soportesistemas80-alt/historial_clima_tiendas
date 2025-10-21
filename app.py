import os
import requests
import json
import csv  # Necesario para DictWriter
# CORRECCIÓN: Imports de IO y Flask para Streaming
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, send_file, Response, stream_with_context
# ReportLab para generar PDF
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, SimpleDocTemplate

# Cargar variables de entorno
load_dotenv()
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'default_secret_key')

# =========================================================================
# 1. DATOS Y CONSTANTES
# =========================================================================
TIENDAS_MAP = {
    # 14 Tiendas Shopping
    "Shopping 1": (7.059683654849175, -73.86856975814536), "Shopping 2": (7.059768491566754, -73.8685599147996),
    "Shopping 3": (7.059715996100198, -73.86874898531704), "Shopping 4": (10.390839993695229, -75.47865011114159),
    "Shopping 5": (11.243198751367304, -74.2100645944834), "Shopping 6": (7.073912014064684, -73.16807685168257),
    "Shopping 7": (6.984873261792733, -73.05045422597566), "Shopping 8": (7.11908224576338, -73.12499126341542),
    "Shopping 9": (7.063583168903841, -73.08545269225122), "Shopping 10": (10.41616320505202, -75.52364301117757),
    "Shopping 11": (10.401998109508904, -75.45978355813655), "Shopping 13": (10.98819210780722, -74.77712600375727),
    "Shopping 18": (5.34731430225259, -72.38879057865113), "Shopping 22": (7.888542992988793, -72.50430970680809),
    # 28 Tiendas Templo
    "Templo 1": (3.452419735639639, -76.52978324939568), "Templo 2": (3.4522853774177134, -76.529898454676),
    "Templo 4": (4.538016339723241, -75.67406369068367), "Templo 5": (3.368715416264726, -76.52363767081717),
    "Templo 6": (3.8896123736819495, -77.07750078080207), "Templo 8": (3.4856000188429475, -76.4975345879099),
    "Templo 9": (3.5270710128823204, -76.29822708320997), "Templo 10": (4.8142852710304105, -75.6937757603496),
    "Templo 11": (3.4499690950433006, -76.53007453971138), "Templo 13": (3.263903562976194, -76.53926833718572),
    "Templo 14": (5.068244286496738, -75.51506847545048), "Templo 16": (4.085044358531649, -76.19849390561589),
    "Templo 17": (3.007623327632023, -76.48390139486145), "Templo 18": (3.4554591168890325, -76.51771937930089),
    "Templo 19": (4.444076234816372, -75.24018041918487), "Templo 20": (3.4038129971746423, -76.51205234961651),
    "Templo 23": (3.5846217572598627, -76.49387927985713), "Templo 24": (3.9009535681896073, -76.29992062234696),
    "Templo 26": (3.431348795529181, -76.48092776935766), "Templo 27": (3.3253938372763114, -76.23473751945423),
    "Templo 28": (2.4405990552966688, -76.60645440231042), "Templo 29": (3.3763857352126685, -76.5474364784691),
    "Templo 30": (3.4340788460477434, -76.53520635714665), "Templo 31": (3.4847382695031377, -76.49658666150108),
    "Templo 32": (3.4156413599943836, -76.5473246406779), "Templo 33": (2.9274842823407994, -75.28997287249801),
    "Templo 35": (3.540330538147451, -76.31075454232808), "Templo 36": (4.750430032121445, -75.91210409550338),
}


def agrupar_y_ordenar_tiendas(tiendas_map):
    agrupadas = {'Shopping': [], 'Templo': []}
    for nombre in tiendas_map.keys():
        clave = "Shopping" if nombre.startswith("Shopping") else ("Templo" if nombre.startswith("Templo") else None)
        if clave:
            num_str = nombre.split()[-1]
            try:
                num = int(''.join(filter(str.isdigit, num_str)))
            except ValueError:
                num = 9999
            agrupadas[clave].append((num, nombre))
    agrupadas['Shopping'].sort();
    agrupadas['Templo'].sort()
    return {'Shopping': [n for _, n in agrupadas['Shopping']], 'Templo': [n for _, n in agrupadas['Templo']]}


TIENDAS_AGRUPADAS = agrupar_y_ordenar_tiendas(TIENDAS_MAP)

# Lista de festivos de Colombia para 2024
DIAS_FESTIVOS_COLOMBIA_2024 = {
    '2024-01-01': 'Año Nuevo', '2024-01-08': 'Día de Reyes Magos', '2024-03-25': 'Día de San José',
    '2024-03-28': 'Jueves Santo', '2024-03-29': 'Viernes Santo', '2024-05-01': 'Día del Trabajo',
    '2024-05-13': 'Día de la Ascensión', '2024-06-03': 'Corpus Christi', '2024-06-10': 'Sagrado Corazón de Jesús',
    '2024-07-01': 'San Pedro y San Pablo', '2024-07-20': 'Día de la Independencia', '2024-08-07': 'Batalla de Boyacá',
    '2024-08-19': 'Asunción de la Virgen', '2024-10-14': 'Día de la Raza', '2024-11-04': 'Día de Todos los Santos',
    '2024-11-11': 'Independencia de Cartagena', '2024-12-08': 'Inmaculada Concepción', '2024-12-25': 'Navidad'
}
DIAS_SEMANA_ES = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}


# =========================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO Y API (USANDO OPEN-METEO)
# =========================================================================

def obtener_historial_climatico(lat, lon):
    fecha_inicio = "2024-01-01"
    hoy = datetime.now()
    fecha_fin_dt = hoy - timedelta(days=5)
    fecha_fin = fecha_fin_dt.strftime('%Y-%m-%d')

    # Error de rango
    try:
        if datetime.strptime(fecha_inicio, '%Y-%m-%d') > datetime.strptime(fecha_fin, '%Y-%m-%d'):
            return {"error": f"Rango de fechas inválido: {fecha_inicio} a {fecha_fin}. No hay datos suficientes."}
    except ValueError:
        return {"error": "Error interno al procesar las fechas. Verifique el formato."}

    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={lat}&longitude={lon}&start_date={fecha_inicio}&end_date={fecha_fin}"
           f"&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
           f"&temperature_unit=celsius&wind_speed_unit=kmh&precipitation_unit=mm&timezone=auto")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        daily_data = data.get('daily', {})
        times = daily_data.get('time', [])
        if not times: return []
        datos_procesados = []

        def map_wmo_code(code):
            if code is None: return 'Condiciones Variadas'
            if code == 0: return "Despejado"
            if 1 <= code <= 3: return "Mayormente Despejado a Parcialmente Nublado"
            if 45 <= code <= 48: return "Niebla / Escarcha"
            if 51 <= code <= 55: return "Llovizna"
            if 61 <= code <= 65: return "Lluvia Moderada"
            if 80 <= code <= 82: return "Aguaceros Fuertes"
            if 95 <= code <= 96: return "Tormenta"
            return "Condiciones Variadas"

        tmax_list, tmin_list, precip_list, viento_list, code_list = (daily_data.get(k, []) for k in
                                                                     ['temperature_2m_max', 'temperature_2m_min',
                                                                      'precipitation_sum', 'wind_speed_10m_max',
                                                                      'weather_code'])

        for i, fecha_str in enumerate(times):
            try:
                fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d')
                nombre_dia = DIAS_SEMANA_ES.get(fecha_dt.weekday(), '-')
            except ValueError:
                nombre_dia = '-'

            es_festivo = 'Sí' if fecha_str in DIAS_FESTIVOS_COLOMBIA_2024 else 'No'
            nombre_festivo = DIAS_FESTIVOS_COLOMBIA_2024.get(fecha_str, '')

            datos_dia = {
                'fecha': fecha_str,
                'nombre_dia': nombre_dia,  # <-- CAMPO NUEVO
                'es_festivo': es_festivo,  # <-- CAMPO NUEVO
                'nombre_festivo': nombre_festivo,  # <-- CAMPO NUEVO
                'tmax': tmax_list[i] if i < len(tmax_list) else None,
                'tmin': tmin_list[i] if i < len(tmin_list) else None,
                'precipitacion_mm': precip_list[i] if i < len(precip_list) else None,
                'viento_kmh': viento_list[i] if i < len(viento_list) else None,
                'nubosidad_perc': 50,
                'condiciones': map_wmo_code(code_list[i] if i < len(code_list) else None)
            }
            datos_procesados.append(datos_dia)
        return datos_procesados

    except requests.exceptions.HTTPError as e:
        return {
            "error": f"Error API HTTP: {e.response.status_code}. Mensaje: {response.text}. Revisa la URL de la API."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Error de conexión a la API: {e}. Revisa tu conexión a internet."}
    except Exception as e:
        return {"error": f"Error inesperado procesando datos: {e}"}


def aplicar_filtros(datos, filtros):
    if not datos or "error" in datos: return datos
    datos_filtrados = []
    tmax_min_val = float(filtros.get('tmax_min')) if filtros.get('tmax_min') else None
    precip_min_val = float(filtros.get('precip_min')) if filtros.get('precip_min') else None
    viento_min_val = float(filtros.get('viento_min')) if filtros.get('viento_min') else None
    condiciones_filtro_val = filtros.get('condiciones_filtro')

    for dia in datos:
        incluir = True
        tmax = dia.get('tmax', -100)
        precip = dia.get('precipitacion_mm', 0)
        viento = dia.get('viento_kmh', 0)
        condicion_actual = dia.get('condiciones')

        if condiciones_filtro_val and condiciones_filtro_val != "TODAS":
            if condicion_actual != condiciones_filtro_val: incluir = False
        if incluir and tmax_min_val is not None and tmax < tmax_min_val: incluir = False
        if incluir and precip_min_val is not None and precip < precip_min_val: incluir = False
        if incluir and viento_min_val is not None and viento < viento_min_val: incluir = False

        if incluir: datos_filtrados.append(dia)
    return datos_filtrados


def calcular_ranking_anual(tiendas_map):
    ranking = []
    fecha_inicio = "2024-01-01"
    hoy = datetime.now()
    fecha_fin = (hoy - timedelta(days=5)).strftime('%Y-%m-%d')

    for nombre, (lat, lon) in tiendas_map.items():
        datos_crudos = obtener_historial_climatico(lat, lon)
        if isinstance(datos_crudos, dict) and "error" in datos_crudos:
            print(f"Error al obtener datos para {nombre}: {datos_crudos['error']}")
            ranking.append({'tienda': nombre, 'tmax_promedio': None, 'lat': lat, 'lon': lon})
            continue
        if datos_crudos:
            tmax_validos = [dia['tmax'] for dia in datos_crudos if dia.get('tmax') is not None]
            avg_tmax = sum(tmax_validos) / len(tmax_validos) if tmax_validos else None
            ranking.append(
                {'tienda': nombre, 'tmax_promedio': round(avg_tmax, 2) if avg_tmax is not None else None, 'lat': lat,
                 'lon': lon})
        else:
            ranking.append({'tienda': nombre, 'tmax_promedio': None, 'lat': lat, 'lon': lon})

    ranking.sort(key=lambda x: x['tmax_promedio'] if x['tmax_promedio'] is not None else -float('inf'), reverse=True)
    return {'ranking': ranking, 'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin}


# =========================================================================
# 3. RUTAS FLASK
# =========================================================================

@app.route("/", methods=["GET", "POST"])
def historial_detallado():
    tienda_seleccionada = None
    datos_historial = None
    error_message = None
    filtros_aplicados = {'tmax_min': '', 'precip_min': '', 'viento_min': '', 'condiciones_filtro': 'TODAS'}

    if request.method == "POST":
        tienda_seleccionada = request.form.get("tienda")
        filtros_aplicados = {
            'tmax_min': request.form.get("tmax_min", ""),
            'precip_min': request.form.get("precip_min", ""),
            'viento_min': request.form.get("viento_min", ""),
            'condiciones_filtro': request.form.get("condiciones_filtro", "TODAS"),
        }

        if tienda_seleccionada in TIENDAS_MAP:
            lat, lon = TIENDAS_MAP[tienda_seleccionada]
            datos_crudos = obtener_historial_climatico(lat, lon)  # Ahora incluye día y festivo
            if isinstance(datos_crudos, dict) and "error" in datos_crudos:
                error_message = datos_crudos["error"]
            else:
                datos_historial = aplicar_filtros(datos_crudos, filtros_aplicados)
        else:
            error_message = "Tienda seleccionada no válida."

    if datos_historial is None and 'tienda' in request.form:
        tienda_seleccionada = request.form.get("tienda")

    return render_template("index.html",
                           tiendas_agrupadas=TIENDAS_AGRUPADAS,
                           tienda_seleccionada=tienda_seleccionada,
                           datos_historial=datos_historial,
                           error_message=error_message,
                           filtros_aplicados=filtros_aplicados,
                           seccion_activa="historial")


@app.route("/ranking", methods=["GET"])
def ranking_anual():
    ranking_data = calcular_ranking_anual(TIENDAS_MAP)
    return render_template("index.html",
                           tiendas_agrupadas=TIENDAS_AGRUPADAS,
                           ranking_data=ranking_data,
                           seccion_activa="ranking")


# =========================================================================
# FUNCIONES DE EXPORTACIÓN (CORREGIDA CON STREAMING, BOM y Delimitadores)
# =========================================================================

def generate_csv_rows(datos, delimiter=',', include_bom=False):
    """Generador que produce filas CSV, maneja BOM, delimitador y encabezados."""
    # StringIO para escribir el texto línea por línea
    string_io = StringIO(newline='')

    # Encabezados amigables. Se añade Día y Festivo
    key_mapping_friendly = {
        'fecha': 'Fecha', 'nombre_dia': 'Día', 'es_festivo': 'Festivo', 'nombre_festivo': 'Nombre Festivo',
        'tmax': 'T. Máx (°C)', 'tmin': 'T. Mín (°C)', 'precipitacion_mm': 'Lluvia (mm)',
        'viento_kmh': 'Viento (km/h)', 'nubosidad_perc': 'Nubosidad (%)', 'condiciones': 'Condiciones'
    }
    headers_keys = list(key_mapping_friendly.keys())

    writer = csv.DictWriter(string_io, fieldnames=headers_keys, extrasaction='ignore', delimiter=delimiter)

    # 1. Escribir BOM si es para Excel (ayuda con la codificación)
    if include_bom:
        # El BOM debe ser codificado en UTF-8 y yield debe ser un string de bytes
        yield u'\ufeff'.encode('utf-8')

        # 2. Escribir Encabezado
    # Crear un diccionario con los nombres amigables
    friendly_headers = {k: key_mapping_friendly[k] for k in headers_keys}
    writer.writerow(friendly_headers)
    string_io.seek(0)

    # Yield debe ser un string de bytes si se incluyó el BOM (para mantener la consistencia)
    yield string_io.read().encode('utf-8') if include_bom else string_io.read()
    string_io.seek(0);
    string_io.truncate(0)

    # 3. Escribir Datos (fila por fila)
    for row_data in datos:
        # csv.DictWriter se encarga del delimitador
        writer.writerow(row_data)
        string_io.seek(0)
        yield string_io.read().encode('utf-8') if include_bom else string_io.read()
        string_io.seek(0);
        string_io.truncate(0)


@app.route("/exportar_datos/<formato>", methods=["POST"])
def exportar_datos(formato):
    """Genera y envía (STREAMING) los datos filtrados como un archivo CSV o XLSX (simulado con CSV)."""
    datos_export_json = request.form.get('datos_export_json')
    tienda_nombre = request.form.get('tienda_nombre')

    try:
        datos = json.loads(datos_export_json)
    except json.JSONDecodeError:
        return f"Error decodificando datos para {formato}.", 400
    if not datos:
        return "No hay datos para exportar.", 400

    # Valores por defecto para CSV estándar
    delimiter = ',';
    mimetype = 'text/csv; charset=utf-8';  # Mimetype para descarga
    download_ext = '.csv';
    include_bom = False
    is_excel = False

    if formato.lower() == 'excel':
        delimiter = ';';  # Delimitador de punto y coma para Excel regional
        download_ext = '_EXCEL.csv';  # Se usa .csv para que Excel lo abra, pero se nombra para identificar
        include_bom = True;  # BOM para manejar acentos en Excel
        is_excel = True
        # Si se usa BOM, el mimetype debe ser más genérico o binario
        mimetype = 'application/octet-stream'

    filename = f"Clima_{tienda_nombre}_{datetime.now().strftime('%Y%m%d')}{download_ext}"

    # Si se incluye BOM, el generador produce bytes, y el mimetype debe reflejarlo
    generator_output = stream_with_context(generate_csv_rows(datos, delimiter, include_bom))

    # Si se incluye BOM, usamos Response(..., mimetype=binario)
    if is_excel:
        response = Response(generator_output, mimetype=mimetype)
    # Si es CSV estándar, usamos Response(..., mimetype=texto)
    else:
        # Aseguramos que se envía como texto si no se usó BOM (no es binario)
        response = Response(generator_output, mimetype=mimetype)

    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'

    return response


# ... (La ruta generar_pdf no necesita cambios)
@app.route("/generar_pdf", methods=["POST"])
def generar_pdf():
    datos_pdf_json = request.form.get('datos_pdf_json')
    tienda_nombre = request.form.get('tienda_nombre')
    try:
        datos_historial = json.loads(datos_pdf_json)
    except json.JSONDecodeError:
        return "Error al decodificar los datos para PDF.", 400
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, title=f"Reporte Climático {tienda_nombre}")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Data', fontSize=10, leading=12))
    story = []
    story.append(Paragraph(f"<b>Reporte Climático Detallado para: {tienda_nombre}</b>", styles['Heading1']))
    story.append(
        Paragraph(f"Rango de fechas consultado: Desde el 1 de enero de 2024 hasta hace 5 días.", styles['Normal']))
    story.append(Paragraph(f"Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
    story.append(Paragraph(f"Días incluidos en el reporte (tras filtros): {len(datos_historial)}", styles['Normal']))
    story.append(Paragraph("<br/>", styles['Normal']))

    for dia in datos_historial:
        # Añadir Día, Festivo y Nombre Festivo al PDF
        festivo_info = f" (Día Festivo: {dia.get('nombre_festivo')})" if dia.get('es_festivo') == 'Sí' else ""
        dia_semana = dia.get('nombre_dia', '')
        linea = (
            f"<b>Fecha:</b> {dia.get('fecha')} ({dia_semana}){festivo_info} | <b>T Max/Min:</b> {dia.get('tmax')}/{dia.get('tmin')} °C | "
            f"<b>Lluvia:</b> {dia.get('precipitacion_mm')} mm | <b>Viento:</b> {dia.get('viento_kmh')} km/h | "
            f"<b>Condiciones:</b> {dia.get('condiciones', 'N/A')}")
        story.append(Paragraph(linea, styles['Data']))

    doc.build(story)
    buffer.seek(0)
    return send_file(
        buffer, as_attachment=True,
        download_name=f"Reporte_Climatico_{tienda_nombre}_{datetime.now().strftime('%Y%m%d')}.pdf",
        mimetype='application/pdf'
    )


if __name__ == "__main__":
    app.run(debug=True)