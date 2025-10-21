import os
import requests
import json
import csv
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, send_file, Response, stream_with_context
# ReportLab para generar PDF
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, SimpleDocTemplate
# Importación necesaria para generar XLSX
import xlsxwriter

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

# La lista de festivos ya no se utiliza para el procesamiento, pero se deja por si acaso.
DIAS_FESTIVOS_COLOMBIA_2024 = {
    '2024-01-01': 'Año Nuevo', '2024-01-08': 'Día de Reyes Magos', '2024-03-25': 'Día de San José',
    '2024-03-28': 'Jueves Santo', '2024-03-29': 'Viernes Santo', '2024-05-01': 'Día del Trabajo',
    '2024-05-13': 'Día de la Ascensión', '2024-06-03': 'Corpus Christi', '2024-06-10': 'Sagrado Corazón de Jesús',
    '2024-07-01': 'San Pedro y San Pablo', '2024-07-20': 'Día de la Independencia', '2024-08-07': 'Batalla de Boyacá',
    '2024-08-19': 'Asunción de la Virgen', '2024-10-14': 'Día de la Raza', '2024-11-04': 'Día de Todos los Santos',
    '2024-11-11': 'Independencia de Cartagena', '2024-12-08': 'Inmaculada Concepción', '2024-12-25': 'Navidad'
}
DIAS_SEMANA_ES = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}


def agrupar_y_ordenar_tiendas(tiendas_map):
    """Agrupa las tiendas por su tipo (Shopping, Templo, etc.) y las ordena alfabéticamente."""
    agrupadas = {}
    for nombre in sorted(tiendas_map.keys()):
        # Asume que el tipo es la primera palabra del nombre (Shopping, Templo)
        tipo = nombre.split()[0]
        if tipo not in agrupadas:
            agrupadas[tipo] = []
        agrupadas[tipo].append(nombre)
    return agrupadas


TIENDAS_AGRUPADAS = agrupar_y_ordenar_tiendas(TIENDAS_MAP)


# =========================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO Y API (CON FILTRO DE AÑO)
# =========================================================================

def obtener_historial_climatico(lat, lon, año):
    """Obtiene el historial climático desde el 1 de enero del año especificado
    hasta 5 días antes de hoy."""

    try:
        año_int = int(año)
    except ValueError:
        return {"error": "El año proporcionado no es válido."}

    fecha_inicio = f"{año_int}-01-01"

    hoy = datetime.now()
    fecha_fin_dt = hoy - timedelta(days=5)
    fecha_fin = fecha_fin_dt.strftime('%Y-%m-%d')

    # Ajuste de rango de fechas si el año consultado es posterior al actual
    if datetime.strptime(fecha_inicio, '%Y-%m-%d') > fecha_fin_dt:
        if año_int == hoy.year:
            # Si es el año actual, ajusta la fecha fin a lo disponible
            pass
        else:
            # Si el año es futuro o el rango es inválido, retorna error
            return {"error": f"Rango de fechas inválido. No hay datos disponibles para el año {año_int}."}

    # Si el año consultado es anterior al actual, se usa el fin de año del año consultado
    if año_int < hoy.year:
        fecha_fin = f"{año_int}-12-31"

    # Límite superior para el año de consulta para evitar errores con años muy lejanos
    if año_int < 2015 or año_int > hoy.year:
        return {"error": f"Consulta limitada al rango 2015-{hoy.year}."}

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

            # >>> Lógica de festivos eliminada <<<

            datos_dia = {
                'fecha': fecha_str,
                'nombre_dia': nombre_dia,
                # 'es_festivo' y 'nombre_festivo' eliminados
                'tmax': tmax_list[i] if i < len(tmax_list) else None,
                'tmin': tmin_list[i] if i < len(tmin_list) else None,
                'precipitacion_mm': precip_list[i] if i < len(precip_list) else None,
                'viento_kmh': viento_list[i] if i < len(viento_list) else None,
                'nubosidad_perc': 50,  # Dato simulado
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
    año_base = 2024

    for nombre, (lat, lon) in tiendas_map.items():
        datos_crudos = obtener_historial_climatico(lat, lon, año_base)
        if isinstance(datos_crudos, dict) and "error" in datos_crudos:
            print(f"Error al obtener datos para {nombre}: {datos_crudos['error']}")
            ranking.append({'tienda': nombre, 'tmax_promedio': None, 'lat': lat, 'lon': lon, 'tipo': nombre.split()[0]})
            continue
        if datos_crudos:
            tmax_validos = [dia['tmax'] for dia in datos_crudos if dia.get('tmax') is not None]
            avg_tmax = sum(tmax_validos) / len(tmax_validos) if tmax_validos else None
            ranking.append(
                {'tienda': nombre, 'tmax_promedio': round(avg_tmax, 2) if avg_tmax is not None else None, 'lat': lat,
                 'lon': lon, 'tipo': nombre.split()[0]})
        else:
            ranking.append({'tienda': nombre, 'tmax_promedio': None, 'lat': lat, 'lon': lon, 'tipo': nombre.split()[0]})

    ranking.sort(key=lambda x: x['tmax_promedio'] if x['tmax_promedio'] is not None else -float('inf'), reverse=True)
    return {'ranking': ranking, 'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin}


# =========================================================================
# 3. RUTAS FLASK (CON FILTRO DE AÑO)
# =========================================================================

@app.route("/", methods=["GET", "POST"])
def historial_detallado():
    tienda_seleccionada = None
    datos_historial = None
    error_message = None

    año_actual = datetime.now().year
    # Por defecto, establece el año de consulta en el año actual
    filtros_aplicados = {
        'tmax_min': '', 'precip_min': '', 'viento_min': '',
        'condiciones_filtro': 'TODAS',
        'año_filtro': str(año_actual)
    }

    if request.method == "POST":
        tienda_seleccionada = request.form.get("tienda")
        # Aseguramos que el año_filtro se recupere correctamente del formulario
        filtros_aplicados = {
            'tmax_min': request.form.get("tmax_min", ""),
            'precip_min': request.form.get("precip_min", ""),
            'viento_min': request.form.get("viento_min", ""),
            'condiciones_filtro': request.form.get("condiciones_filtro", "TODAS"),
            'año_filtro': request.form.get("año_filtro", str(año_actual)),
        }

        if tienda_seleccionada in TIENDAS_MAP:
            lat, lon = TIENDAS_MAP[tienda_seleccionada]
            año_consulta = filtros_aplicados['año_filtro']

            # Pasar el año a la función
            datos_crudos = obtener_historial_climatico(lat, lon, año_consulta)

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
                           año_actual=año_actual,
                           seccion_activa="historial")


@app.route("/ranking", methods=["GET"])
def ranking_anual():
    ranking_data = calcular_ranking_anual(TIENDAS_MAP)
    return render_template("index.html",
                           tiendas_agrupadas=TIENDAS_AGRUPADAS,
                           ranking_data=ranking_data,
                           seccion_activa="ranking")


# =========================================================================
# 4. FUNCIONES DE EXPORTACIÓN (CSV, XLSX, PDF)
# =========================================================================

# Función de generador para CSV (stream)
def generate_csv_rows(datos_historial):
    """Generador que produce filas CSV a partir de datos de historial."""

    # Define los encabezados para el CSV/Excel (sin columnas de festivos)
    headers = [
        'Fecha', 'Dia',
        'T Max (°C)', 'T Min (°C)', 'Precipitacion (mm)',
        'Viento Max (km/h)', 'Condiciones', 'Nubosidad (%)'
    ]

    # 1. Yield the header row
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    yield output.getvalue()

    # 2. Yield data rows
    for dia in datos_historial:
        output.seek(0)
        output.truncate(0)

        row = [
            dia.get('fecha', ''),
            dia.get('nombre_dia', ''),
            # 'es_festivo' y 'nombre_festivo' eliminados
            dia.get('tmax', ''),
            dia.get('tmin', ''),
            dia.get('precipitacion_mm', ''),
            dia.get('viento_kmh', ''),
            dia.get('condiciones', ''),
            dia.get('nubosidad_perc', '')
        ]
        writer.writerow(row)
        yield output.getvalue()


# Función para generar XLSX (Excel)
def exportar_xlsx_stream(tienda_nombre, año_consulta, datos_historial):
    """Genera un archivo XLSX en memoria y lo devuelve como stream."""
    buffer = BytesIO()
    workbook = xlsxwriter.Workbook(buffer, {'in_memory': True})
    worksheet = workbook.add_worksheet(f'Clima_{tienda_nombre}')

    # Encabezados (sin columnas de festivos)
    headers = [
        'Fecha', 'Día',
        'T Max (°C)', 'T Min (°C)', 'Precipitación (mm)',
        'Viento Max (km/h)', 'Condiciones', 'Nubosidad (%)'
    ]

    # Formato de encabezado
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})

    # Escribir encabezados
    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header, header_format)

    # Escribir datos
    row_num = 1
    for dia in datos_historial:
        col_num = 0
        # Columna 0: Fecha
        worksheet.write(row_num, col_num, dia.get('fecha', ''))
        col_num += 1
        # Columna 1: Día
        worksheet.write(row_num, col_num, dia.get('nombre_dia', ''))
        col_num += 1
        # Columnas de Festivos eliminadas (continuamos desde T Max)
        # Columna 2: T Max
        worksheet.write(row_num, col_num, dia.get('tmax', ''))
        col_num += 1
        # Columna 3: T Min
        worksheet.write(row_num, col_num, dia.get('tmin', ''))
        col_num += 1
        # Columna 4: Precipitación
        worksheet.write(row_num, col_num, dia.get('precipitacion_mm', ''))
        col_num += 1
        # Columna 5: Viento
        worksheet.write(row_num, col_num, dia.get('viento_kmh', ''))
        col_num += 1
        # Columna 6: Condiciones
        worksheet.write(row_num, col_num, dia.get('condiciones', ''))
        col_num += 1
        # Columna 7: Nubosidad
        worksheet.write(row_num, col_num, dia.get('nubosidad_perc', ''))
        col_num += 1
        row_num += 1

    # AJUSTE: Usar worksheet.autofit() para ajustar todas las columnas
    worksheet.autofit()

    workbook.close()
    buffer.seek(0)
    return buffer


@app.route("/exportar_datos/<formato>", methods=["POST"])
def exportar_datos(formato):
    """Maneja la exportación a CSV y XLSX."""
    datos_export_json = request.form.get('datos_export_json')
    tienda_nombre = request.form.get('tienda_nombre')
    año_consulta = request.form.get('año_consulta')

    try:
        datos_historial = json.loads(datos_export_json)
    except json.JSONDecodeError:
        return "Error al decodificar los datos para exportación.", 400

    if not datos_historial:
        return "No hay datos para exportar después de aplicar los filtros.", 404

    filename_base = f"Clima_{tienda_nombre}_{año_consulta}_{datetime.now().strftime('%Y%m%d')}"

    if formato == 'csv':
        response = Response(
            stream_with_context(generate_csv_rows(datos_historial)),
            mimetype='text/csv'
        )
        response.headers['Content-Disposition'] = f'attachment; filename={filename_base}.csv'
        return response

    elif formato == 'excel':
        buffer = exportar_xlsx_stream(tienda_nombre, año_consulta, datos_historial)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"{filename_base}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    return "Formato de exportación no soportado.", 400


@app.route("/generar_pdf", methods=["POST"])
def generar_pdf():
    # Obtener el año y el rango de fechas para el PDF
    datos_pdf_json = request.form.get('datos_pdf_json')
    tienda_nombre = request.form.get('tienda_nombre')
    año_consulta = request.form.get('año_consulta')

    try:
        datos_historial = json.loads(datos_pdf_json)
    except json.JSONDecodeError:
        return "Error al decodificar los datos para PDF.", 400

    # Calcular el rango de fechas real
    if datos_historial:
        fecha_inicio_reporte = datos_historial[0]['fecha']
        fecha_fin_reporte = datos_historial[-1]['fecha']
    else:
        # Usamos el año_consulta directamente si no hay datos
        fecha_inicio_reporte = f"01-01-{año_consulta}"
        fecha_fin_reporte = "Sin datos"

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, title=f"Reporte Climático {tienda_nombre} - {año_consulta}")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Data', fontSize=10, leading=12))
    story = []

    story.append(
        Paragraph(f"<b>Reporte Climático Detallado para: {tienda_nombre} ({año_consulta})</b>", styles['Heading1']))
    story.append(
        Paragraph(f"Rango de fechas consultado: Desde {fecha_inicio_reporte} hasta {fecha_fin_reporte}.",
                  styles['Normal']))
    story.append(Paragraph(f"Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
    story.append(Paragraph(f"Días incluidos en el reporte (tras filtros): {len(datos_historial)}", styles['Normal']))
    story.append(Paragraph("<br/>", styles['Normal']))

    for dia in datos_historial:
        # La información de festivos ha sido eliminada del PDF
        dia_semana = dia.get('nombre_dia', '')
        linea = (
            f"<b>Fecha:</b> {dia.get('fecha')} ({dia_semana}) | <b>T Max/Min:</b> {dia.get('tmax')}/{dia.get('tmin')} °C | "
            f"<b>Lluvia:</b> {dia.get('precipitacion_mm')} mm | <b>Viento:</b> {dia.get('viento_kmh')} km/h | "
            f"<b>Condiciones:</b> {dia.get('condiciones', 'N/A')}")
        story.append(Paragraph(linea, styles['Data']))

    doc.build(story)
    buffer.seek(0)
    return send_file(
        buffer, as_attachment=True,
        download_name=f"Reporte_Climatico_{tienda_nombre}_{año_consulta}_{datetime.now().strftime('%Y%m%d')}.pdf",
        mimetype='application/pdf'
    )


if __name__ == "__main__":
    app.run(debug=True)
