import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, send_file
# ReportLab para generar PDF
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, SimpleDocTemplate
from io import BytesIO

# Cargar variables de entorno (Asegúrate de tener un archivo .env si lo usas)
load_dotenv()

app = Flask(__name__)

# Configuración de variables de entorno
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'default_secret_key')

# =========================================================================
# 1. CONSOLIDACIÓN DE DATOS DE 42 TIENDAS (Shopping y Templo)
# =========================================================================

# Las coordenadas están en formato (Latitud, Longitud)
TIENDAS_MAP = {
    # 14 Tiendas Shopping
    "Shopping 1": (7.059683654849175, -73.86856975814536),
    "Shopping 2": (7.059768491566754, -73.8685599147996),
    "Shopping 3": (7.059715996100198, -73.86874898531704),
    "Shopping 4": (10.390839993695229, -75.47865011114159),
    "Shopping 5": (11.243198751367304, -74.2100645944834),
    "Shopping 6": (7.073912014064684, -73.16807685168257),
    "Shopping 7": (6.984873261792733, -73.05045422597566),
    "Shopping 8": (7.11908224576338, -73.12499126341542),
    "Shopping 9": (7.063583168903841, -73.08545269225122),
    "Shopping 10": (10.41616320505202, -75.52364301117757),
    "Shopping 11": (10.401998109508904, -75.45978355813655),
    "Shopping 13": (10.98819210780722, -74.77712600375727),
    "Shopping 18": (5.34731430225259, -72.38879057865113),
    "Shopping 22": (7.888542992988793, -72.50430970680809),

    # 28 Tiendas Templo
    "Templo 1": (3.452419735639639, -76.52978324939568),
    "Templo 2": (3.4522853774177134, -76.529898454676),
    "Templo 4": (4.538016339723241, -75.67406369068367),
    "Templo 5": (3.368715416264726, -76.52363767081717),
    "Templo 6": (3.8896123736819495, -77.07750078080207),
    "Templo 8": (3.4856000188429475, -76.4975345879099),
    "Templo 9": (3.5270710128823204, -76.29822708320997),
    "Templo 10": (4.8142852710304105, -75.6937757603496),
    "Templo 11": (3.4499690950433006, -76.53007453971138),
    "Templo 13": (3.263903562976194, -76.53926833718572),
    "Templo 14": (5.068244286496738, -75.51506847545048),
    "Templo 16": (4.085044358531649, -76.19849390561589),
    "Templo 17": (3.007623327632023, -76.48390139486145),
    "Templo 18": (3.4554591168890325, -76.51771937930089),
    "Templo 19": (4.444076234816372, -75.24018041918487),
    "Templo 20": (3.4038129971746423, -76.51205234961651),
    "Templo 23": (3.5846217572598627, -76.49387927985713),
    "Templo 24": (3.9009535681896073, -76.29992062234696),
    "Templo 26": (3.431348795529181, -76.48092776935766),
    "Templo 27": (3.3253938372763114, -76.23473751945423),
    "Templo 28": (2.4405990552966688, -76.60645440231042),
    "Templo 29": (3.3763857352126685, -76.5474364784691),
    "Templo 30": (3.4340788460477434, -76.53520635714665),
    "Templo 31": (3.4847382695031377, -76.49658666150108),
    "Templo 32": (3.4156413599943836, -76.5473246406779),
    "Templo 33": (2.9274842823407994, -75.28997287249801),
    "Templo 35": (3.540330538147451, -76.31075454232808),
    "Templo 36": (4.750430032121445, -75.91210409550338),
}


def agrupar_y_ordenar_tiendas(tiendas_map):
    """Agrupa las tiendas en 'Shopping' y 'Templo' y las ordena numéricamente."""
    agrupadas = {'Shopping': [], 'Templo': []}

    for nombre in tiendas_map.keys():
        if nombre.startswith("Shopping"):
            clave = "Shopping"
        elif nombre.startswith("Templo"):
            clave = "Templo"
        else:
            continue

        # Extraer el número para el orden
        num_str = nombre.split()[-1]
        try:
            num = int(''.join(filter(str.isdigit, num_str)))
        except ValueError:
            num = 9999

        agrupadas[clave].append((num, nombre))

    # Ordenar por número
    agrupadas['Shopping'].sort()
    agrupadas['Templo'].sort()

    # Devolver un diccionario limpio con el nombre
    return {
        'Shopping': [nombre for num, nombre in agrupadas['Shopping']],
        'Templo': [nombre for num, nombre in agrupadas['Templo']]
    }


TIENDAS_AGRUPADAS = agrupar_y_ordenar_tiendas(TIENDAS_MAP)


# =========================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO Y API (USANDO OPEN-METEO)
# =========================================================================

def obtener_historial_climatico(lat, lon):
    """
    Función para obtener datos climáticos usando Open-Meteo Historical Weather API.
    El rango fijo es desde el 1 de enero de 2024 hasta hace 5 días.
    Retorna una lista de diccionarios (datos diarios).
    """
    # Establecer fecha de inicio fija: 1 de enero de 2024
    fecha_inicio = "2024-01-01"

    # Calcular fecha de fin (Hoy - 5 días, para asegurar que los datos estén archivados)
    hoy = datetime.now()
    fecha_fin = (hoy - timedelta(days=5)).strftime('%Y-%m-%d')

    # Manejar caso de fechas no válidas
    try:
        if datetime.strptime(fecha_inicio, '%Y-%m-%d') > datetime.strptime(fecha_fin, '%Y-%m-%d'):
            return {
                "error": f"La fecha de inicio ({fecha_inicio}) es posterior a la fecha de fin calculada ({fecha_fin}). No hay datos suficientes para mostrar."}
    except ValueError:
        return {"error": "Error interno al procesar las fechas. Verifique el formato."}

    # URL de la API de Open-Meteo
    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={lat}&longitude={lon}&start_date={fecha_inicio}&end_date={fecha_fin}"
           f"&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
           f"&temperature_unit=celsius&wind_speed_unit=kmh&precipitation_unit=mm&timezone=auto")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        daily_data = data.get('daily', {})
        n_days = len(daily_data.get('time', []))

        datos_procesados = []

        # Mapeo de WMO Weather Codes (WMO: World Meteorological Organization)
        def map_wmo_code(code):
            # Códigos oficiales WMO 4680
            if code == 0:
                return "Despejado"
            elif 1 <= code <= 3:
                return "Mayormente Despejado a Parcialmente Nublado"
            elif 45 <= code <= 48:
                return "Niebla / Escarcha"
            elif 51 <= code <= 55:
                return "Llovizna"
            elif 61 <= code <= 65:
                return "Lluvia Moderada"
            elif 66 <= code <= 67:
                return "Lluvia Congelante"
            elif 80 <= code <= 82:
                return "Aguaceros Fuertes"
            elif 95 <= code <= 96:
                return "Tormenta"
            else:
                return "Condiciones Variadas"

        for i in range(n_days):
            datos_procesados.append({
                'fecha': daily_data['time'][i],
                # Renombrar variables para coincidir con la estructura de la plantilla
                'tmax': daily_data.get('temperature_2m_max', [None])[i],
                'tmin': daily_data.get('temperature_2m_min', [None])[i],
                'precipitacion_mm': daily_data.get('precipitation_sum', [None])[i],
                'viento_kmh': daily_data.get('wind_speed_10m_max', [None])[i],
                # Placeholder, ya que esta variable no está disponible en la API daily archive
                'nubosidad_perc': 50,
                'condiciones': map_wmo_code(daily_data.get('weather_code', [0])[i]),
            })

        return datos_procesados

    except requests.exceptions.HTTPError as e:
        return {
            "error": f"Error API HTTP: {e.response.status_code}. Mensaje: {response.text}. Revisa la URL de la API."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Error de conexión a la API: {e}. Revisa tu conexión a internet."}


def aplicar_filtros(datos, filtros):
    """
    Aplica filtros DINÁMICOS al historial climático día por día.
    El filtro de condición climática es de coincidencia exacta y se aplica
    antes que los filtros numéricos.
    """
    if not datos or "error" in datos:
        return datos

    datos_filtrados = []

    # Obtener valores de filtro
    tmax_min_val = float(filtros.get('tmax_min')) if filtros.get('tmax_min') else None
    precip_min_val = float(filtros.get('precip_min')) if filtros.get('precip_min') else None
    viento_min_val = float(filtros.get('viento_min')) if filtros.get('viento_min') else None
    condiciones_filtro_val = filtros.get('condiciones_filtro')

    for dia in datos:
        incluir = True

        # Usar valores de fallback (default)
        tmax = dia.get('tmax', -100)
        precip = dia.get('precipitacion_mm', 0)
        viento = dia.get('viento_kmh', 0)
        condicion_actual = dia.get('condiciones')

        # 1. FILTRO DE CONDICIONES CLIMÁTICAS (COINCIDENCIA EXACTA Y EXCLUYENTE)
        if condiciones_filtro_val and condiciones_filtro_val != "TODAS":
            if condicion_actual != condiciones_filtro_val:
                incluir = False

        # 2. FILTROS NUMÉRICOS (Solo se aplican si el día ya no ha sido excluido por la condición)

        # Filtro de Temperatura Máxima Mínima
        if incluir and tmax_min_val is not None and tmax < tmax_min_val:
            incluir = False

        # Filtro de Precipitación Mínima
        if incluir and precip_min_val is not None and precip < precip_min_val:
            incluir = False

        # Filtro de Velocidad de Viento Mínima
        if incluir and viento_min_val is not None and viento < viento_min_val:
            incluir = False

        if incluir:
            datos_filtrados.append(dia)

    return datos_filtrados


def calcular_ranking_anual(tiendas_map):
    """
    Calcula el Ranking Anual basado en la Temperatura Máxima Promedio.
    ADVERTENCIA: Esta función es LENTA (42 llamadas a API) y puede agotar límites.
    """
    ranking = []

    # 1. Calcular rango de fechas
    fecha_inicio = "2024-01-01"
    hoy = datetime.now()
    fecha_fin = (hoy - timedelta(days=5)).strftime('%Y-%m-%d')

    # 2. Iterar sobre todas las tiendas
    for nombre, (lat, lon) in tiendas_map.items():
        # Obtener datos históricos para la tienda
        datos_crudos = obtener_historial_climatico(lat, lon)

        if isinstance(datos_crudos, dict) and "error" in datos_crudos:
            # Si hay un error de API para esta tienda, simplemente la saltamos
            print(f"Error al obtener datos para {nombre}: {datos_crudos['error']}")
            ranking.append({
                'tienda': nombre,
                'tmax_promedio': None,  # Usar None para indicar dato faltante
                'lat': lat,
                'lon': lon
            })
            continue

        # 3. Calcular el promedio de T. Máxima
        if datos_crudos:
            # Filtrar valores válidos de tmax
            tmax_validos = [dia['tmax'] for dia in datos_crudos if dia.get('tmax') is not None]

            if tmax_validos:
                avg_tmax = sum(tmax_validos) / len(tmax_validos)

                ranking.append({
                    'tienda': nombre,
                    'tmax_promedio': round(avg_tmax, 2),
                    'lat': lat,
                    'lon': lon
                })
            else:
                ranking.append({
                    'tienda': nombre,
                    'tmax_promedio': None,
                    'lat': lat,
                    'lon': lon
                })

    # 4. Ordenar el ranking (Descendente por T. Máx. Promedio, ignorando Nones)
    ranking.sort(key=lambda x: x['tmax_promedio'] if x['tmax_promedio'] is not None else -float('inf'), reverse=True)

    return {'ranking': ranking, 'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin}


# =========================================================================
# 3. RUTAS FLASK
# =========================================================================

@app.route("/", methods=["GET", "POST"])
def historial_detallado():
    """Ruta principal para la consulta de historial climático día por día."""
    tienda_seleccionada = None
    datos_historial = None
    error_message = None
    # Valores por defecto para mantener los campos del formulario
    filtros_aplicados = {'tmax_min': '', 'precip_min': '', 'viento_min': '', 'condiciones_filtro': 'TODAS'}

    if request.method == "POST":
        tienda_seleccionada = request.form.get("tienda")

        # 1. Obtener y almacenar los filtros dinámicos
        filtros_aplicados = {
            'tmax_min': request.form.get("tmax_min", ""),
            'precip_min': request.form.get("precip_min", ""),
            'viento_min': request.form.get("viento_min", ""),
            'condiciones_filtro': request.form.get("condiciones_filtro", "TODAS"),
        }

        if tienda_seleccionada in TIENDAS_MAP:
            lat, lon = TIENDAS_MAP[tienda_seleccionada]

            # Obtener datos crudos (rango 2024)
            datos_crudos = obtener_historial_climatico(lat, lon)

            if isinstance(datos_crudos, dict) and "error" in datos_crudos:
                error_message = datos_crudos["error"]
            else:
                # 2. Aplicar filtros dinámicos
                datos_historial = aplicar_filtros(datos_crudos, filtros_aplicados)
        else:
            error_message = "Tienda seleccionada no válida."

    # Intentar mantener la selección de la tienda si hay datos de historial
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
    """Ruta para la pestaña de Ranking anual."""

    # LLAMADA LENTA: ESTA FUNCIÓN HARÁ 42 LLAMADAS A LA API.
    ranking_data = calcular_ranking_anual(TIENDAS_MAP)

    return render_template("index.html",
                           tiendas_agrupadas=TIENDAS_AGRUPADAS,
                           ranking_data=ranking_data,
                           seccion_activa="ranking")


@app.route("/generar_pdf", methods=["POST"])
def generar_pdf():
    """Genera el PDF a partir de los datos filtrados en el frontend."""
    datos_pdf_json = request.form.get('datos_pdf_json')
    tienda_nombre = request.form.get('tienda_nombre')

    try:
        datos_historial = json.loads(datos_pdf_json)
    except json.JSONDecodeError:
        return "Error al decodificar los datos del historial.", 400

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                            title=f"Reporte Climático {tienda_nombre}")

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
        linea = (f"<b>Fecha:</b> {dia.get('fecha')} | <b>T Max/Min:</b> {dia.get('tmax')}/{dia.get('tmin')} °C | "
                 f"<b>Lluvia:</b> {dia.get('precipitacion_mm')} mm | <b>Viento:</b> {dia.get('viento_kmh')} km/h | "
                 f"<b>Condiciones:</b> {dia.get('condiciones', 'N/A')}")
        story.append(Paragraph(linea, styles['Data']))

    doc.build(story)

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"Reporte_Climatico_{tienda_nombre}_{datetime.now().strftime('%Y%m%d')}.pdf",
        mimetype='application/pdf'
    )


if __name__ == "__main__":
    app.run(debug=True)