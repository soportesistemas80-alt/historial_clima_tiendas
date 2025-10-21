import os
import io
import requests
from functools import wraps
from flask import Flask, render_template, request, session, send_file, redirect, url_for
from dotenv import load_dotenv
from datetime import datetime

# --- Importaciones de ReportLab ---
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, Paragraph, Spacer, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# ------------------------------------------------

# Cargar variables de entorno
load_dotenv()

# --- Configuración y Inicialización ---
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "una_clave_secreta_de_fallback_segura")

GEOCODING_API_KEY = os.environ.get("GEOAPIFY_KEY")
VISUAL_CROSSING_API_KEY = os.environ.get("VISUAL_CROSSING_KEY")

GEOCODING_API_URL = "https://api.geoapify.com/v1/geocode/search"
WEATHER_API_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


# --- Funciones de Utilidad ---

def geocode_address(address):
    # Función para convertir dirección a lat/lon
    if not GEOCODING_API_KEY:
        return None

    params = {
        "text": address,
        "apiKey": GEOCODING_API_KEY,
        "limit": 1
    }
    try:
        resp = requests.get(GEOCODING_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features")
        if features:
            coords = features[0]["geometry"]["coordinates"]  # [lon, lat]
            return coords[1], coords[0]  # Retorna (lat, lon)
    except Exception as e:
        print("Error en geocoding:", e)
    return None


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "historial" not in session:
            session["historial"] = []
        if "theme" not in session:  # Inicializa el tema
            session["theme"] = "light"
        return f(*args, **kwargs)

    return wrapper


# --- FUNCIÓN DE DATOS DE CLIMA CON MÁS DETALLE (MODIFICADA) ---
def format_weather_data(day_data, resolved_address, fecha_hora, query, has_hour):
    """
    Formatea los datos brutos de la API.
    Añade un campo 'Fecha/Hora Display' para manejar la etiqueta.
    """

    # NEW: Lógica para la etiqueta condicional de Fecha y Hora
    fecha_key = "Fecha y Hora" if has_hour else "Fecha (Día Completo)"

    data = {
        "Ubicación": resolved_address,
        fecha_key: fecha_hora,  # Usa la clave dinámica
        "Condiciones": day_data.get('conditions', "N/A"),

        # --- Temperatura y Sensación ---
        "Temperatura": f"{day_data.get('temp', day_data.get('tempmax', 'N/A'))} °C",
        "Sensación Térmica": f"{day_data.get('feelslike', 'N/A')} °C",

        # --- Solo para el reporte de DÍA COMPLETO ('tempmax' solo existe si no se pasó la hora) ---
        **({
               "Máx. del Día": f"{day_data.get('tempmax', 'N/A')} °C",
               "Mín. del Día": f"{day_data.get('tempmin', 'N/A')} °C",
               "Sensación Máx.": f"{day_data.get('feelslikemax', 'N/A')} °C",
               "Sensación Mín.": f"{day_data.get('feelslikemin', 'N/A')} °C",
           } if 'tempmax' in day_data else {}),

        # --- Humedad y Punto de Rocío ---
        "Humedad": f"{day_data.get('humidity', 'N/A')} %",
        "Punto de Rocío (Dew)": f"{day_data.get('dew', 'N/A')} °C",

        # --- Precipitaciones ---
        "Precipitaciones (mm)": day_data.get('precip', 0),
        "Prob. Precipitación": f"{day_data.get('precipprob', 'N/A')} %",
        "Tipo de Precipitación": day_data.get('preciptype', ['Ninguno'])[0] if isinstance(day_data.get('preciptype'),
                                                                                          list) else day_data.get(
            'preciptype', 'Ninguno'),
        "Cobertura Precip.": f"{day_data.get('precipcover', 'N/A')} %",

        # --- Viento ---
        "Viento (Velocidad)": f"{day_data.get('windspeed', 'N/A')} km/h",
        "Viento (Dirección)": f"{day_data.get('winddir', 'N/A')} °",
        "Ráfaga de Viento": f"{day_data.get('windgust', 'N/A')} km/h",

        # --- Otros Parámetros ---
        "Presión (hPa)": f"{day_data.get('pressure', 'N/A')} hPa",
        "Visibilidad (km)": day_data.get('visibility', 'N/A'),
        "Cobertura Nubes": f"{day_data.get('cloudcover', 'N/A')} %",
        "Índice UV": day_data.get('uvindex', "N/A"),

        # Campos de Sol/Luna (solo disponibles para el día completo)
        **({
               "Radiación Solar": f"{day_data.get('solarradiation', 'N/A')} W/m²",
               "Energía Solar": f"{day_data.get('solarenergy', 'N/A')} MJ/m²",
               "Hora de Salida del Sol": day_data.get('sunrise', 'N/A'),
               "Hora de Puesta del Sol": day_data.get('sunset', 'N/A'),
           } if 'sunrise' in day_data else {}),

        # NEW: Variables internas para facilitar HTML/PDF
        "__Fecha_Key__": fecha_key,
        "__Fecha_Value__": fecha_hora
    }

    clean_data = {}
    for k, v in data.items():
        if v is not None and str(v).strip().upper() not in ('N/A', '', 'NONE', '0 KM/H', 'NINGUNO'):
            clean_data[k] = str(v)

    # El key dinámico ya está en el diccionario, solo necesitamos mover los tags internos al final
    # La key "Ubicación" debe ser el input del usuario (resolved_address en este punto es el input original)

    return clean_data


# --- FUNCIÓN DE GENERACIÓN DE PDF (MODIFICADA) ---
def generar_pdf_reportlab(ubicacion, fecha_hora, datos_clima):
    """Genera el PDF usando la librería ReportLab con el diseño mejorado y estilos corregidos."""

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            leftMargin=40, rightMargin=40,
                            topMargin=40, bottomMargin=40)
    story = []
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name='TitleBlue',
        parent=styles['h1'],
        fontName='Helvetica-Bold',
        fontSize=20,
        alignment=1,  # Centro
        textColor=colors.HexColor('#3498db')
    ))

    styles.add(ParagraphStyle(
        name='SubtitleNormal',
        parent=styles['Normal'],
        fontSize=12,
        alignment=0,  # Izquierda
        textColor=colors.HexColor('#555555')
    ))

    styles.add(ParagraphStyle(
        name='FooterStyle',
        parent=styles['Italic'],
        fontSize=8,
        alignment=2,  # Derecha
        textColor=colors.HexColor('#95a5a6')
    ))

    # --- ENCABEZADO Y TÍTULO ---
    story.append(Paragraph("Consulta de Clima Histórico", styles['TitleBlue']))
    story.append(Spacer(1, 6))

    story.append(Table([[Paragraph("", styles['Normal'])]], colWidths=[doc.width], style=TableStyle([
        ('LINEBELOW', (0, 0), (-1, -1), 1.5, colors.HexColor('#3498db'))
    ])))
    story.append(Spacer(1, 18))

    # Información de Ubicación y Fecha (MODIFICADO)
    # Buscamos la clave dinámica almacenada para la fecha
    fecha_key = datos_clima.get('__Fecha_Key__', 'Fecha y Hora')
    fecha_valor = datos_clima.get('__Fecha_Value__', fecha_hora)

    story.append(Paragraph(f"<b>Ubicación:</b> {ubicacion}", styles['SubtitleNormal']))
    story.append(Paragraph(f"<b>{fecha_key}:</b> {fecha_valor}", styles['SubtitleNormal']))  # Usamos la clave dinámica
    story.append(Spacer(1, 24))

    # --- TABLA DE DATOS ---
    data = [['Campo', 'Valor']]

    # Excluimos las claves de resumen y las claves internas
    excluded_keys = ['Ubicación', fecha_key, 'Condiciones', 'Temperatura', '__Fecha_Key__', '__Fecha_Value__']

    for clave, valor in datos_clima.items():
        if clave not in excluded_keys:
            data.append([clave, str(valor)])

    table_width = doc.width
    col_widths = [table_width * 0.5, table_width * 0.5]

    tabla = Table(data, colWidths=col_widths)

    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498db')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#f0f0f0'), colors.HexColor('#ffffff')]),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
    ])

    tabla.setStyle(style)
    story.append(Paragraph("<b>Detalles Completos del Clima</b>", styles['h3']))
    story.append(Spacer(1, 6))
    story.append(tabla)
    story.append(Spacer(1, 36))

    # --- PIE DE PÁGINA ---
    footer_text = "Datos proporcionados por Visual Crossing (Clima) y Geoapify (Geocodificación). Generado por la aplicación Flask."
    story.append(Paragraph(footer_text, styles['FooterStyle']))

    doc.build(story)
    buffer.seek(0)
    return send_file(buffer,
                     as_attachment=True,
                     download_name=f"reporte_clima_{fecha_valor.replace(' ', '_').replace(':', '')}.pdf",
                     mimetype="application/pdf")


# --- Rutas de la Aplicación (MODIFICADO) ---

@app.route("/toggle_theme", methods=["POST"])
def toggle_theme():
    """Alterna entre tema claro y oscuro y redirige al inicio."""
    session["theme"] = "dark" if session.get("theme") == "light" else "light"
    # Mantiene la pestaña actual
    return redirect(url_for('index', tab=request.form.get('current_tab', 'consulta')))


@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    resultado = None

    # 1. Recuperar y limpiar notificaciones de sesión (Mecanismo Toast)
    notification_type = session.pop("notification_type", None)
    notification_message = session.pop("notification_message", None)

    if not VISUAL_CROSSING_API_KEY or not GEOCODING_API_KEY:
        notification_type = "error"
        notification_message = "Error de configuración interna: Las claves API no están definidas en el servidor (archivo .env)."

        return render_template("index.html",
                               historial=session["historial"],
                               theme=session["theme"],
                               notification_type=notification_type,
                               notification_message=notification_message,
                               today=datetime.now().strftime("%Y-%m-%d"))

    if request.method == "POST":
        metodo = request.form.get("metodo", "")
        fecha = request.form.get("fecha", "").strip()
        hora = request.form.get("hora", "").strip()

        # Determina si hay hora para la etiqueta condicional (NEW)
        has_hour = bool(hora)

        if not fecha:
            session["notification_type"] = "error"
            session["notification_message"] = "Por favor, ingresa la fecha."
            return redirect(url_for('index', tab='consulta'))

        # --- VALIDACIÓN DE FECHA FUTURA ---
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M")

            if has_hour:
                fecha_hora_str = f"{fecha} {hora}"
                fecha_obj = datetime.strptime(fecha_hora_str, "%Y-%m-%d %H:%M")
                now_obj = datetime.strptime(now, "%Y-%m-%d %H:%M")
            else:
                fecha_obj = datetime.strptime(fecha, "%Y-%m-%d")
                now_obj = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            if fecha_obj > now_obj:
                session["notification_type"] = "error"
                session[
                    "notification_message"] = "¡Error de fecha! No se puede consultar el clima histórico para una fecha u hora futura."
                return redirect(url_for('index', tab='consulta'))

        except ValueError:
            session["notification_type"] = "error"
            session["notification_message"] = "Formato de fecha u hora inválido."
            return redirect(url_for('index', tab='consulta'))

        query = None
        lat = None
        lon = None
        original_input = None

        # --- LÓGICA DE GEOCODIFICACIÓN Y VALIDACIÓN ESTRICTA DE ENTRADA ---
        if metodo == "ciudad":
            ciudad = request.form.get("ciudad", "").strip()
            if not ciudad:
                session["notification_type"] = "error"
                session["notification_message"] = "El campo 'Ciudad/Región' es obligatorio."
                return redirect(url_for('index', tab='consulta'))

            # Validación estricta para Ciudad: No debe parecer dirección
            if any(char.lower() in ciudad.lower() for char in
                   ['#', 'n°', 'cl.', 'calle', 'carrera', 'avenida', 'av.', 'st.', 'ave.', 'km', 'no.']):
                session["notification_type"] = "error"
                session[
                    "notification_message"] = "Para entradas que parecen una dirección (contienen #, Cl, Av, etc.), por favor selecciona el método 'Dirección'."
                return redirect(url_for('index', tab='consulta'))

            original_input = ciudad  # Usamos el input original como ubicación del reporte

            # Intentar geocodificar la ciudad. Si funciona, se usa lat/lon. Si no, se usa el nombre.
            geo = geocode_address(ciudad)
            if geo is not None:
                lat, lon = geo
                query = f"{lat},{lon}"
            else:
                # Si Geoapify falla, usamos el nombre de la ciudad/región directamente en Visual Crossing
                query = ciudad

        elif metodo == "direccion":
            direccion = request.form.get("direccion", "").strip()
            if not direccion:
                session["notification_type"] = "error"
                session["notification_message"] = "El campo 'Dirección' es obligatorio."
                return redirect(url_for('index', tab='consulta'))

            if len(direccion.split()) < 2 or direccion.lower() in ['cali', 'bogota', 'lima', 'mexico']:
                session["notification_type"] = "error"
                session[
                    "notification_message"] = "Para búsquedas de 'Dirección' necesitas incluir calle/carrera. Si es solo una ciudad, usa el método 'Ciudad/Región'."
                return redirect(url_for('index', tab='consulta'))

            original_input = direccion
            geo = geocode_address(direccion)
            if geo is None:
                session["notification_type"] = "error"
                session[
                    "notification_message"] = "No se pudo convertir esa dirección a coordenadas válidas. Intenta ser más específico."
                return redirect(url_for('index', tab='consulta'))
            lat, lon = geo
            query = f"{lat},{lon}"

        elif metodo == "coordenadas":
            coord = request.form.get("coordenadas", "").strip()
            if not coord:
                session["notification_type"] = "error"
                session["notification_message"] = "El campo 'Coordenadas' es obligatorio."
                return redirect(url_for('index', tab='consulta'))

            original_input = coord
            try:
                lat_str, lon_str = coord.replace(' ', '').split(",", 1)
                lat = float(lat_str)
                lon = float(lon_str)
                query = f"{lat},{lon}"
            except:
                session["notification_type"] = "error"
                session[
                    "notification_message"] = "Formato de coordenadas inválido. Usa: latitud, longitud (Ej: 3.36, -76.52)"
                return redirect(url_for('index', tab='consulta'))

        else:
            session["notification_type"] = "error"
            session["notification_message"] = "Método de búsqueda no válido."
            return redirect(url_for('index', tab='consulta'))

        # Construcción de la URL de consulta
        fecha_consulta = fecha
        if has_hour:
            fecha_consulta = f"{fecha}T{hora}:00"
            include_param = "hours"
        else:
            include_param = "days,alerts,current,events"

        api_query = query
        url = f"{WEATHER_API_URL}/{api_query}/{fecha_consulta}?key={VISUAL_CROSSING_API_KEY}&unitGroup=metric&include={include_param}"

        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()

            day_data = None
            fecha_resultado = None

            if include_param == "hours":
                if data.get("days") and data["days"][0].get("hours"):
                    day_data = data["days"][0]["hours"][0]
                    fecha_resultado = f"{fecha} {hora}"
            else:
                if data.get("days"):
                    day_data = data["days"][0]
                    fecha_resultado = fecha

            if day_data is None:
                session["notification_type"] = "error"
                session[
                    "notification_message"] = "No se encontraron datos históricos para esa ubicación y fecha. Verifica que la fecha sea válida o que la hora exista."
            else:
                # --- CORRECCIÓN SOLICITADA: Usar el INPUT original del usuario como Ubicación ---
                report_address = original_input

                # Formatear datos, pasando el nuevo flag
                resultado = format_weather_data(day_data, report_address, fecha_resultado, api_query, has_hour)

                # 2. Almacenar resultado en sesión para el PDF actual
                session["ultimo_clima"] = {
                    "datos": resultado,
                    "ubicacion": report_address,
                    "fecha_hora": resultado["__Fecha_Value__"],  # Usar el valor correcto para el nombre del archivo
                }

                # 3. Almacenar resultado COMPLETO en el historial
                hora_registro = datetime.now().strftime("%H:%M:%S")
                session["historial"].insert(0, {
                    "consulta": f"{report_address} ({resultado['__Fecha_Value__']})",
                    "hora_registro": hora_registro,
                    "data_pdf": resultado,
                    "ubicacion": report_address,
                    "fecha_hora": resultado["__Fecha_Value__"],
                })
                session.modified = True

                session["notification_type"] = "success"
                session["notification_message"] = "¡Consulta exitosa! El reporte está listo."

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"

            session["notification_type"] = "error"
            if status == 400:
                session[
                    "notification_message"] = f"Error 400 (Petición Inválida). Verifica la fecha/hora o el formato de la ubicación."
            elif status in [401, 403]:
                session[
                    "notification_message"] = "Error de Autenticación. Verifica que la clave VISUAL_CROSSING_KEY en tu .env sea válida."
            else:
                session["notification_message"] = f"Error HTTP: {status}. Ocurrió un problema consultando el clima."

        except Exception as e:
            session["notification_type"] = "error"
            session["notification_message"] = f"Ocurrió un error inesperado: {str(e)}"

        return redirect(url_for('index', tab='reporte'))

    # Si se llega por GET (incluyendo recarga), cargar el último resultado
    if "ultimo_clima" in session:
        resultado = session["ultimo_clima"]["datos"]

    return render_template("index.html",
                           resultado=resultado,
                           historial=session["historial"],
                           theme=session["theme"],
                           today=datetime.now().strftime("%Y-%m-%d"),
                           notification_type=notification_type,
                           notification_message=notification_message)


@app.route("/descargar_pdf", methods=["POST"])
def descargar_pdf():
    """Ruta para generar y descargar el PDF del último reporte (Reporte Tab)."""
    ultimo = session.get("ultimo_clima")
    if not ultimo:
        session["notification_type"] = "error"
        session["notification_message"] = "No hay datos de clima recientes para generar el PDF."
        return redirect(url_for('index', tab='reporte'))

    datos = ultimo["datos"]
    ubicacion = ultimo["ubicacion"]
    fecha_hora = ultimo["fecha_hora"]  # Usado solo para el nombre del archivo

    return generar_pdf_reportlab(ubicacion, fecha_hora, datos)


@app.route("/descargar_historial_pdf/<int:index>", methods=["POST"])
def descargar_historial_pdf(index):
    """Ruta para generar y descargar el PDF de un elemento del historial, usando su índice."""

    historial = session.get("historial")
    if not historial or index < 0 or index >= len(historial):
        session["notification_type"] = "error"
        session["notification_message"] = "Error: El índice de historial para la descarga es inválido."
        return redirect(url_for('index', tab='historial'))

    item = historial[index]

    datos = item["data_pdf"]
    ubicacion = item["ubicacion"]
    fecha_hora = item["fecha_hora"]

    return generar_pdf_reportlab(ubicacion, fecha_hora, datos)


# --- RUTA: VACIAR HISTORIAL ---
@app.route("/vaciar_historial", methods=["POST"])
def vaciar_historial():
    """Vacía por completo el historial de consultas de la sesión."""
    session["historial"] = []
    session.modified = True
    session["notification_type"] = "info"
    session["notification_message"] = "El historial de consultas ha sido vaciado."
    session.pop("ultimo_clima", None)
    return redirect(url_for('index', tab='historial'))


if __name__ == "__main__":
    app.run(debug=True, port=5000)