from flask import Flask, jsonify, request,make_response  # Importa Flask y una función para convertir a JSON
import psycopg2               # Importa el conector de PostgreSQL
import psycopg2.extras        # Para obtener resultados como diccionarios (opcional pero útil)
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import get_jwt, create_access_token, get_jwt_identity, jwt_required, JWTManager # Importar JWT
from flask_mail import Mail, Message # Importar Flask-Mail
import os # Necesario para variables de entorno
from datetime import datetime, timezone,timedelta, date # Para fechas y horas
import secrets # Para generar tokens seguros
from urllib.parse import urlparse
import json # <--- AÑADIR ESTA LÍNEA
import firebase_admin
from firebase_admin import exceptions as firebase_exceptions
from firebase_admin import credentials
from firebase_admin import messaging # Es probable que también necesites esto
from firebase_admin import storage # Es probable que también necesites esto
from flask_executor import Executor # <-- Añadir importación
from apscheduler.schedulers.background import BackgroundScheduler # <-- Añadir
from apscheduler.triggers.interval import IntervalTrigger       # <-- Añadir
import atexit                                                   # <-- Añadir (para apagar scheduler)
import logging                                                  # <-- Añadir (para logs del scheduler)
import concurrent.futures # <-- AÑADIR
import requests  # Para llamar a la API de Jolpica / Ergast
import uuid # Para nombres de archivo únicos
import urllib.request
import ssl
import subprocess
import tempfile
from curl_cffi import requests as cffi_requests # Importamos esto para la descarga potente
import base64
import urllib.parse # Para codificar el texto del prompt
import math # Necesario para cálculos si fuera el caso, aunque usaremos lógica simple
# Asegúrate de que 'datetime', 'timezone' y 'ZoneInfo' estén ya importados correctamente arriba.

# Configurar logging para ver mensajes de APScheduler (opcional pero útil)
logging.basicConfig(level=logging.INFO)
logging.getLogger('apscheduler').setLevel(logging.DEBUG) # Más detalle del scheduler


'''
# --- Inicialización de Firebase Admin ---
try:
    # --- ¡¡IMPORTANTE!! Cambia esta ruta por la ruta REAL de tu archivo de credenciales ---
    FIREBASE_CRED_PATH = os.environ.get('FIREBASE_CRED_PATH', '/ruta/segura/en/tu/nas/firebase-adminsdk-xxxx.json')

    if os.path.exists(FIREBASE_CRED_PATH):
        cred = credentials.Certificate(FIREBASE_CRED_PATH)
        firebase_admin.initialize_app(cred)
        print("INFO: Firebase Admin SDK inicializado correctamente.")
    else:
        print(f"WARN: No se encontró el archivo de credenciales de Firebase en '{FIREBASE_CRED_PATH}'. Las notificaciones push estarán desactivadas.")
        # Puedes decidir si lanzar un error o continuar sin notificaciones
        # raise FileNotFoundError(f"Firebase credentials not found at {FIREBASE_CRED_PATH}")
except Exception as e:
    print(f"ERROR: Fallo al inicializar Firebase Admin SDK: {e}")
    # Considerar si la app debe fallar al iniciar si Firebase es crítico
    # raise e

# --- Fin Inicialización Firebase Admin ---
'''

# --- Inicialización de Firebase Admin (MODIFICADA) ---
try:
    # --- ¡¡IMPORTANTE!! Cambia esta ruta y el ID del proyecto ---
    FIREBASE_CRED_PATH = os.environ.get('FIREBASE_CRED_PATH', '/ruta/segura/en/tu/nas/firebase-adminsdk-xxxx.json')
    FIREBASE_PROJECT_ID = os.environ.get('FIREBASE_PROJECT_ID', 'AQUI_VA_TU_ID_DE_PROYECTO_FIREBASE') # <-- ¡PON TU ID AQUÍ!

    if os.path.exists(FIREBASE_CRED_PATH):
        cred = credentials.Certificate(FIREBASE_CRED_PATH)
        # --- Añadir projectId explícitamente ---
        firebase_admin.initialize_app(cred, {
            'projectId': FIREBASE_PROJECT_ID,
            'storageBucket': 'f1-porra-app-links.firebasestorage.app' # <-- AÑADE ESTO
        })
        # Añadimos log con el Project ID usado
        print(f"INFO: Firebase Admin SDK con storage inicializado correctamente para proyecto '{FIREBASE_PROJECT_ID}'.")
    else:
        print(f"ERROR: No se encontró el archivo de credenciales Firebase en '{FIREBASE_CRED_PATH}'")
except Exception as e:
    print(f"!!!!!!!! ERROR AL INICIALIZAR FIREBASE ADMIN SDK !!!!!!!!")
    print(f"Error: {e}")
    # Considera detener la app si Firebase no se puede inicializar
    # raise e

# --- FIN Inicialización Firebase Admin MODIFICADA ---

# zoneinfo está disponible en Python 3.9+. Si usas una versión anterior,
# necesitarás instalar y usar pytz: pip install pytz
try:
    from zoneinfo import ZoneInfo # Python 3.9+
except ImportError:
    from pytz import timezone as ZoneInfo # Fallback para Python < 3.9 (requiere pip install pytz)


# Leer DATABASE_URL o valores individuales
database_url = os.environ.get('DATABASE_URL')
if database_url:
    result = urlparse(database_url)
    DB_USER = result.username
    DB_PASS = result.password
    DB_HOST = result.hostname
    DB_PORT = result.port if result.port else 5432 # Puerto por defecto
    DB_NAME = result.path[1:] # Quitar la barra inicial
else:
     # Fallback a variables individuales si DATABASE_URL no está
     DB_HOST = os.environ.get("DB_HOST", "localhost")
     DB_NAME = os.environ.get("DB_NAME", "f1_porra_db")
     DB_USER = os.environ.get("DB_USER", "postgres")
     DB_PASS = os.environ.get("DB_PASS", "tu_contraseña") # ¡Importante!
     DB_PORT = int(os.environ.get("DB_PORT", 5432))

# --- Configuración Jolpica / Ergast F1 (NUEVO) ---

JOLPICA_BASE_URL = os.environ.get("JOLPICA_BASE_URL", "https://api.jolpi.ca/ergast/f1")
JOLPICA_TIMEOUT = int(os.environ.get("JOLPICA_TIMEOUT", 5))  # segundos
JOLPICA_STANDINGS_CACHE_TTL_MIN = int(os.environ.get("JOLPICA_STANDINGS_CACHE_TTL_MIN", 30))  # minutos


class JolpicaError(Exception):
    """Error genérico para problemas al llamar a Jolpica / Ergast."""
    pass


def _safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _jolpica_get(path):
    """
    Llama a Jolpica (API Ergast) y devuelve el JSON ya parseado.
    Lanza JolpicaError si hay problema de red, código HTTP != 200 o JSON inválido.
    """
    url = f"{JOLPICA_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    try:
        resp = requests.get(url, timeout=JOLPICA_TIMEOUT)
    except requests.RequestException as exc:
        print(f"JOLPICA ERROR: Error de red llamando a {url}: {exc}")
        raise JolpicaError("Error de red llamando a Jolpica") from exc

    if resp.status_code != 200:
        # Logueamos algo de la respuesta para debug
        body_preview = resp.text[:500]
        print(f"JOLPICA ERROR: Código {resp.status_code} desde {url}. Cuerpo: {body_preview}")
        raise JolpicaError(f"Respuesta {resp.status_code} desde Jolpica")

    try:
        data = resp.json()
    except ValueError as exc:
        print(f"JOLPICA ERROR: JSON inválido desde {url}: {exc}")
        raise JolpicaError("Respuesta JSON inválida desde Jolpica") from exc

    return data


def _parse_driver_standings(ergast_json):
    """
    Recibe el JSON tal cual de Ergast y devuelve una lista simplificada de pilotos:
    [
      {
        "position": 1,
        "points": 175,
        "wins": 6,
        "driverCode": "VER",
        "driverName": "Max Verstappen",
        "constructorName": "Red Bull Racing"
      },
      ...
    ]
    """
    mrdata = ergast_json.get("MRData", {})
    standings_table = mrdata.get("StandingsTable", {})
    lists = standings_table.get("StandingsLists", [])
    if not lists:
        return []

    driver_standings = lists[0].get("DriverStandings", []) or []
    simplified = []

    for item in driver_standings:
        driver = item.get("Driver", {}) or {}
        constructors = item.get("Constructors", []) or []

        code = driver.get("code") or driver.get("permanentNumber") or driver.get("driverId")
        given = driver.get("givenName", "") or ""
        family = driver.get("familyName", "") or ""
        full_name = f"{given} {family}".strip()

        constructor_name = constructors[0].get("name") if constructors else None

        simplified.append({
            "position": _safe_int(item.get("position")),
            "points": _safe_int(item.get("points")),
            "wins": _safe_int(item.get("wins"), 0),
            "driverCode": code,
            "driverName": full_name,
            "constructorName": constructor_name,
        })

    return simplified


def _parse_constructor_standings(ergast_json):
    """
    Devuelve una lista simplificada de constructores:
    [
      {
        "position": 1,
        "points": 250,
        "wins": 8,
        "constructorName": "Red Bull Racing",
        "constructorId": "red_bull"
      },
      ...
    ]
    """
    mrdata = ergast_json.get("MRData", {})
    standings_table = mrdata.get("StandingsTable", {})
    lists = standings_table.get("StandingsLists", [])
    if not lists:
        return []

    constructor_standings = lists[0].get("ConstructorStandings", []) or []
    simplified = []

    for item in constructor_standings:
        constructor = item.get("Constructor", {}) or {}

        simplified.append({
            "position": _safe_int(item.get("position")),
            "points": _safe_int(item.get("points")),
            "wins": _safe_int(item.get("wins"), 0),
            "constructorName": constructor.get("name"),
            "constructorId": constructor.get("constructorId"),
        })

    return simplified

# Crea la aplicación Flask
app = Flask(__name__)

#mail = Mail(app) # Inicializa Flask-Mail con tu app
# --- Configuración de Flask-JWT-Extended ---
# Necesita una clave secreta. ¡CAMBIA ESTO por algo seguro y mantenlo secreto en producción!
# Puedes generarla con: python -c 'import os; print(os.urandom(24))'
jwt = JWTManager(app) # Inicializa JWTManager con tu app
# --- Configuración Flask-Mail ---
# Es MUY RECOMENDABLE usar variables de entorno para esto en producción
# Para probar localmente, puedes definirlas temporalmente o crear un archivo .env
app.config["JWT_SECRET_KEY"] = os.environ.get('JWT_SECRET_KEY', 'cambiar-esta-clave-secreta-ya!')
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(days=14)
# Configuración para Gmail
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587 # Puerto para TLS
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False # No usar SSL si se usa TLS en el puerto 587
# ¡IMPORTANTE! Usa variables de entorno para tu email y contraseña de aplicación
app.config['MAIL_USERNAME'] = os.environ.get('GMAIL_USER') # Ejemplo: tuemail@gmail.com
app.config['MAIL_PASSWORD'] = os.environ.get('GMAIL_APP_PASSWORD') # La contraseña de aplicación de 16 caracteres que generaste
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('GMAIL_USER') # O "Nombre App <tuemail@gmail.com>"



mail = Mail(app) # Inicializa Flask-Mail con tu app


# --- Configurar Flask-Executor ---
# Usaremos ThreadPoolExecutor por simplicidad inicial
# Ajusta max_workers según los recursos de tu NAS
app.config['EXECUTOR_TYPE'] = 'thread'
app.config['EXECUTOR_MAX_WORKERS'] = 5
# executor = Executor(app) # <-- YA NO SE USA Flask-Executor

# --- NUEVO: Crear instancia global de ThreadPoolExecutor ---
# Ajusta max_workers según los recursos de tu servidor/NAS
MAX_WORKERS_FCM = 10
thread_pool_executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_FCM)
print(f"INFO: ThreadPoolExecutor inicializado con max_workers={MAX_WORKERS_FCM}")
# --- FIN NUEVO ---


# ======= i18n para notificaciones FCM =======
SUPPORTED_LANGS = {'es','en','fr','pt','ca'}

def _pick_lang(lang: str) -> str:
    lang = (lang or '').strip().lower()
    return lang if lang in SUPPORTED_LANGS else 'es'

# Diccionario de textos (placeholders: {race}, {deadline}, {porra}, {status}, {current}, {next})
FCM_TEXTS = {
    'deadline_reminder': {
        'title': {
            'es': "⏰ ¡Última oportunidad!",
            'en': "⏰ Last chance!",
            'fr': "⏰ Dernière chance !",
            'pt': "⏰ Última oportunidade!",
            'ca': "⏰ Última oportunitat!"
        },
        'body': {
            'es': "La fecha límite para apostar en {race} es pronto ({deadline}). ¡No te olvides!",
            'en': "Betting for {race} closes soon ({deadline}). Don’t forget!",
            'fr': "Les paris pour {race} se terminent bientôt ({deadline}). N’oublie pas !",
            'pt': "As apostas para {race} fecham em breve ({deadline}). Não te esqueças!",
            'ca': "L’aposta per {race} tanca aviat ({deadline}). No t’oblidis!"
        }
    },
    'bet_status_update': {
        'title': {
            'es': "🎯 Estado de tu apuesta",
            'en': "🎯 Bet status",
            'fr': "🎯 Statut du pari",
            'pt': "🎯 Estado da aposta",
            'ca': "🎯 Estat de l’aposta"
        },
        'body_accept': {
            'es': "Tu apuesta para {race} en la porra “{porra}” ha sido ACEPTADA.",
            'en': "Your bet for {race} in pool “{porra}” has been ACCEPTED.",
            'fr': "Ton pari pour {race} dans la porra “{porra}” a été ACCEPTÉ.",
            'pt': "A tua aposta para {race} na porra “{porra}” foi ACEITE.",
            'ca': "La teva aposta per {race} a la porra “{porra}” ha estat ACCEPTADA."
        },
        'body_reject': {
            'es': "Tu apuesta para {race} en la porra “{porra}” ha sido RECHAZADA.",
            'en': "Your bet for {race} in pool “{porra}” has been REJECTED.",
            'fr': "Ton pari pour {race} dans la porra “{porra}” a été REFUSÉ.",
            'pt': "A tua aposta para {race} na porra “{porra}” foi RECUSADA.",
            'ca': "La teva aposta per {race} a la porra “{porra}” ha estat REBUTJADA."
        }
    },
    'porra_invitation': {
        'title': {
            'es': "👋 Invitación a porra",
            'en': "👋 Pool invitation",
            'fr': "👋 Invitation à une porra",
            'pt': "👋 Convite para porra",
            'ca': "👋 Invitació a una porra"
        },
        'body': {
            'es': "{inviter} te ha invitado a unirte a “{porra}”.",
            'en': "{inviter} has invited you to join “{porra}”.",
            'fr': "{inviter} t’a invité à rejoindre “{porra}”.",
            'pt': "{inviter} convidou-te para entrares em “{porra}”.",
            'ca': "{inviter} t’ha convidat a unir-te a “{porra}”."
        }
    },
    'result_ready': {
        'title': {
            'es': "🏁 ¡Resultado disponible!",
            'en': "🏁 Result is out!",
            'fr': "🏁 Résultat disponible !",
            'pt': "🏁 Resultado disponível!",
            'ca': "🏁 Resultat disponible!"
        },
        'body': {
            'es': "Ya puedes ver tus puntos para {race}.",
            'en': "You can now check your points for {race}.",
            'fr': "Tu peux voir tes points pour {race}.",
            'pt': "Já podes ver os teus pontos para {race}.",
            'ca': "Ja pots veure els teus punts per {race}."
        }
    },
    'next_race_available': {
        'title': {
            'es': "🚀 ¡Nueva carrera abierta!",
            'en': "🚀 Next race open!",
            'fr': "🚀 Prochaine course ouverte !",
            'pt': "🚀 Próxima corrida aberta!",
            'ca': "🚀 Nova cursa oberta!"
        },
        'body': {
            'es': "Resultados de {current} listos. Ya puedes apostar en {next}.",
            'en': "Results for {current} are ready. You can now bet on {next}.",
            'fr': "Résultats de {current} prêts. Tu peux parier sur {next}.",
            'pt': "Resultados de {current} prontos. Já podes apostar em {next}.",
            'ca': "Resultats de {current} llestos. Ja pots apostar a {next}."
        }
    },
    'betting_closed': {
        'title': {
            'es': "🔒 Apuestas cerradas",
            'en': "🔒 Betting closed",
            'fr': "🔒 Paris fermés",
            'pt': "🔒 Apostas encerradas",
            'ca': "🔒 Apostes tancades"
        },
        'body': {
            'es': "Las apuestas para {race} han cerrado.",
            'en': "Betting for {race} is now closed.",
            'fr': "Les paris pour {race} sont fermés.",
            'pt': "As apostas para {race} foram encerradas.",
            'ca': "Les apostes per {race} s’han tancat."
        }
    },
    'trophy_unlocked': {
        'title': {
            'es': "🏆 ¡Nuevo trofeo!",
            'en': "🏆 New trophy!",
            'fr': "🏆 Nouveau trophée !",
            'pt': "🏆 Novo troféu!",
            'ca': "🏆 Nou trofeu!"
        },
        'body': {
            'es': "Has conseguido el trofeo: {trophy}.",
            'en': "You’ve unlocked the trophy: {trophy}.",
            'fr': "Tu as obtenu le trophée : {trophy}.",
            'pt': "Conseguiste o troféu: {trophy}.",
            'ca': "Has aconseguit el trofeu: {trophy}."
        }
    }
}

def _fcm_text(kind: str, lang: str, **kwargs):
    lang = _pick_lang(lang)
    pack = FCM_TEXTS.get(kind, {})
    title_map = pack.get('title', {})
    # Para bet_status_update elegimos body según status
    if kind == 'bet_status_update':
        status = kwargs.get('status', 'ACEPTADA')
        body_map = pack.get('body_accept' if status == 'ACEPTADA' else 'body_reject', {})
    else:
        body_map = pack.get('body', {})
    title = title_map.get(lang, title_map.get('es', ''))
    body_tpl = body_map.get(lang, body_map.get('es', ''))
    return title, body_tpl.format(**kwargs)
# ======= fin i18n =======

# --- Funciones de caché en PostgreSQL para clasificaciones F1 (NUEVO) ---

def _get_driver_standings_cache(season="current"):
    """
    Lee de la tabla f1_driver_standings.
    Devuelve un diccionario con season, last_updated (datetime) e items (lista de filas),
    o None si no hay registro.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            "SELECT season, last_updated, payload FROM f1_driver_standings WHERE season = %s;",
            (season,)
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None

        payload = row["payload"]
        # Si por algún motivo psycopg2 devolviera texto, intentamos parsear a JSON
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                print(f"WARNING: payload de f1_driver_standings no es JSON válido para season={season}")
                payload = []

        return {
            "season": row["season"],
            "last_updated": row["last_updated"],
            "items": payload,
        }
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR en _get_driver_standings_cache: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()


def upload_base64_to_firebase(b64_string):
    print("Procesando imagen recibida en Base64...")
    try:
        # Limpiar cabecera si viene (ej: "data:image/png;base64,...")
        if "," in b64_string:
            b64_string = b64_string.split(",")[1]

        # Decodificar
        image_data = base64.b64decode(b64_string)

        # Subir a Firebase
        bucket = storage.bucket()
        filename = f"noticias/news_{uuid.uuid4()}.png"
        blob = bucket.blob(filename)

        blob.upload_from_string(image_data, content_type='image/png')
        blob.make_public()

        print(f"Imagen Base64 subida a Firebase: {blob.public_url}")
        return blob.public_url
    except Exception as e:
        print(f"Error procesando Base64: {e}")
        return None

# --- Función auxiliar para subir imagen desde URL a Firebase Storage ---

# --- REEMPLAZAR ESTA FUNCIÓN EN mi_api.py ---
def upload_image_to_firebase(image_url):
    print(f"Descargando imagen (vía curl_cffi - Chrome Impersonation): {image_url}")
    
    # Imagen de respaldo (Logo genérico)
    FALLBACK_IMAGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/3/33/F1.svg/1200px-F1.svg.png"

    try:
        # Usamos curl_cffi para imitar un navegador real (Chrome 120).
        # Esto evita el error 403 (firma rota) y el 409 (bloqueo de bot).
        response = cffi_requests.get(
            image_url,
            impersonate="chrome120", # <--- La clave: nos hacemos pasar por Chrome
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"Descarga OK ({len(response.content)} bytes). Subiendo a Firebase...")
            
            # Subir a Firebase Storage
            bucket = storage.bucket()
            filename = f"noticias/news_{uuid.uuid4()}.png"
            blob = bucket.blob(filename)
            
            # Subir el contenido binario
            blob.upload_from_string(response.content, content_type='image/png')
            blob.make_public()
            
            public_url = blob.public_url
            print(f"Imagen subida a Firebase correctamente: {public_url}")
            return public_url
            
        else:
            print(f"Error descarga curl_cffi: Status {response.status_code}")
            return FALLBACK_IMAGE

    except Exception as e:
        print(f"!!!!!!!! EXCEPCIÓN EN UPLOAD (curl_cffi): {e}")
        return FALLBACK_IMAGE

# --- Tarea de notificación en background (reutilizada y simplificada) ---
def send_fcm_news_notification_task(news_data):
    """
    Envía notificaciones push personalizadas por idioma.
    TÍTULO: Texto genérico (ej: "¡Nueva Noticia!")
    CUERPO: El titular de la noticia.
    """
    print(f"BACKGROUND TASK (News): Iniciando envío multilingüe...")

    # Diccionario de títulos genéricos
    GENERIC_TITLES = {
        'es': "📰 ¡Nueva noticia F1!",
        'en': "📰 New F1 News!",
        'fr': "📰 Nouvelle actualité F1 !",
        'pt': "📰 Nova notícia F1!",
        'ca': "📰 Nova notícia F1!"
    }
    
    try:
        # 1. Conectar a BD y obtener tokens E IDIOMA
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        # IMPORTANTE: Seleccionamos también language_code
        cur.execute("SELECT fcm_token, language_code FROM usuario WHERE fcm_token IS NOT NULL AND fcm_token != '';")
        users_raw = cur.fetchall()
        cur.close()
        conn.close()

        if not users_raw:
            print("BACKGROUND TASK (News): No hay usuarios suscritos.")
            return

        print(f"BACKGROUND TASK (News): Procesando {len(users_raw)} usuarios.")

        # 2. Asegurar instancia de Firebase
        try:
            app = firebase_admin.get_app()
        except ValueError:
            cred = credentials.Certificate(FIREBASE_CRED_PATH)
            bucket_name = os.environ.get('FIREBASE_BUCKET_NAME', 'f1-porra-app-links.firebasestorage.app') 
            app = firebase_admin.initialize_app(cred, {
                'projectId': FIREBASE_PROJECT_ID,
                'storageBucket': bucket_name
            })

        # 3. Bucle de envío personalizado
        enviados = 0
        errores = 0

        for row in users_raw:
            token = row[0]
            # Si language_code es null o vacío, usamos 'es' por defecto
            user_lang = (row[1] or 'es').strip().lower()
            
            # A) TÍTULO GENÉRICO según idioma
            notification_title = GENERIC_TITLES.get(user_lang, GENERIC_TITLES['es'])
            
            # B) CUERPO DE LA NOTIFICACIÓN = TITULAR DE LA NOTICIA
            # Buscamos keys como 'titulo_en', 'titulo_fr', etc. Fallback a 'titulo_es'.
            news_headline_key = f'titulo_{user_lang}'
            notification_body = news_data.get(news_headline_key) or news_data.get('titulo_es') or "Nueva Noticia F1"
            
            try:
                message = messaging.Message(
                    notification=messaging.Notification(
                        title=notification_title, # Ej: "📰 ¡Nueva noticia F1!"
                        body=notification_body,   # Ej: "Alonso renueva con Aston Martin"
                    ),
                    data={
                        'tipo_notificacion': 'nueva_noticia', # Esto es clave para Flutter
                        'click_action': 'FLUTTER_NOTIFICATION_CLICK'
                    },
                    token=token
                )
                messaging.send(message, app=app)
                enviados += 1
            except Exception as e:
                errores += 1
                # print(f"Fallo token {user_lang}: {e}") # Descomentar para debug

        print(f"BACKGROUND TASK (News): Finalizado. Enviados: {enviados}, Errores: {errores}")

    except Exception as e:
        print(f"!!!!!!!! ERROR BACKGROUND TASK (News) !!!!!!!!")
        print(e)
        import traceback
        traceback.print_exc()

def _save_driver_standings_cache(season, items):
    """
    Guarda (upsert) en la tabla f1_driver_standings.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO f1_driver_standings (season, last_updated, payload)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (season) DO UPDATE
            SET last_updated = EXCLUDED.last_updated,
                payload = EXCLUDED.payload;
            """,
            (season, psycopg2.extras.Json(items))
        )
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR en _save_driver_standings_cache: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()


def _get_constructor_standings_cache(season="current"):
    """
    Lee de la tabla f1_constructor_standings.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            "SELECT season, last_updated, payload FROM f1_constructor_standings WHERE season = %s;",
            (season,)
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None

        payload = row["payload"]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                print(f"WARNING: payload de f1_constructor_standings no es JSON válido para season={season}")
                payload = []

        return {
            "season": row["season"],
            "last_updated": row["last_updated"],
            "items": payload,
        }
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR en _get_constructor_standings_cache: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()


def _save_constructor_standings_cache(season, items):
    """
    Guarda (upsert) en la tabla f1_constructor_standings.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO f1_constructor_standings (season, last_updated, payload)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (season) DO UPDATE
            SET last_updated = EXCLUDED.last_updated,
                payload = EXCLUDED.payload;
            """,
            (season, psycopg2.extras.Json(items))
        )
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR en _save_constructor_standings_cache: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()


def _is_cache_fresh(last_updated):
    """
    Devuelve True si last_updated es reciente según JOLPICA_STANDINGS_CACHE_TTL_MIN.
    """
    if not last_updated:
        return False
    try:
        # En BD ya guardamos timestamptz, así que debería ser datetime con tz
        if last_updated.tzinfo is None:
            last_updated_utc = last_updated.replace(tzinfo=timezone.utc)
        else:
            last_updated_utc = last_updated.astimezone(timezone.utc)
        age = datetime.now(timezone.utc) - last_updated_utc
        return age <= timedelta(minutes=JOLPICA_STANDINGS_CACHE_TTL_MIN)
    except Exception as exc:
        print(f"WARNING: No se pudo calcular la frescura de caché: {exc}")
        return False

# --- Job APScheduler: sincronizar clasificaciones F1 con Jolpica (NUEVO) ---

def sync_f1_standings_job():
    """
    Job periódico para sincronizar la clasificación de pilotos y constructores
    desde Jolpica/Ergast y guardar en la caché (tablas f1_*_standings).

    Se asume que se ejecuta en background via APScheduler.
    """
    print(f"\n--- JOB F1_STANDINGS: Inicio sync_f1_standings_job ({datetime.now(timezone.utc).isoformat()}) ---")
    season = "current"

    try:
        # --- Pilotos ---
        print("JOB F1_STANDINGS: Obteniendo clasificación de PILOTOS desde Jolpica...")
        driver_path = "current/driverStandings.json"
        raw_drivers = _jolpica_get(driver_path)
        driver_items = _parse_driver_standings(raw_drivers) or []
        _save_driver_standings_cache(season, driver_items)
        print(f"JOB F1_STANDINGS: Guardados {len(driver_items)} pilotos en caché para season={season}.")

        # --- Constructores ---
        print("JOB F1_STANDINGS: Obteniendo clasificación de CONSTRUCTORES desde Jolpica...")
        constructor_path = "current/constructorStandings.json"
        raw_constructors = _jolpica_get(constructor_path)
        constructor_items = _parse_constructor_standings(raw_constructors) or []
        _save_constructor_standings_cache(season, constructor_items)
        print(f"JOB F1_STANDINGS: Guardados {len(constructor_items)} constructores en caché para season={season}.")

        print(f"--- JOB F1_STANDINGS: FIN OK ({datetime.now(timezone.utc).isoformat()}) ---")

    except JolpicaError as e:
        # Error “esperable” de proveedor externo
        print("!!!!!!!! JOB F1_STANDINGS: ERROR Jolpica !!!!!!!!")
        print(f"Detalle: {e}")

    except Exception as e:
        # Cualquier error inesperado
        print("!!!!!!!! JOB F1_STANDINGS: ERROR inesperado !!!!!!!!")
        print(f"Detalle: {e}")

# --- check_deadlines_and_notify MODIFICADA (v2 - notifica una vez por usuario/carrera) ---
def check_deadlines_and_notify():
    """
    Tarea programada para buscar carreras cuya fecha límite está próxima
    y notificar UNA VEZ a los usuarios participantes que aún no han apostado para esa carrera.
    """
    print(f"\n--- TAREA PROGRAMADA: Iniciando check_deadlines_and_notify ({datetime.now()}) ---")
    conn = None
    cur = None 

    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("TAREA DEADLINE: Conexión DB establecida.")

        now = datetime.now(timezone.utc)
        reminder_threshold_start = now + timedelta(hours=9, minutes=35) 
        reminder_threshold_end = now + timedelta(hours=10, minutes=30)

        print(f"TAREA DEADLINE: Buscando carreras con fecha límite entre {reminder_threshold_start.isoformat()} y {reminder_threshold_end.isoformat()}")

        sql_find_races = """
            SELECT id_carrera, ano, desc_carrera, fecha_limite_apuesta
            FROM carrera
            WHERE fecha_limite_apuesta > %s AND fecha_limite_apuesta <= %s;
        """
        cur.execute(sql_find_races, (reminder_threshold_start, reminder_threshold_end))
        upcoming_races = cur.fetchall()
        print(f"TAREA DEADLINE: Encontradas {len(upcoming_races)} carreras próximas.")

        if not upcoming_races:
            cur.close(); conn.close()
            print("TAREA DEADLINE: No hay carreras en la ventana de recordatorio. Finalizando.")
            return

        for race in upcoming_races:
            id_carrera = race['id_carrera']
            ano_carrera = race['ano']
            desc_carrera = race['desc_carrera']
            fecha_limite = race['fecha_limite_apuesta']
            # Asegurar que fecha_limite tenga timezone para un formato consistente
            if fecha_limite.tzinfo is None:
                fecha_limite_aware = ZoneInfo("Europe/Madrid").localize(fecha_limite) if not isinstance(fecha_limite, datetime) else fecha_limite.replace(tzinfo=ZoneInfo("Europe/Madrid"))
            else:
                fecha_limite_aware = fecha_limite.astimezone(ZoneInfo("Europe/Madrid"))
            fecha_limite_str = fecha_limite_aware.strftime('%d/%m %H:%M')


            print(f"\nTAREA DEADLINE: Procesando carrera ID {id_carrera} ('{desc_carrera}') - Límite: {fecha_limite_str}")

            # Usuarios únicos que necesitan recordatorio para ESTA CARRERA y sus tokens
            unique_users_needing_reminder_for_race = {} # {user_id: fcm_token}

            cur.execute("SELECT id_porra FROM porra WHERE ano = %s;", (ano_carrera,))
            porras = cur.fetchall()
            if not porras:
                print(f"  TAREA DEADLINE: No hay porras para el año {ano_carrera}. Saltando carrera '{desc_carrera}'.")
                continue
            
            print(f"  TAREA DEADLINE: Encontradas {len(porras)} porras para el año {ano_carrera} para la carrera '{desc_carrera}'.")

            for porra in porras:
                id_porra = porra['id_porra']
                # print(f"    TAREA DEADLINE: Verificando porra ID {id_porra}...")

                sql_participants = "SELECT id_usuario FROM participacion WHERE id_porra = %s AND estado IN ('CREADOR', 'ACEPTADA');"
                cur.execute(sql_participants, (id_porra,))
                participants_in_porra = {row['id_usuario'] for row in cur.fetchall()}
                if not participants_in_porra:
                    # print(f"      TAREA DEADLINE: Porra {id_porra} sin participantes activos. Saltando.")
                    continue

                sql_who_bet = "SELECT DISTINCT id_usuario FROM apuesta WHERE id_porra = %s AND id_carrera = %s;"
                cur.execute(sql_who_bet, (id_porra, id_carrera))
                users_who_bet_in_porra = {row['id_usuario'] for row in cur.fetchall()}
                
                users_needing_reminder_in_porra = participants_in_porra - users_who_bet_in_porra

                if users_needing_reminder_in_porra:
                    # Obtener tokens Y ZONA HORARIA para estos usuarios
                    user_ids_to_query = list(users_needing_reminder_in_porra - unique_users_needing_reminder_for_race.keys())
                    if user_ids_to_query:
                        placeholders = ','.join(['%s'] * len(user_ids_to_query))
                        # --- CAMBIO: Añadido 'timezone' a la consulta ---
                        sql_get_tokens = f"""
                            SELECT id_usuario, fcm_token, language_code, timezone
                            FROM usuario
                            WHERE id_usuario IN ({placeholders})
                              AND fcm_token IS NOT NULL AND fcm_token != '';
                        """
                        cur.execute(sql_get_tokens, tuple(user_ids_to_query))
                        tokens_found = cur.fetchall()
                        for row in tokens_found:
                            unique_users_needing_reminder_for_race[row['id_usuario']] = {
                                "token": row['fcm_token'],
                                "lang": (row['language_code'] or 'es').strip().lower(),
                                "tz": row['timezone'] or 'Europe/Madrid' # Por defecto Madrid si no tiene
                            }
            
            # Enviar notificaciones personalizadas por hora
            if unique_users_needing_reminder_for_race:
                print(f"  TAREA DEADLINE: Enviando a {len(unique_users_needing_reminder_for_race)} usuarios.")
                
                data_payload = {
                    'tipo_notificacion': 'deadline_reminder',
                    'race_name': desc_carrera,
                    'race_id': str(id_carrera),
                    'ano_carrera': str(ano_carrera)
                }

                for user_id, info in unique_users_needing_reminder_for_race.items():
                    try:
                        token = info["token"]
                        lang = info["lang"]
                        user_tz_str = info["tz"]

                        # 1. Calcular la hora local para ESTE usuario
                        try:
                            # Intentar usar la zona horaria del usuario
                            user_tz = ZoneInfo(user_tz_str)
                        except Exception:
                            # Si falla (ej: nombre raro), usar Madrid como respaldo
                            user_tz = ZoneInfo("Europe/Madrid")
                        
                        # Convertir la fecha límite (que ya es aware) a la zona del usuario
                        local_deadline = fecha_limite_aware.astimezone(user_tz)
                        # Formatear hora: "14:00" o "10:00"
                        local_deadline_str = local_deadline.strftime('%H:%M')

                        # 2. Generar texto
                        title, body = _fcm_text('deadline_reminder', lang, race=desc_carrera, deadline=local_deadline_str)

                        message = messaging.Message(
                            notification=messaging.Notification(title=title, body=body),
                            data=data_payload,
                            token=token
                        )
                        thread_pool_executor.submit(_send_single_reminder_task, message)
                    except Exception as err:
                        print(f"ERROR envío individual deadline: {err}")
            else:
                print(f"  TAREA DEADLINE: Nadie nuevo a quien notificar para '{desc_carrera}'.")


    except psycopg2.Error as db_err:
        print(f"!!!!!!!! TAREA DEADLINE PROGRAMADA DB ERROR !!!!!!!!")
        print(f"ERROR: {db_err}")
    except Exception as e:
        print(f"!!!!!!!! TAREA DEADLINE PROGRAMADA GENERAL ERROR !!!!!!!!")
        import traceback
        traceback.print_exc()
    finally:
        if cur is not None: cur.close()
        if conn is not None: conn.close()
        print(f"--- TAREA PROGRAMADA: Finalizando check_deadlines_and_notify ({datetime.now()}) ---\n")

# --- FIN check_deadlines_and_notify MODIFICADA ---

# --- send_bulk_fcm_reminders_generic (NUEVA o versión modificada de send_bulk_fcm_reminders) ---
def send_bulk_fcm_reminders_generic(tokens, race_name, deadline_str, data_payload):
    """
    Prepara y envía notificaciones de recordatorio a una lista de tokens FCM
    en paralelo usando concurrent.futures.ThreadPoolExecutor, con un data_payload personalizable.
    """
    global thread_pool_executor

    if not tokens:
        print("RECORDATORIO FCM (Generic): No hay tokens a los que enviar.")
        return

    unique_tokens = list(set(t for t in tokens if t)) 
    if not unique_tokens:
         print("RECORDATORIO FCM (Generic): No hay tokens válidos después de limpiar.")
         return

    print(f"RECORDATORIO FCM (Generic): Preparando {len(unique_tokens)} tareas de envío para '{race_name}'...")

    submitted_count = 0
    for token in unique_tokens:
        message = messaging.Message(
            notification=messaging.Notification(
                title="⏰ ¡Última Oportunidad para Apostar!",
                body=f"La fecha límite para apostar en {race_name} es pronto ({deadline_str}). ¡No te olvides!"
            ),
            data=data_payload, # Usar el data_payload proporcionado
            token=token
        )

        try:
            if thread_pool_executor is None:
                 print("!!!!!!!! RECORDATORIO FCM ERROR (Generic): ¡¡ThreadPoolExecutor no está inicializado!! !!!!!!!!!!")
                 continue 
            thread_pool_executor.submit(_send_single_reminder_task, message) # Reutiliza _send_single_reminder_task
            submitted_count += 1
        except Exception as submit_err:
             print(f"!!!!!!!! RECORDATORIO FCM ERROR (Generic): Fallo al hacer submit para token ...{token[-10:]}. Error: {submit_err} !!!!!!!!!!")
        
    print(f"RECORDATORIO FCM (Generic): {submitted_count} tareas de envío para '{race_name}' enviadas al ThreadPoolExecutor.")

# --- FIN send_bulk_fcm_reminders_generic ---

# --- Nueva Función para Tarea en Background ---
# ¡OJO! Esta función se ejecuta en otro hilo. No tiene acceso directo
# a 'request', 'conn', 'cur' de la petición original.
# Pasamos toda la info necesaria como argumentos.
# La instancia de Firebase Admin inicializada globalmente SÍ debería ser accesible.
# --- NUEVA Función Auxiliar para Tarea de Envío Individual ---
def _send_single_reminder_task(message):
    """Tarea ejecutada por el executor para enviar UN recordatorio."""
    token = message.token # Extraer token para logging
    try:
        # ¡Importante! Asegurar inicialización de Firebase dentro de la tarea del executor
        # ya que puede ejecutarse en un contexto diferente.
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
             print(f"TASK WARN (send): Firebase NO inicializado en TAREA para token ...{token[-10:]}. Re-inicializando...")
             try:
                 # Re-usar constantes globales
                 if os.path.exists(FIREBASE_CRED_PATH):
                     cred_task = credentials.Certificate(FIREBASE_CRED_PATH)
                     firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID})
                     print(f"TASK INFO (send): Firebase inicializado en TAREA para token ...{token[-10:]}")
                 else:
                      print(f"TASK ERROR (send): Credenciales NO encontradas en TAREA para token ...{token[-10:]}")
                      return # No se puede enviar
             except ValueError:
                  print(f"TASK INFO (send): Firebase ya inicializado por otro hilo de TAREA para token ...{token[-10:]}")
             except Exception as init_error:
                  print(f"TASK ERROR (send): Fallo al inicializar Firebase en TAREA para token ...{token[-10:]}: {init_error}")
                  return # No se puede enviar

        # Enviar el mensaje individual
        response = messaging.send(message)
        print(f"TASK SUCCESS (send): Recordatorio enviado a token ...{token[-10:]}. MsgID: {response}")
        return True # Indicar éxito

    # --- Captura de errores específicos del envío ---
    except firebase_exceptions.UnregisteredError:
        print(f"TASK ERROR (send): Token ...{token[-10:]} no registrado (UnregisteredError).")
        # Podrías añadir lógica aquí para marcar el token como inválido en la BD si quisieras
        # remove_invalid_tokens_from_db([token])
    except firebase_exceptions.InvalidRegistrationTokenError:
         print(f"TASK ERROR (send): Token ...{token[-10:]} inválido (InvalidRegistrationTokenError).")
         # remove_invalid_tokens_from_db([token])
    except firebase_exceptions.FirebaseError as fb_error:
        print(f"TASK ERROR FIREBASE (send): Token ...{token[-10:]}: {fb_error} (Code: {getattr(fb_error, 'code', 'N/A')})")
    except Exception as e:
        print(f"TASK ERROR GENERAL (send): Token ...{token[-10:]}:")
        import traceback
        traceback.print_exc()
    return False # Indicar fallo
# --- FIN Función Auxiliar ---
# --- MODIFICAR ESTA FUNCIÓN en mi_api.txt ---

# --- NUEVA Función para Notificación de Estado de Apuesta ---
def send_fcm_bet_status_notification_task(user_id, fcm_token, race_name, porra_name, new_status, porra_id, ano, id_creador, tipo_porra, lang='es'):
    """
    Tarea en background para enviar notificación FCM sobre aceptación/rechazo de apuesta.
    """
    status_text = "ACEPTADA" if new_status == 'ACEPTADA' else "RECHAZADA"
    print(f"BACKGROUND TASK (Bet Status): Enviando a user {user_id} sobre apuesta {status_text}...")

    try:
        # --- INICIO: Inicialización Firebase (Igual que el resto) ---
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            try:
                if os.path.exists(FIREBASE_CRED_PATH):
                    cred_task = credentials.Certificate(FIREBASE_CRED_PATH)
                    firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID})
            except ValueError: pass
            except Exception: return
        # --- FIN ---

        if not fcm_token: return

        lang = _pick_lang(lang)
        title, body = _fcm_text('bet_status_update', lang, race=race_name, porra=porra_name, status=new_status)

        # DATOS CLAVE PARA LA NAVEGACIÓN EN FLUTTER
        data_payload = {
            'tipo_notificacion': 'bet_status_update',
            'race_name': str(race_name),
            'porra_name': str(porra_name),
            'new_status': str(new_status),
            # Datos necesarios para abrir PorraDetailScreen directamente
            'porra_id': str(porra_id),
            'ano': str(ano),
            'id_creador': str(id_creador),
            'tipo_porra': str(tipo_porra)
        }

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=data_payload,
            token=fcm_token
        )
        messaging.send(message)
        print(f"BACKGROUND TASK (Bet Status): Enviado con éxito.")

    except Exception as e:
        print(f"ERROR BACKGROUND TASK (Bet Status): {e}")

def send_fcm_new_bet_admin_notification_task(admin_id, fcm_token, race_name, porra_name, user_name, porra_id, ano, id_creador, tipo_porra, lang='es'):
    """
    NUEVA: Notifica al ADMIN de una porra que hay una nueva apuesta pendiente para revisar.
    """
    print(f"BACKGROUND TASK (New Bet Admin): Notificando admin {admin_id} de apuesta de {user_name}...")

    try:
        # --- Inicialización Firebase ---
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            try:
                if os.path.exists(FIREBASE_CRED_PATH):
                    cred_task = credentials.Certificate(FIREBASE_CRED_PATH)
                    firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID})
            except ValueError: pass
            except Exception: return
        
        if not fcm_token: return

        # Título y cuerpo "hardcoded" con soporte básico de idioma (puedes añadirlo a FCM_TEXTS si prefieres)
        lang = _pick_lang(lang)
        if lang == 'es':
            title = "🔔 Nueva apuesta pendiente"
            body = f"{user_name} ha enviado una apuesta para {race_name} en '{porra_name}'."
        elif lang == 'en':
            title = "🔔 New pending bet"
            body = f"{user_name} sent a bet for {race_name} in '{porra_name}'."
        elif lang == 'fr':
            title = "🔔 Nouveau pari en attente"
            body = f"{user_name} a envoyé un pari pour {race_name} dans '{porra_name}'."
        elif lang == 'pt':
            title = "🔔 Nova aposta pendente"
            body = f"{user_name} enviou uma aposta para {race_name} em '{porra_name}'."
        elif lang == 'ca':
            title = "🔔 Nova aposta pendent"
            body = f"{user_name} ha enviat una aposta per {race_name} a '{porra_name}'."
        else: # Fallback básico (Español)
            title = "🔔 Nueva apuesta pendiente"
            body = f"{user_name} ha enviado una apuesta para {race_name} en '{porra_name}'."

        data_payload = {
            'tipo_notificacion': 'new_bet_admin', # Tipo nuevo para Flutter
            'race_name': str(race_name),
            'porra_name': str(porra_name),
            'user_name': str(user_name),
            # Datos para navegación
            'porra_id': str(porra_id),
            'ano': str(ano),
            'id_creador': str(id_creador),
            'tipo_porra': str(tipo_porra)
        }

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=data_payload,
            token=fcm_token
        )
        messaging.send(message)
        print(f"BACKGROUND TASK (New Bet Admin): Enviado con éxito.")

    except Exception as e:
        print(f"ERROR BACKGROUND TASK (New Bet Admin): {e}")

# --- NUEVA Función para Notificación de Invitación a Porra ---
def send_fcm_invitation_notification_task(user_id_invitado, fcm_token_invitado, porra_id, porra_name, nombre_invitador, lang='es'):
    """
    Tarea en background para enviar notificación FCM sobre una nueva invitación a porra.
    """
    print(f"BACKGROUND TASK (Porra Invitation): Iniciando envío FCM para user {user_id_invitado} para unirse a '{porra_name}' invitado por {nombre_invitador}...")

    try:
        # --- INICIO: Verificación/Inicialización Firebase (Copiar bloque estándar) ---
        # Es crucial asegurarse de que Firebase esté inicializado en el contexto del hilo de esta tarea.
        # Usamos un nombre único para la app de Firebase en esta tarea para evitar conflictos si se llama concurrentemente.
        task_firebase_app_name = f'firebase-task-invitation-{user_id_invitado}-{porra_id}-{datetime.now().timestamp()}'
        
        app_initialized = False
        try:
            # Intentar obtener la app si ya fue inicializada con este nombre (poco probable pero seguro)
            firebase_admin.get_app(name=task_firebase_app_name)
            app_initialized = True
            print(f"BACKGROUND TASK (Porra Invitation): Firebase app '{task_firebase_app_name}' ya existe.")
        except ValueError: # ValueError: "The default Firebase app already exists." o "No Firebase app '[name]' has been created - call Firebase Admin SDK initialize_app() first."
            # Si no existe con ese nombre específico, intentamos inicializarla.
            # También manejamos el caso donde la app por defecto ya existe pero queremos usar una específica.
            pass # Continuar para intentar inicializar

        if not app_initialized:
            print(f"BACKGROUND TASK (Porra Invitation): Firebase app '{task_firebase_app_name}' no detectada. Intentando inicializar...")
            try:
                if os.path.exists(FIREBASE_CRED_PATH):
                    cred_task = credentials.Certificate(FIREBASE_CRED_PATH)
                    firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID}, name=task_firebase_app_name)
                    print(f"BACKGROUND TASK (Porra Invitation): Firebase app '{task_firebase_app_name}' inicializada DENTRO de la tarea.")
                else:
                    print(f"BACKGROUND TASK ERROR (Porra Invitation): No se encontró credenciales en '{FIREBASE_CRED_PATH}'. Abortando.")
                    return
            except ValueError as ve: # Esto puede ocurrir si otra tarea inicializó la default mientras tanto.
                 print(f"BACKGROUND TASK INFO (Porra Invitation): Firebase app '{task_firebase_app_name}' o la default ya fue inicializada por otro hilo: {ve}. Asumiendo que está lista.")
                 # Si la app por defecto ya existe y es la que queremos usar, esto está bien.
                 # Si queríamos una nombrada y falló porque otra nombrada igual ya existe, también está bien.
                 pass
            except Exception as init_error:
                print(f"BACKGROUND TASK ERROR (Porra Invitation): Fallo al inicializar Firebase app '{task_firebase_app_name}': {init_error}")
                return
        # --- FIN: Verificación/Inicialización Firebase ---

        if not fcm_token_invitado:
            print(f"BACKGROUND TASK (Porra Invitation): No hay token FCM para user {user_id_invitado}. Abortando.")
            return

        # --- Construir Mensaje Multiidioma ---
        lang = _pick_lang(lang)
        title, body = _fcm_text('porra_invitation', lang, porra=porra_name, inviter=nombre_invitador)

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={
                'tipo_notificacion': 'porra_invitation',
                'porra_id': str(porra_id),
                'porra_name': porra_name,
                'inviter_name': nombre_invitador
            },
            token=fcm_token_invitado,
        )
        print(f"BACKGROUND TASK (Porra Invitation): Mensaje construido para token ...{fcm_token_invitado[-10:]}")

        # --- Envío del Mensaje ---
        # Usar la app específica si fue inicializada, o la default si el intento de nombrada usó la default.
        try:
            current_app = firebase_admin.get_app(name=task_firebase_app_name)
        except ValueError:
            current_app = firebase_admin.get_app() # Fallback a la app por defecto

        response = messaging.send(message, app=current_app)
        print(f"--- BACKGROUND TASK SUCCESS (Porra Invitation)! MsgID: {response} ---")

    except firebase_exceptions.FirebaseError as fb_error: # Errores específicos de Firebase
        print(f"!!!!!!!! BACKGROUND TASK FIREBASE ERROR (Porra Invitation) !!!!!!!!")
        print(f"Error: {fb_error} (Code: {getattr(fb_error, 'code', 'N/A')})")
        if fb_error.code == 'messaging/registration-token-not-registered':
            print(f"BACKGROUND TASK (Porra Invitation): Token {fcm_token_invitado[:10]}... no registrado. Considerar eliminarlo de la BD.")
            # Aquí podrías llamar a una función para limpiar el token de la BD
            # remove_invalid_fcm_tokens([fcm_token_invitado])
    except Exception as e:
        print(f"!!!!!!!! BACKGROUND TASK GENERAL ERROR (Porra Invitation) !!!!!!!!")
        import traceback
        traceback.print_exc()
    finally:
        print(f"BACKGROUND TASK (Porra Invitation): Finalizado para user {user_id_invitado}, porra '{porra_name}'.")

# --- FIN NUEVA Función ---

# --- NUEVA Función para Notificación de Resultado Listo ---
def send_fcm_result_notification_task(user_id, fcm_token, race_name, porra_id, lang='es'):
    """
    Tarea que se ejecuta en background para enviar notificación FCM
    cuando un resultado de carrera está listo.
    """
    print(f"BACKGROUND TASK (Result Ready): Iniciando envío FCM para user {user_id}, carrera '{race_name}'...")
    try:
        # --- INICIO: Verificación/Inicialización Firebase (igual que en otras tareas) ---
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            print(f"BACKGROUND TASK (Result Ready): Firebase Admin SDK no detectado. Intentando inicializar...")
            # (Aquí iría la misma lógica de inicialización que en send_fcm_notification_task)
            # ... (copiar bloque try/except de inicialización de Firebase de la otra función) ...
            # Es importante asegurarse que Firebase esté inicializado en el contexto del hilo
            try:
                firebase_cred_path_task = os.environ.get('FIREBASE_CRED_PATH', '/ruta/segura/en/tu/nas/firebase-adminsdk-xxxx.json')
                if os.path.exists(firebase_cred_path_task):
                    cred_task = credentials.Certificate(firebase_cred_path_task)
                    # Asegurar inicialización con Project ID si es necesario (copiar de la inicialización principal)
                    firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID}, name=f'firebase-task-{user_id}-{porra_id}') # Usar nombre único si hay problemas
                    print("BACKGROUND TASK (Result Ready): Firebase inicializado DENTRO de la tarea.")
                else:
                    print(f"BACKGROUND TASK ERROR (Result Ready): No se encontró credenciales en '{firebase_cred_path_task}'. Abortando.")
                    return
            except ValueError:
                 # Ya inicializado por otro hilo, probablemente seguro continuar
                 print(f"BACKGROUND TASK INFO (Result Ready): Firebase ya inicializado por otro hilo.")
                 pass
            except Exception as init_error:
                print(f"BACKGROUND TASK ERROR (Result Ready): Fallo al inicializar Firebase: {init_error}")
                return
        # --- FIN: Verificación/Inicialización Firebase ---

        if not fcm_token:
            print(f"BACKGROUND TASK (Result Ready): No hay token FCM para user {user_id}. Abortando.")
            return

        # Construir el mensaje específico para resultado listo
        lang = _pick_lang(lang)
        title, body = _fcm_text('result_ready', lang, race=race_name)

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={
                'tipo_notificacion': 'result_ready',
                'race_name': race_name,
                'porra_id': str(porra_id)
            },
            token=fcm_token,
        )
        print(f"BACKGROUND TASK (Result Ready): Mensaje construido. Llamando a messaging.send() para token ...{fcm_token[-10:]}")

        # Envío del mensaje (igual que en trofeos)
        response = messaging.send(message)
        print(f"--- BACKGROUND TASK SUCCESS (Result Ready)! ---")
        print(f"Response (Message Name): {response}")

    except firebase_admin.messaging.ApiCallError as fcm_api_error:
        print(f"!!!!!!!! BACKGROUND TASK FCM API ERROR (Result Ready) !!!!!!!!")
        print(f"ERROR: Código={fcm_api_error.code}, Mensaje='{fcm_api_error.message}'")
    except Exception as e:
        print(f"!!!!!!!! BACKGROUND TASK GENERAL ERROR (Result Ready) !!!!!!!!")
        import traceback
        traceback.print_exc()
    finally:
        print(f"BACKGROUND TASK (Result Ready): Finalizado para user {user_id}, carrera '{race_name}'.")
# --- FIN NUEVA Función ---

# --- NUEVA Función para Notificación de Próxima Carrera Disponible ---
def send_fcm_next_race_notification_task(user_id, fcm_token, current_race_name, next_race_name, porra_id, next_race_id, lang='es'):
    """
    Tarea que se ejecuta en background para enviar notificación FCM
    cuando la siguiente carrera está disponible para apostar.
    """
    print(f"BACKGROUND TASK (Next Race Ready): Iniciando envío FCM para user {user_id}, siguiente carrera '{next_race_name}'...")
    try:
        # --- INICIO: Verificación/Inicialización Firebase (igual que en otras tareas) ---
        # (Copia el bloque completo de inicialización de Firebase que usas en
        #  send_fcm_result_notification_task o send_fcm_notification_task aquí
        #  para asegurar que Firebase esté listo en el hilo de la tarea)
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            print(f"BACKGROUND TASK (Next Race Ready): Firebase Admin SDK no detectado. Intentando inicializar...")
            try:
                firebase_cred_path_task = os.environ.get('FIREBASE_CRED_PATH', '/ruta/segura/en/tu/nas/firebase-adminsdk-xxxx.json')
                if os.path.exists(firebase_cred_path_task):
                    cred_task = credentials.Certificate(firebase_cred_path_task)
                    # Asegurar inicialización con Project ID
                    firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID}, name=f'firebase-task-nextrace-{user_id}-{porra_id}') # Nombre único
                    print("BACKGROUND TASK (Next Race Ready): Firebase inicializado DENTRO de la tarea.")
                else:
                    print(f"BACKGROUND TASK ERROR (Next Race Ready): No se encontró credenciales en '{firebase_cred_path_task}'. Abortando.")
                    return
            except ValueError:
                 print(f"BACKGROUND TASK INFO (Next Race Ready): Firebase ya inicializado por otro hilo.")
                 pass # Ya inicializado
            except Exception as init_error:
                print(f"BACKGROUND TASK ERROR (Next Race Ready): Fallo al inicializar Firebase: {init_error}")
                return
        # --- FIN: Verificación/Inicialización Firebase ---

        if not fcm_token:
            print(f"BACKGROUND TASK (Next Race Ready): No hay token FCM para user {user_id}. Abortando.")
            return

        # Construir el mensaje específico para próxima carrera
        lang = _pick_lang(lang)
        title, body = _fcm_text('next_race_available', lang, current=current_race_name, next=next_race_name)

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={
                'tipo_notificacion': 'next_race_available',
                'current_race_name': current_race_name,
                'next_race_name': next_race_name,
                'porra_id': str(porra_id),
                'next_race_id': str(next_race_id)
            },
            token=fcm_token,
        )
        print(f"BACKGROUND TASK (Next Race Ready): Mensaje construido. Llamando a messaging.send() para token ...{fcm_token[-10:]}")

        # Envío del mensaje
        response = messaging.send(message)
        print(f"--- BACKGROUND TASK SUCCESS (Next Race Ready)! ---")
        print(f"Response (Message Name): {response}")

    except firebase_admin.messaging.ApiCallError as fcm_api_error:
        print(f"!!!!!!!! BACKGROUND TASK FCM API ERROR (Next Race Ready) !!!!!!!!")
        print(f"ERROR: Código={fcm_api_error.code}, Mensaje='{fcm_api_error.message}'")
    except Exception as e:
        print(f"!!!!!!!! BACKGROUND TASK GENERAL ERROR (Next Race Ready) !!!!!!!!")
        import traceback
        traceback.print_exc()
    finally:
        print(f"BACKGROUND TASK (Next Race Ready): Finalizado para user {user_id}, siguiente carrera '{next_race_name}'.")
# --- FIN NUEVA Función ---

# --- send_fcm_betting_closed_notification_task MODIFICADA (v2 - genérica por carrera) ---
def send_fcm_betting_closed_notification_task(user_id, fcm_token, race_id, race_name, ano_carrera, lang='es'):
    """
    Tarea en background para enviar notificación FCM genérica cuando las apuestas para una carrera han cerrado.
    """
    print(f"BACKGROUND TASK (Betting Closed - Generic): User {user_id}, Carrera '{race_name}' (Año: {ano_carrera})...")

    try:
        # --- INICIO: Verificación/Inicialización Firebase (Bloque estándar) ---
        task_firebase_app_name = f'firebase-task-bettingclosed-generic-{user_id}-{race_id}-{datetime.now().timestamp()}'
        app_initialized = False
        try:
            firebase_admin.get_app(name=task_firebase_app_name)
            app_initialized = True
        except ValueError:
            pass

        if not app_initialized:
            try:
                if os.path.exists(FIREBASE_CRED_PATH):
                    cred_task = credentials.Certificate(FIREBASE_CRED_PATH)
                    firebase_admin.initialize_app(cred_task, {'projectId': FIREBASE_PROJECT_ID}, name=task_firebase_app_name)
                    print(f"BACKGROUND TASK (Betting Closed - Generic): Firebase app '{task_firebase_app_name}' inicializada.")
                else:
                    print(f"BACKGROUND TASK ERROR (Betting Closed - Generic): Credenciales no encontradas '{FIREBASE_CRED_PATH}'.")
                    return
            except ValueError as ve:
                 print(f"BACKGROUND TASK INFO (Betting Closed - Generic): Firebase app ya inicializada: {ve}.")
                 pass 
            except Exception as init_error:
                print(f"BACKGROUND TASK ERROR (Betting Closed - Generic): Fallo al inicializar Firebase app '{task_firebase_app_name}': {init_error}")
                return
        # --- FIN: Verificación/Inicialización Firebase ---

        if not fcm_token:
            print(f"BACKGROUND TASK (Betting Closed - Generic): No hay token FCM para user {user_id}. Abortando.")
            return

        lang = _pick_lang(lang)
        title, body = _fcm_text('betting_closed', lang, race=race_name)

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={
                'tipo_notificacion': 'betting_closed',
                'race_id': str(race_id),
                'race_name': race_name,
                'ano_carrera': str(ano_carrera)
            },
            token=fcm_token
        )
        
        try:
            current_app = firebase_admin.get_app(name=task_firebase_app_name)
        except ValueError:
            current_app = firebase_admin.get_app()

        response = messaging.send(message, app=current_app)
        print(f"--- BACKGROUND TASK SUCCESS (Betting Closed - Generic)! User: {user_id}, Race: {race_name}, MsgID: {response} ---")

    except firebase_exceptions.FirebaseError as fb_error:
        print(f"!!!!!!!! BACKGROUND TASK FIREBASE ERROR (Betting Closed - Generic) !!!!!!!!")
        print(f"User {user_id}, Token ...{fcm_token[-10:] if fcm_token else 'N/A'}. Error: {fb_error} (Code: {getattr(fb_error, 'code', 'N/A')})")
    except Exception as e:
        print(f"!!!!!!!! BACKGROUND TASK GENERAL ERROR (Betting Closed - Generic) !!!!!!!!")
        print(f"User {user_id}, Token ...{fcm_token[-10:] if fcm_token else 'N/A'}.")
        import traceback
        traceback.print_exc()
    finally:
        print(f"BACKGROUND TASK (Betting Closed - Generic): Finalizado para user {user_id}, carrera '{race_name}'.")
# --- FIN send_fcm_betting_closed_notification_task MODIFICADA ---

# --- check_betting_closed_and_notify MODIFICADA (v2 - notifica una vez por usuario/carrera) ---
def check_betting_closed_and_notify():
    """
    Tarea programada para buscar carreras cuya fecha límite de apuesta acaba de pasar
    (hace ~1 hora) y notificar UNA VEZ a los participantes para esa carrera.
    """
    print(f"\n--- TAREA PROGRAMADA: Iniciando check_betting_closed_and_notify ({datetime.now()}) ---")
    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("TAREA CIERRE: Conexión DB establecida.")

        now_utc = datetime.now(timezone.utc)
        # Ajusta esta ventana según la frecuencia del job. Si corre cada 30 min:
        deadline_passed_since = now_utc - timedelta(hours=1, minutes=15) 
        deadline_passed_until = now_utc - timedelta(minutes=45)     

        print(f"TAREA CIERRE: Buscando carreras con fecha_limite_apuesta entre {deadline_passed_since.isoformat()} y {deadline_passed_until.isoformat()}")
        # --- Logging Adicional para Depuración de Ventana ---
        # print(f"TAREA CIERRE DEBUG: now_utc: {now_utc.isoformat()}")
        # print(f"TAREA CIERRE DEBUG: Carrera de prueba manual (UTC): {(datetime(2025, 5, 22, 16, 16, 0, tzinfo=ZoneInfo('Europe/Madrid'))).astimezone(timezone.utc).isoformat()}")
        # --- Fin Logging Adicional ---

        sql_find_races = """
            SELECT id_carrera, ano, desc_carrera, fecha_limite_apuesta
            FROM carrera
            WHERE fecha_limite_apuesta > %s AND fecha_limite_apuesta <= %s;
        """
        cur.execute(sql_find_races, (deadline_passed_since, deadline_passed_until))
        recently_closed_races = cur.fetchall()
        print(f"TAREA CIERRE: Encontradas {len(recently_closed_races)} carreras cuyo plazo cerró hace ~1 hora.")

        if not recently_closed_races:
            cur.close(); conn.close()
            print("TAREA CIERRE: No hay carreras en la ventana de notificación de cierre. Finalizando.")
            return

        global thread_pool_executor
        if thread_pool_executor is None:
            print("!!!!!!!! TAREA CIERRE ERROR: ThreadPoolExecutor no inicializado. No se pueden enviar notificaciones. !!!!!!!!!!")
            cur.close(); conn.close()
            return

        for race in recently_closed_races:
            id_carrera = race['id_carrera']
            ano_carrera = race['ano']
            desc_carrera = race['desc_carrera']
            print(f"\nTAREA CIERRE: Procesando carrera ID {id_carrera} ('{desc_carrera}')")

            # Usuarios únicos a notificar para ESTA CARRERA y sus tokens
            unique_users_to_notify_for_race = {} # {user_id: fcm_token}

            # Obtener todos los participantes de TODAS las porras de ese año
            # que tengan token FCM y que estén activos.
            sql_all_participants_year_with_tokens = """
                SELECT DISTINCT u.id_usuario, u.fcm_token, u.language_code
                FROM usuario u
                JOIN participacion pa ON u.id_usuario = pa.id_usuario
                JOIN porra po ON pa.id_porra = po.id_porra
                WHERE po.ano = %s
                  AND pa.estado IN ('CREADOR', 'ACEPTADA')
                  AND u.fcm_token IS NOT NULL AND u.fcm_token != '';
            """
            cur.execute(sql_all_participants_year_with_tokens, (ano_carrera,))
            all_relevant_users_with_tokens = cur.fetchall()

            if not all_relevant_users_with_tokens:
                print(f"  TAREA CIERRE: No hay participantes con tokens para el año {ano_carrera} para la carrera '{desc_carrera}'. Saltando.")
                continue
            
            print(f"  TAREA CIERRE: {len(all_relevant_users_with_tokens)} participantes potenciales con token para el año {ano_carrera}.")

            for user_data in all_relevant_users_with_tokens:
                user_id = user_data['id_usuario']
                fcm_token = user_data['fcm_token']
                user_lang = (user_data.get('language_code') or 'es').strip().lower()
                if user_id not in unique_users_to_notify_for_race: # Asegurar unicidad
                    unique_users_to_notify_for_race[user_id] = {"token": fcm_token, "lang": user_lang}
            
            if unique_users_to_notify_for_race:
                print(f"  TAREA CIERRE: Enviando notificación de cierre de apuestas para '{desc_carrera}' a {len(unique_users_to_notify_for_race)} usuarios únicos.")
                for user_id, info in unique_users_to_notify_for_race.items():
                    try:
                        thread_pool_executor.submit(
                            send_fcm_betting_closed_notification_task, # La versión genérica
                            user_id,
                            info["token"],
                            id_carrera,
                            desc_carrera,
                            ano_carrera, # Pasamos año en lugar de porra_id/porra_name
                            info["lang"]
                        )
                    except Exception as submit_err:
                        print(f"!!!!!!!! TAREA CIERRE ERROR SUBMIT (Generic): User {user_id}, Carrera {id_carrera}. Error: {submit_err} !!!!!!!!!!")
            else:
                print(f"  TAREA CIERRE: No hay usuarios únicos con tokens para notificar para la carrera '{desc_carrera}'.")
            
            print(f"  TAREA CIERRE: Notificaciones para carrera '{desc_carrera}' (si alguna) enviadas al executor.")

    except psycopg2.Error as db_err:
        print(f"!!!!!!!! TAREA CIERRE PROGRAMADA DB ERROR !!!!!!!!")
        print(f"ERROR: {db_err}")
    except Exception as e:
        print(f"!!!!!!!! TAREA CIERRE PROGRAMADA GENERAL ERROR !!!!!!!!")
        import traceback
        traceback.print_exc()
    finally:
        if cur is not None: cur.close()
        if conn is not None: conn.close()
        print(f"--- TAREA PROGRAMADA: Finalizando check_betting_closed_and_notify ({datetime.now()}) ---\n")

# --- FIN check_betting_closed_and_notify MODIFICADA ---

# --- FIN Nueva Función ---

def send_fcm_notification_task(user_id, fcm_token, trofeo_codigo, trofeo_nombre, trofeo_desc, lang='es'):
    """Tarea que se ejecuta en background para enviar notificación FCM."""
    print(f"BACKGROUND TASK: Iniciando envío FCM para user {user_id}, trofeo '{trofeo_codigo}'...")
    try:
        # --- INICIO: Añadir inicialización si es necesario ---
        # Verifica si la app por defecto de Firebase ya está inicializada EN ESTE CONTEXTO
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            print(f"BACKGROUND TASK: Firebase Admin SDK no detectado en este contexto. Intentando inicializar...")
            try:
                # Reutiliza la ruta de tus credenciales (asegúrate que sea accesible desde el worker)
                firebase_cred_path_task = os.environ.get('FIREBASE_CRED_PATH', '/ruta/segura/en/tu/nas/firebase-adminsdk-xxxx.json')
                if os.path.exists(firebase_cred_path_task):
                    cred_task = credentials.Certificate(firebase_cred_path_task)
                    # Puedes darle un nombre único a esta inicialización si quieres evitar conflictos,
                    # aunque inicializar la default suele ser seguro si se hace tras verificar.
                    firebase_admin.initialize_app(cred_task)
                    print("BACKGROUND TASK: Firebase Admin SDK inicializado DENTRO de la tarea.")
                else:
                    print(f"BACKGROUND TASK ERROR: No se encontró el archivo de credenciales en '{firebase_cred_path_task}' dentro de la tarea. Abortando.")
                    return # No se puede continuar sin credenciales
            except Exception as init_error:
                print(f"BACKGROUND TASK ERROR: Fallo al inicializar Firebase Admin SDK DENTRO de la tarea: {init_error}")
                import traceback
                traceback.print_exc()
                return # No se puede continuar si falla la inicialización
        # --- FIN: Añadir inicialización si es necesario ---

        # El resto del código de la función sigue igual...
        if not fcm_token:
            print(f"BACKGROUND TASK: No hay token FCM para user {user_id}. Abortando.")
            return

        # Ya no necesitas la comprobación explícita de _apps aquí si la inicialización anterior funciona

        # --- Construir Mensaje (multiidioma) ---
        lang = _pick_lang(lang)
        # Priorizamos el nombre del trofeo; si no hay, caemos al código
        trophy_label = (trofeo_nombre or trofeo_codigo or "").strip()
        title, body = _fcm_text('trophy_unlocked', lang, trophy=trophy_label)

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={
                'tipo_notificacion': 'trophy_unlocked',
                'trofeo_codigo': str(trofeo_codigo or ''),
                'trofeo_nombre': trofeo_nombre or "",
                'trofeo_desc': trofeo_desc or ""
            },
            token=fcm_token
        )
        print(f"BACKGROUND TASK: Mensaje construido. Llamando a messaging.send() para token ...{fcm_token[-10:]}")

        response = messaging.send(message)
        print(f"--- BACKGROUND TASK SUCCESS! ---")
        print(f"Response (Message Name): {response}")

    except firebase_admin.messaging.ApiCallError as fcm_api_error:
         print(f"!!!!!!!! BACKGROUND TASK FCM API ERROR !!!!!!!!")
         print(f"ERROR: Código={fcm_api_error.code}, Mensaje='{fcm_api_error.message}'")
         # Considera añadir lógica para manejar errores específicos de FCM aquí
    except Exception as e:
        print(f"!!!!!!!! BACKGROUND TASK GENERAL ERROR !!!!!!!!")
        import traceback
        traceback.print_exc()
    finally:
         print(f"BACKGROUND TASK: Finalizado para user {user_id}, trofeo '{trofeo_codigo}'.")

# --- FIN FUNCIÓN MODIFICADA ---

# --- NUEVA Función Auxiliar ---
def get_expected_driver_count(ano, cur):
    """Consulta la BD para obtener el número de pilotos definidos para un año."""
    try:
        cur.execute("SELECT COUNT(*) FROM piloto_temporada WHERE ano = %s;", (ano,))
        count_result = cur.fetchone()
        if count_result:
            return count_result[0] # Devuelve el conteo
        else:
            return 0 # O lanza un error si prefieres que sea obligatorio tener pilotos
    except Exception as e:
        print(f"Error obteniendo conteo de pilotos para año {ano}: {e}")
        # Lanza el error para que la función que llama lo maneje
        raise ValueError(f"No se pudo determinar el número de pilotos para el año {ano}.")

# --- Función calcular_puntuaciones_api (MODIFICADA v3 - Manejo robusto de listas) ---
def calcular_puntuaciones_api(resultado_carrera_dict, lista_apuestas_dicts):
    """
    Calcula puntuaciones adaptado para la API (con listas de posiciones JSONB).
    Compara piloto a piloto y es robusto ante posible diferencia de longitud
    entre resultado y apuesta (aunque no debería ocurrir con la validación actual).

    resultado_carrera_dict: Dict con 'posiciones' (lista códigos P1-PN) y 'vrapida'.
    lista_apuestas_dicts: Lista de Dicts [{'id_usuario': id, 'posiciones': [...], 'vrapida': ...}, ...]
    Devuelve: Lista de dicts [{'id_usuario': id, 'puntos': pts}, ...]
    """
    lista_puntuaciones = []

    # Validación básica del resultado (igual que antes)
    if not resultado_carrera_dict or \
       'posiciones' not in resultado_carrera_dict or \
       'vrapida' not in resultado_carrera_dict or \
       not isinstance(resultado_carrera_dict['posiciones'], list) or \
       not resultado_carrera_dict['posiciones']:
        print("Error: Formato de resultado de carrera inválido para calcular.")
        return []

    resultado_posiciones_codigos = resultado_carrera_dict['posiciones'] # Lista códigos P1-PN resultado
    resultado_vrapida = resultado_carrera_dict['vrapida']
    num_pilotos_resultado = len(resultado_posiciones_codigos)

    # Mapa para búsqueda rápida de la posición REAL (índice 0 a N-1) de cada piloto en el resultado
    posicion_resultado_map = {
        piloto: index for index, piloto in enumerate(resultado_posiciones_codigos)
    }
    print(f"DEBUG [calcular_puntuaciones]: Resultado con {num_pilotos_resultado} pilotos. VR: {resultado_vrapida}")

    # Iterar sobre cada apuesta recibida
    for apuesta_usuario in lista_apuestas_dicts:
        try:
            id_usuario = apuesta_usuario['id_usuario']
            apuesta_posiciones_codigos = apuesta_usuario['posiciones'] # Lista códigos P1-PN apuesta
            apuesta_vrapida = apuesta_usuario['vrapida']
            num_pilotos_apuesta = len(apuesta_posiciones_codigos)
            puntuacion = 0

            # Advertir si las longitudes no coinciden (gracias a la validación, no debería pasar)
            if num_pilotos_apuesta != num_pilotos_resultado:
                print(f"WARN [calcular_puntuaciones]: Longitud apuesta ({num_pilotos_apuesta}) user {id_usuario} != resultado ({num_pilotos_resultado}). Calculando con pilotos comunes.")

            # --- Lógica de Puntuación Modificada ---
            # Iterar sobre las POSICIONES DE LA APUESTA (0 a N-1)
            for i, piloto_apostado in enumerate(apuesta_posiciones_codigos):
                # Buscar la posición REAL de este piloto en el MAPA del resultado
                posicion_real_idx = posicion_resultado_map.get(piloto_apostado)

                # Si el piloto apostado SÍ está en el resultado oficial...
                if posicion_real_idx is not None:
                    posicion_apostada_idx = i # Índice (0 a N-1) donde el usuario puso al piloto

                    # Calcular puntos según la diferencia de índices
                    if posicion_apostada_idx == posicion_real_idx:
                        puntuacion += 10 # Acierto exacto
                    elif abs(posicion_apostada_idx - posicion_real_idx) == 1:
                        puntuacion += 5 # Acierto +/- 1 posición
                    # else: 0 puntos por diferencia > 1
                # else: Si el piloto apostado no está en el resultado, 0 puntos por él.

            # Puntos por vuelta rápida (sin cambios)
            if apuesta_vrapida == resultado_vrapida:
                puntuacion += 10

            lista_puntuaciones.append({"id_usuario": id_usuario, "puntos": puntuacion})
            # print(f"DEBUG [calcular_puntuaciones]: User {id_usuario} -> {puntuacion} puntos.") # Log opcional

        except KeyError as ke: # Manejo de errores (sin cambios)
            print(f"Error de clave procesando apuesta user {apuesta_usuario.get('id_usuario')}: Falta {ke}")
            continue
        except Exception as e_calc: # Manejo de errores (sin cambios)
            print(f"Error inesperado calculando puntos user {apuesta_usuario.get('id_usuario')}: {e_calc}")
            continue

    return lista_puntuaciones
# --- FIN calcular_puntuaciones_api MODIFICADA ---

# --- Función Auxiliar para Otorgar Trofeos (v4 - Con Notificaciones FCM) ---
# --- INICIO: Función _award_trophy MODIFICADA con Logging Extremo (mi_api.txt) ---
# REEMPLAZA ESTA FUNCIÓN COMPLETA
def _award_trophy(user_id, trofeo_codigo, conn, cur, detalles=None):
    """
    Intenta otorgar un trofeo a un usuario si aún no lo tiene.
    Si lo otorga, lanza una tarea en background para enviar la notificación FCM.
    (VERSIÓN CON LOGGING DETALLADO PARA DEBUG)
    """
    print(f"\n--- FN: _award_trophy ---") # LOG Inicio función
    print(f"DEBUG [_award_trophy]: START - Otorgando '{trofeo_codigo}' a user {user_id}")

    if not user_id or not trofeo_codigo:
        print(f"DEBUG [_award_trophy]: EXIT - User ID o Trofeo Código inválidos.")
        return False

    try:
        # Obtener ID y detalles del trofeo
        print(f"DEBUG [_award_trophy]: Querying trofeo '{trofeo_codigo}'...")
        cur.execute("SELECT id_trofeo, nombre, descripcion FROM trofeo WHERE codigo_trofeo = %s AND activo = TRUE;", (trofeo_codigo,))
        trofeo_row = cur.fetchone()
        if not trofeo_row:
            print(f"WARN [_award_trophy]: Trofeo '{trofeo_codigo}' NO encontrado o NO activo.")
            print(f"--- FN: _award_trophy --- END (Trofeo no encontrado/activo)\n")
            return False
        id_trofeo, trofeo_nombre, trofeo_desc = trofeo_row['id_trofeo'], trofeo_row['nombre'], trofeo_row['descripcion']
        print(f"DEBUG [_award_trophy]: Trofeo ID: {id_trofeo}, Nombre: '{trofeo_nombre}'")

        # Comprobar si el usuario ya tiene el trofeo
        print(f"DEBUG [_award_trophy]: Checking if user {user_id} already has trofeo {id_trofeo}...")
        cur.execute("SELECT 1 FROM usuario_trofeo WHERE id_usuario = %s AND id_trofeo = %s;", (user_id, id_trofeo))
        if cur.fetchone():
            print(f"DEBUG [_award_trophy]: Usuario {user_id} YA tiene el trofeo '{trofeo_codigo}'.")
            print(f"--- FN: _award_trophy --- END (Ya lo tenía)\n")
            return False # Ya lo tiene, no hacemos nada más

        # No lo tiene, proceder a insertar
        print(f"DEBUG [_award_trophy]: Usuario {user_id} NO tiene trofeo '{trofeo_codigo}'. Preparando INSERT.")
        detalles_json = None
        if detalles and isinstance(detalles, dict):
            try:
                # Serializador simple para fechas
                def default_serializer(o):
                    if isinstance(o, (datetime, date)): return o.isoformat()
                    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
                detalles_json = json.dumps(detalles, default=default_serializer)
                print(f"DEBUG [_award_trophy]: Detalles adicionales serializados: {detalles_json}")
            except TypeError as e:
                print(f"ERROR [_award_trophy]: Error serializando detalles: {e}")
                detalles_json = json.dumps({"error": "serializacion fallida"})

        # Insertar en usuario_trofeo
        sql_insert = "INSERT INTO usuario_trofeo (id_usuario, id_trofeo, detalles_adicionales) VALUES (%s, %s, %s);"
        cur.execute(sql_insert, (user_id, id_trofeo, detalles_json))
        # Verificar si la inserción fue exitosa (opcional pero bueno)
        if cur.rowcount == 1:
             print(f"DEBUG [_award_trophy]: INSERT exitoso para user {user_id}, trofeo {id_trofeo}.")
        else:
             print(f"WARN [_award_trophy]: INSERT para trofeo {id_trofeo} / user {user_id} no afectó filas (¿raro?).")
             # Considerar si devolver False aquí o continuar con la notificación igualmente

        # --- Lógica de Notificación FCM ASÍNCRONA ---
        fcm_token = None
        user_lang = 'es'
        try:
            # Obtener token FCM y language_code del usuario
            print(f"DEBUG [_award_trophy]: Querying FCM token + language_code for user {user_id}...")
            cur.execute("SELECT fcm_token, language_code FROM usuario WHERE id_usuario = %s;", (user_id,))
            user_row = cur.fetchone()

            if user_row and user_row.get('fcm_token') and (user_row['fcm_token'] or '').strip():
                fcm_token = user_row['fcm_token'].strip()
                user_lang = (user_row.get('language_code') or 'es').strip().lower()
                print(f"DEBUG [_award_trophy]: Token FCM ok (...{fcm_token[-10:]}) y lang='{user_lang}' para user {user_id}.")
            else:
                print(f"WARN [_award_trophy]: No hay token FCM válido para el usuario {user_id}. No se enviará notificación.")

            # Si tenemos token, ENVIAMOS LA TAREA al executor
            if fcm_token:
                print(f"DEBUG [_award_trophy]: Enviando tarea FCM (trofeo) al executor para user {user_id}, trofeo '{trofeo_codigo}'...")
                global thread_pool_executor
                if thread_pool_executor is None:
                    print(f"!!!!!!!! ERROR CRÍTICO [_award_trophy]: El executor global no está inicializado!! No se puede enviar tarea FCM. !!!!!!!!!!")
                else:
                    # *** Ahora la tarea acepta 'lang' como último parámetro ***
                    thread_pool_executor.submit(
                        send_fcm_notification_task,
                        user_id,
                        fcm_token,
                        trofeo_codigo,
                        trofeo_nombre,
                        trofeo_desc,
                        user_lang  # <-- AÑADIDO
                    )
                    print(f"DEBUG [_award_trophy]: Tarea FCM enviada al executor (se ejecutará en background).")
            else:
                print(f"DEBUG [_award_trophy]: No se envía tarea FCM (no hay token).")

        except Exception as e_fcm_logic:
            # Error al obtener token o al hacer submit (no al ejecutar la tarea)
            print(f"ERROR [_award_trophy]: Excepción en lógica FCM previo al envío de tarea FCM para user {user_id}. Error: {e_fcm_logic}")
            import traceback; traceback.print_exc()
        # --- FIN Lógica de Notificación ---

        print(f"--- FN: _award_trophy --- END (Trofeo otorgado)\n")
        return True # Trofeo insertado (independientemente de si se envió notif)

    except psycopg2.Error as db_err:
        print(f"ERROR DB [_award_trophy]: Error DB otorgando '{trofeo_codigo}' a user {user_id}. Error: {db_err}")
        print(f"--- FN: _award_trophy --- END (Error DB)\n")
        return False
    except Exception as e:
        print(f"ERROR General [_award_trophy]: Otorgando '{trofeo_codigo}' a user {user_id}. Error: {e}")
        import traceback; traceback.print_exc()
        print(f"--- FN: _award_trophy --- END (Error General)\n")
        return False

# --- FIN: Función _award_trophy MODIFICADA con Logging Extremo ---
# --- FIN Función Auxiliar MODIFICADA ---

# --- FUNCIONES AUXILIARES CLASIFICACIÓN (NUEVO FASE 2) ---

def _get_q_rules(ano, cur):
    """Obtiene las reglas de Q1/Q2/Q3 para un año específico."""
    cur.execute("SELECT q1_eliminated, q2_eliminated, total_drivers FROM configuracion_q WHERE ano = %s;", (str(ano),))
    row = cur.fetchone()
    if not row:
        # Fallback por defecto (Standard F1 actual: 20 pilotos, 5 fuera en Q1, 5 fuera en Q2)
        return {"q1": 5, "q2": 5, "total": 20}
    return {"q1": row['q1_eliminated'], "q2": row['q2_eliminated'], "total": row['total_drivers']}

def _calculate_qualifying_points(apuesta_clasificacion, resultado_clasificacion, q_rules):
    """
    Calcula los puntos de clasificación (10/5/1).
    apuesta_clasificacion: Lista de códigos ['VER', 'HAM', ...]
    resultado_clasificacion: Lista de códigos ['VER', 'HAM', ...] (Del resultado oficial)
    q_rules: Dict con q1, q2, total.
    """
    puntos = 0
    if not apuesta_clasificacion or not resultado_clasificacion:
        return 0

    # Definir rangos (índices base 0)
    # Q3: 0 a (Total - Q1 - Q2 - 1)
    # Q2: (Total - Q1 - Q2) a (Total - Q1 - 1)
    # Q1: (Total - Q1) a (Total - 1)
    
    cutoff_q3 = q_rules['total'] - q_rules['q1'] - q_rules['q2'] # Ej: 20 - 5 - 5 = 10. Q3 es índices 0-9
    cutoff_q2 = q_rules['total'] - q_rules['q1']             # Ej: 20 - 5 = 15. Q2 es índices 10-14. Q1 es 15-19

    # Mapa de posición real para búsqueda rápida: {'VER': 0, 'HAM': 1...}
    mapa_resultado = {piloto: idx for idx, piloto in enumerate(resultado_clasificacion)}

    for idx_apuesta, piloto in enumerate(apuesta_clasificacion):
        if piloto not in mapa_resultado:
            continue # El piloto no corrió o no está en resultados
            
        idx_real = mapa_resultado[piloto]

        # 1. Poleman (Solo si el usuario lo puso primero Y quedó primero)
        if idx_apuesta == 0 and idx_real == 0:
            puntos += 10
            continue # No suma los otros puntos si ya sumó 10

        # 2. Posición Exacta (para el resto, o si no fue pole)
        if idx_apuesta == idx_real:
            puntos += 5
            continue

        # 3. Acierto de Rango (Q1/Q2/Q3)
        # Determinar rango apuesta
        rango_apuesta = 0 # 3=Q3, 2=Q2, 1=Q1
        if idx_apuesta < cutoff_q3: rango_apuesta = 3
        elif idx_apuesta < cutoff_q2: rango_apuesta = 2
        else: rango_apuesta = 1

        # Determinar rango real
        rango_real = 0
        if idx_real < cutoff_q3: rango_real = 3
        elif idx_real < cutoff_q2: rango_real = 2
        else: rango_real = 1

        if rango_apuesta == rango_real:
            puntos += 1

    return puntos

@app.route('/api/config/q-rules/<string:year>', methods=['GET'])
def get_q_rules_config(year):
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        rules = _get_q_rules(year, cur)
        cur.close()
        return jsonify(rules), 200
    except Exception as e:
        print(f"Error getting Q rules: {e}")
        return jsonify({"error": "Error interno"}), 500
    finally:
        if conn: conn.close()

# --- Endpoint GET /api/usuarios (MODIFICADO para búsqueda paginada y exclusiones) ---
@app.route('/api/usuarios', methods=['GET'])
@jwt_required() # <-- AÑADIR JWT REQUERIDO para saber quién busca y para exclusiones
def obtener_usuarios():
    conn = None
    try:
        id_usuario_actual_str = get_jwt_identity()
        id_usuario_actual_int = int(id_usuario_actual_str)

        # Parámetros de paginación y búsqueda
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 10, type=int)
        search_query = request.args.get('search', '', type=str)
        # Parámetro para excluir miembros de una porra específica (opcional)
        exclude_porra_id_str = request.args.get('exclude_porra_id', None)

        if page < 1: page = 1
        if page_size < 1: page_size = 10
        if page_size > 50: page_size = 50 # Limitar tamaño de página
        offset = (page - 1) * page_size

        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        base_sql = " FROM usuario u WHERE u.id_usuario != %s " # Excluir al usuario actual
        params = [id_usuario_actual_int]

        # Búsqueda por nombre (case-insensitive)
        if search_query:
            base_sql += " AND u.nombre ILIKE %s " # ILIKE para case-insensitive
            params.append(f"%{search_query}%")

        # Excluir miembros de una porra específica
        if exclude_porra_id_str:
            try:
                exclude_porra_id_int = int(exclude_porra_id_str)
                base_sql += """
                    AND u.id_usuario NOT IN (
                        SELECT pa.id_usuario FROM participacion pa
                        WHERE pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA', 'PENDIENTE')
                    )
                """
                params.append(exclude_porra_id_int)
            except ValueError:
                print(f"WARN: Invalid exclude_porra_id value: {exclude_porra_id_str}")
                # No hacer nada si el ID de porra no es válido, o devolver error 400


        # --- Contar total de items para paginación ---
        count_sql = "SELECT COUNT(u.id_usuario) " + base_sql
        cur.execute(count_sql, tuple(params))
        total_items = cur.fetchone()[0]

        # --- Obtener la página de usuarios ---
        # Seleccionamos solo id y nombre, no email ni hash de contraseña
        # Ordenar por nombre para consistencia
        main_query_sql = "SELECT u.id_usuario, u.nombre " + base_sql + " ORDER BY u.nombre ASC LIMIT %s OFFSET %s;"
        params.extend([page_size, offset])
        cur.execute(main_query_sql, tuple(params))
        usuarios_db = cur.fetchall()
        cur.close()

        lista_usuarios = [dict(usuario) for usuario in usuarios_db]

        return jsonify({
            "total_items": total_items,
            "page": page,
            "page_size": page_size,
            "items": lista_usuarios
        })

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_usuarios (paginado): {error}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "No se pudieron obtener los usuarios"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- INICIO: Función obtener_carreras MODIFICADA (mi_api.txt) ---
# REEMPLAZA ESTA FUNCIÓN COMPLETA
# ... (importaciones existentes y configuración de la app Flask) ...
# Asegúrate de que flask_jwt_extended y otras dependencias están importadas.

# --- NUEVO Endpoint GET /api/auth/me ---
# Devuelve información del usuario autenticado si el token es válido
@app.route('/api/auth/me', methods=['GET'])
@jwt_required()
def get_current_user_profile():
    current_user_id_str = get_jwt_identity() # Obtiene el 'sub' (ID de usuario) del token JWT
    conn = None
    try:
        current_user_id_int = int(current_user_id_str)
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT id_usuario, nombre, email, es_admin FROM usuario WHERE id_usuario = %s;", (current_user_id_int,))
        user_data = cur.fetchone()
        cur.close()

        if user_data:
            return jsonify({
                "id_usuario": user_data["id_usuario"],
                "nombre": user_data["nombre"],
                "email": user_data["email"],
                "es_admin": user_data["es_admin"]
            }), 200
        else:
            # Esto no debería ocurrir si el token es válido y el usuario existe,
            # pero es una salvaguarda.
            return jsonify({"error": "Usuario no encontrado con el token proporcionado"}), 404

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en get_current_user_profile: {error}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Error interno al obtener el perfil del usuario"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- Endpoints para clasificaciones F1 (pilotos y constructores) (NUEVO) ---

@app.route('/api/f1/standings/drivers', methods=['GET'])
def obtener_clasificacion_pilotos_f1():
    """
    Devuelve la clasificación actual (o de una temporada concreta) de pilotos F1.
    La información se obtiene de Jolpica/Ergast Y SE AUMENTA con datos locales (nombres, colores)
    desde la tabla 'piloto_temporada'.
    """
    season_param = request.args.get('season', 'current').strip() or 'current'
    force_refresh_param = (request.args.get('force_refresh', 'false') or '').strip().lower()
    force_refresh = force_refresh_param in ('1', 'true', 't', 'yes', 'y', 'si', 'sí')

    cache = _get_driver_standings_cache(season_param)
    
    # Si tenemos caché y no se pide refresco forzado y está fresca, devolvemos directamente
    if cache and not force_refresh and _is_cache_fresh(cache.get("last_updated")):
        last_updated = cache.get("last_updated")
        if isinstance(last_updated, datetime):
            last_updated_str = last_updated.astimezone(timezone.utc).isoformat()
        else:
            last_updated_str = str(last_updated) if last_updated is not None else None

        return jsonify({
            "season": cache.get("season"),
            "last_updated": last_updated_str,
            "stale": False,
            "items": cache.get("items", []), # Devuelve items ya aumentados/cacheados
        }), 200

    # Si no hay caché o está vieja o se ha pedido force_refresh, intentamos ir a Jolpica
    conn_db = None
    try:
        # 1. Obtener datos base de Jolpica
        path = "current/driverStandings.json" if season_param == "current" else f"{season_param}/driverStandings.json"
        raw_json = _jolpica_get(path)
        items = _parse_driver_standings(raw_json) # Lista de dicts de Jolpica
        
        # 2. Obtener la temporada real (ej: "2025" en lugar de "current")
        season_year_from_ergast = raw_json.get("MRData", {}).get("StandingsTable", {}).get("season")
        if not season_year_from_ergast:
             # Si no podemos determinar el año, devolvemos los items tal cual
             print(f"WARN [drivers_standings]: No se pudo determinar el 'season' desde Ergast. Devolviendo datos sin aumentar.")
             return jsonify({"season": season_param, "last_updated": datetime.now(timezone.utc).isoformat(), "stale": True, "items": items}), 200

        # 3. Conectar a la BD local para obtener datos locales (colores, nombres)
        conn_db = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur_db = conn_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql_local_data = """
            SELECT codigo_piloto, nombre_completo, escuderia, color_fondo_hex
            FROM piloto_temporada
            WHERE ano = %s;
        """
        cur_db.execute(sql_local_data, (season_year_from_ergast,))
        local_pilotos_db = cur_db.fetchall()
        cur_db.close()
        
        # Crear un mapa para búsqueda rápida
        local_data_map = {p['codigo_piloto']: p for p in local_pilotos_db}

        # 4. Aumentar los 'items' de Jolpica con los datos locales
        augmented_items = []
        for item in items:
            driver_code = item.get('driverCode')
            local_info = local_data_map.get(driver_code)
            
            if local_info:
                # Sobrescribir nombre y escudería con los de la BD local
                item['driverName'] = local_info.get('nombre_completo') or item['driverName']
                item['constructorName'] = local_info.get('escuderia') or item['constructorName']
                # Añadir el color
                item['color_fondo_hex'] = local_info.get('color_fondo_hex') or '#CCCCCC'
            else:
                # Si no está en la BD local, asignar color por defecto
                item['color_fondo_hex'] = '#CCCCCC'
                print(f"WARN [drivers_standings]: Piloto {driver_code} de Jolpica no encontrado en piloto_temporada para año {season_year_from_ergast}.")

            augmented_items.append(item)

        # 5. Guardar en caché (BD)
        _save_driver_standings_cache(season_param, augmented_items) # Guardar items aumentados
        now_str = datetime.now(timezone.utc).isoformat()
        
        return jsonify({
            "season": season_param,
            "last_updated": now_str,
            "stale": False,
            "items": augmented_items, # Devolver items aumentados
        }), 200

    except JolpicaError as e:
        print(f"ERROR en obtener_clasificacion_pilotos_f1 (Jolpica): {e}")
        # Si Jolpica falla pero tenemos caché, devolvemos caché marcada como obsoleta
        if cache:
            last_updated = cache.get("last_updated")
            if isinstance(last_updated, datetime):
                last_updated_str = last_updated.astimezone(timezone.utc).isoformat()
            else:
                last_updated_str = str(last_updated) if last_updated is not None else None

            return jsonify({
                "season": cache.get("season"),
                "last_updated": last_updated_str,
                "stale": True,
                "items": cache.get("items", []), # Devolver caché (ya debería estar aumentada)
            }), 200

        return jsonify({
            "error": "jolpica_unavailable",
            "message": "No se pudo obtener la clasificación de pilotos desde el proveedor externo."
        }), 503

    except (Exception, psycopg2.DatabaseError) as e:
        print(f"ERROR inesperado en obtener_clasificacion_pilotos_f1 (DB local o general): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "internal_error"}), 500
    finally:
        if conn_db is not None:
            conn_db.close()


@app.route('/api/f1/standings/constructors', methods=['GET'])
def obtener_clasificacion_constructores_f1():
    """
    Devuelve la clasificación actual (o de una temporada concreta) de constructores F1.
    La información se obtiene de Jolpica/Ergast Y SE AUMENTA con datos locales (nombres, colores)
    desde la tabla 'constructor_temporada'.
    """
    season_param = request.args.get('season', 'current').strip() or 'current'
    force_refresh_param = (request.args.get('force_refresh', 'false') or '').strip().lower()
    force_refresh = force_refresh_param in ('1', 'true', 't', 'yes', 'y', 'si', 'sí')

    cache = _get_constructor_standings_cache(season_param)
    
    # Si tenemos caché y no se pide refresco forzado y está fresca, devolvemos directamente
    if cache and not force_refresh and _is_cache_fresh(cache.get("last_updated")):
        last_updated = cache.get("last_updated")
        if isinstance(last_updated, datetime):
            last_updated_str = last_updated.astimezone(timezone.utc).isoformat()
        else:
            last_updated_str = str(last_updated) if last_updated is not None else None

        return jsonify({
            "season": cache.get("season"),
            "last_updated": last_updated_str,
            "stale": False,
            "items": cache.get("items", []), # Devuelve items ya aumentados/cacheados
        }), 200

    # Si no hay caché o está vieja o se ha pedido force_refresh, intentamos ir a Jolpica
    conn_db = None
    try:
        # 1. Obtener datos base de Jolpica
        path = "current/constructorStandings.json" if season_param == "current" else f"{season_param}/constructorStandings.json"
        raw_json = _jolpica_get(path)
        items = _parse_constructor_standings(raw_json) # Lista de dicts de Jolpica
        
        # 2. Obtener la temporada real (ej: "2025" en lugar de "current")
        season_year_from_ergast = raw_json.get("MRData", {}).get("StandingsTable", {}).get("season")
        if not season_year_from_ergast:
             print(f"WARN [constructors_standings]: No se pudo determinar el 'season' desde Ergast. Devolviendo datos sin aumentar.")
             return jsonify({"season": season_param, "last_updated": datetime.now(timezone.utc).isoformat(), "stale": True, "items": items}), 200

        # 3. Conectar a la BD local para obtener datos locales (colores, nombres)
        conn_db = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur_db = conn_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql_local_data = """
            SELECT constructor_ref, nombre_constructor, color_fondo_hex
            FROM constructor_temporada
            WHERE ano = %s;
        """
        cur_db.execute(sql_local_data, (season_year_from_ergast,))
        local_constructors_db = cur_db.fetchall()
        cur_db.close()
        
        # Crear un mapa para búsqueda rápida usando 'constructor_ref'
        local_data_map = {c['constructor_ref']: c for c in local_constructors_db}
        
        # 4. Aumentar los 'items' de Jolpica con los datos locales
        augmented_items = []
        for item in items:
            constructor_ref_id = item.get('constructorId') # 'constructorId' de Jolpica (ej: "red_bull")
            local_info = local_data_map.get(constructor_ref_id)
            
            if local_info:
                # Sobrescribir nombre con el de la BD local
                item['constructorName'] = local_info.get('nombre_constructor') or item['constructorName']
                # Añadir el color
                item['color_fondo_hex'] = local_info.get('color_fondo_hex') or '#CCCCCC'
            else:
                # Si no está en la BD local, asignar color por defecto
                item['color_fondo_hex'] = '#CCCCCC'
                print(f"WARN [constructors_standings]: Constructor {constructor_ref_id} de Jolpica no encontrado en constructor_temporada para año {season_year_from_ergast}.")

            augmented_items.append(item)

        # 5. Guardar en caché (BD)
        _save_constructor_standings_cache(season_param, augmented_items) # Guardar items aumentados
        now_str = datetime.now(timezone.utc).isoformat()
        
        return jsonify({
            "season": season_param,
            "last_updated": now_str,
            "stale": False,
            "items": augmented_items, # Devolver items aumentados
        }), 200

    except JolpicaError as e:
        print(f"ERROR en obtener_clasificacion_constructores_f1 (Jolpica): {e}")
        # Si Jolpica falla pero tenemos caché, devolvemos caché marcada como obsoleta
        if cache:
            last_updated = cache.get("last_updated")
            if isinstance(last_updated, datetime):
                last_updated_str = last_updated.astimezone(timezone.utc).isoformat()
            else:
                last_updated_str = str(last_updated) if last_updated is not None else None

            return jsonify({
                "season": cache.get("season"),
                "last_updated": last_updated_str,
                "stale": True,
                "items": cache.get("items", []), # Devolver caché (ya debería estar aumentada)
            }), 200

        return jsonify({
            "error": "jolpica_unavailable",
            "message": "No se pudo obtener la clasificación de constructores desde el proveedor externo."
        }), 503

    except (Exception, psycopg2.DatabaseError) as e:
        print(f"ERROR inesperado en obtener_clasificacion_constructores_f1 (DB local o general): {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "internal_error"}), 500
    finally:
        if conn_db is not None:
            conn_db.close()

# --- Endpoint GET /api/carreras (MODIFICADO para incluir resultado_detallado) ---
@app.route('/api/carreras', methods=['GET'])
def obtener_carreras():
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        # Usar DictCursor para acceder a columnas por nombre fácilmente
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Obtener columnas clave de carrera, INCLUYENDO resultado_detallado
        # Ordenadas por año y luego por ID
        sql_query = """
            SELECT
                id_carrera,
                ano,
                desc_carrera,
                fecha_limite_apuesta, -- Esto es Clasificación (Sábado)
                fecha_limite_carrera, -- NUEVO: Esto es Carrera (Domingo)
                resultado_detallado,
                posiciones,
                vrapida
            FROM carrera
            ORDER BY ano DESC, id_carrera ASC;
        """
        cur.execute(sql_query)

        carreras_db = cur.fetchall()
        cur.close()

        # Convertir resultados a lista de diccionarios estándar, formateando fechas
        lista_carreras = []
        for carrera_row in carreras_db:
            carrera_dict = dict(carrera_row)
            # Formatear fecha si existe
            if 'fecha_limite_apuesta' in carrera_dict and isinstance(carrera_dict['fecha_limite_apuesta'], datetime):
                 carrera_dict['fecha_limite_apuesta'] = carrera_dict['fecha_limite_apuesta'].isoformat()
            if 'fecha_limite_carrera' in carrera_dict and isinstance(carrera_dict['fecha_limite_carrera'], datetime):
                 carrera_dict['fecha_limite_carrera'] = carrera_dict['fecha_limite_carrera'].isoformat()

            # El campo resultado_detallado (JSONB) debería ser manejado correctamente por DictCursor
            # como un diccionario Python si no es NULL. No se necesita conversión extra aquí.

            lista_carreras.append(carrera_dict)


        # Devolver como JSON
        return jsonify(lista_carreras)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_carreras: {error}")
        if conn: conn.close() # Asegurar cierre en error
        return jsonify({"error": "No se pudieron obtener las carreras"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()
# --- FIN: Función obtener_carreras MODIFICADA ---

# --- Endpoint POST /api/usuarios (MODIFICADO para Verificación de Email) ---
@app.route('/api/usuarios', methods=['POST'])
def registrar_usuario():
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    nombre = data.get('nombre')
    email = data.get('email')
    password = data.get('password')

    # Validación básica
    if not all([nombre, email, password]):
        return jsonify({"error": "Faltan campos requeridos (nombre, email, password)"}), 400
    if not isinstance(nombre, str) or not isinstance(email, str) or not isinstance(password, str) or \
       len(nombre.strip()) == 0 or len(email.strip()) == 0 or len(password) < 6: # Mínimo 6 caracteres para pass
         return jsonify({"error": "Nombre, email o password inválidos o vacíos (mínimo 6 caracteres para password)"}), 400

    nombre = nombre.strip()
    email = email.strip().lower()

    # --- Hashear la contraseña ---
    password_hash = generate_password_hash(password)

    # --- Generar Token y Expiración para Verificación ---
    token_verificacion = secrets.token_urlsafe(32)
    # Expiración, por ejemplo, en 1 día (puedes ajustarlo)
    expiracion_token = datetime.now(timezone.utc) + timedelta(days=1)
    print(f"DEBUG [registrar_usuario]: Token generado: {token_verificacion}")

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- Insertar Usuario con Estado No Verificado y Token ---
        sql_insert = """
            INSERT INTO usuario
                (nombre, email, password_hash, email_verificado, token_verificacion, token_verificacion_expira)
            VALUES (%s, %s, %s, FALSE, %s, %s)
            RETURNING id_usuario;
        """
        cur.execute(sql_insert, (nombre, email, password_hash, token_verificacion, expiracion_token))
        new_user_id = cur.fetchone()['id_usuario']

        # --- Preparar y Enviar Email de Verificación ---
        # **** CAMBIO AQUÍ: Usar apiBaseUrl en lugar de FRONTEND_URL ****
        # Necesitamos obtener la URL base de la API. Si está detrás de Ngrok, etc.,
        # puede ser complicado obtenerla automáticamente. Usaremos una variable de entorno
        # o una configuración fija si es necesario. Por simplicidad, intentaremos
        # obtenerla de la configuración de Flask si está disponible, o usaremos una variable de entorno.
        # Si no, tendrás que ajustarla manualmente.

        # Opción 1: Intentar desde la config de Flask (puede no estar definida así)
        # api_base_url = app.config.get('SERVER_NAME') or app.config.get('API_BASE_URL') # Ajusta según tu config

        # Opción 2: Usar una variable de entorno específica para la URL pública de la API
        api_public_url_base = os.environ.get('API_PUBLIC_URL', f'http://{DB_HOST}:5000') # Ajusta el puerto si es diferente (Flask suele ser 5000)
                                                                                        # Si usas Ngrok, pon tu URL de Ngrok aquí o en la variable de entorno

        # Opción 3: Hardcodearla (menos flexible)
        # api_public_url_base = 'https://TU_URL_NGROK_O_PRODUCCION.com'

        # --- CAMBIO DEEP LINK ---
        # Usamos el esquema personalizado en lugar de una URL HTTP
        verification_link = f"https://f1-porra-app-links.web.app/verify-email?token={token_verificacion}" # <-- USA TU DOMINIO
        # --- FIN CAMBIO DEEP LINK ---

        print(f"DEBUG [registrar_usuario]: Enlace generado para email (Deep Link): {verification_link}") # Debugger


        try:
            msg = Message(subject="Verifica tu email / Verify your email / Vérifiez votre e-mail / Verifique o seu e-mail / Verifica el teu correu - F1 Porra App",
                          recipients=[email])
            msg.body = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ES • Verificación de correo
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
¡Bienvenido/a {nombre}!

Gracias por registrarte en F1 Porra App.

Por favor, haz clic en el siguiente enlace para verificar tu dirección de correo electrónico (el enlace caduca en 24 horas):
{verification_link}

Si no te registraste, por favor ignora este email.

Saludos,
El equipo de F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EN • Email Verification
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Welcome {nombre}!

Thank you for registering in F1 Porra App.

Please click the link below to verify your email address (the link expires in 24 hours):
{verification_link}

If you didn’t create this account, please ignore this email.

Regards,
The F1 Porra App Team


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FR • Vérification de l’e-mail
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bienvenue {nombre} !

Merci de vous être inscrit(e) à F1 Porra App.

Veuillez cliquer sur le lien ci-dessous pour vérifier votre adresse e-mail (le lien expire dans 24 heures) :
{verification_link}

Si vous n’êtes pas à l’origine de cette inscription, ignorez cet e-mail.

Cordialement,
L’équipe F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PT • Verificação de e-mail
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bem-vindo/a {nombre}!

Obrigado por te registares na F1 Porra App.

Clica no link abaixo para verificares o teu e-mail (o link expira em 24 horas):
{verification_link}

Se não fizeste este registo, ignora este e-mail.

Cumprimentos,
A equipa da F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CA • Verificació del correu
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Benvingut/da {nombre}!

Gràcies per registrar-te a F1 Porra App.

Si us plau, fes clic a l’enllaç següent per verificar la teva adreça de correu electrònic (l’enllaç caduca en 24 hores):
{verification_link}

Si no t’has registrat tu, ignora aquest correu.

Salutacions,
L’equip de F1 Porra App
"""

            print(f"DEBUG: Intentando enviar email de verificación a {email}...") # Debug
            mail.send(msg)
            print(f"DEBUG: Email de verificación enviado (aparentemente) a {email}.") # Debug

        except Exception as e_mail:
             print(f"ERROR al enviar email de verificación a {email}: {e_mail}")
             import traceback
             print(f"ERROR DETALLADO al enviar email de verificación a {email}:")
             traceback.print_exc()
             print(f"ERROR (resumen) al enviar email: {e_mail}")
             # Opción 1: Deshacer el registro (rollback) y devolver error. Es más seguro.
             conn.rollback() # Deshacer el INSERT del usuario
             cur.close()
             conn.close()
             return jsonify({"error": "No se pudo enviar el email de verificación. Inténtalo de nuevo más tarde."}), 500

        # --- Commit y Respuesta Final ---
        conn.commit()
        cur.close()

        # No devolvemos el ID o email aquí, solo un mensaje genérico
        return jsonify({"mensaje": "Registro casi completo. Revisa tu email para verificar tu cuenta."}), 201

    except psycopg2.errors.UniqueViolation as e:
        conn.rollback()
        error_detail = str(e).lower()
        if 'usuario_nombre_key' in error_detail:
             return jsonify({"error": f"El nombre de usuario '{nombre}' ya existe"}), 409
        elif 'usuario_email_key' in error_detail:
             return jsonify({"error": f"El email '{email}' ya está registrado"}), 409
        else:
             return jsonify({"error": "Conflicto de datos únicos al registrar"}), 409

    except (Exception, psycopg2.DatabaseError) as error:
        import traceback
        print(f"ERROR DETALLADO en registrar_usuario:")
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al registrar el usuario"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()

# --- Endpoint POST /api/porras/<id_porra>/apuestas (MODIFICADO v7 - Lógica Estado Apuesta Mejorada) ---
# --- Endpoint POST /api/porras/<id_porra>/apuestas (MODIFICADO v8 - Fix error cast text[] to jsonb) ---
@app.route('/api/porras/<int:id_porra>/apuestas', methods=['POST'])
@jwt_required()
def registrar_o_actualizar_apuesta(id_porra):
    id_usuario_actual = get_jwt_identity() # ID String

    # --- Validaciones básicas input ---
    if not request.is_json: return jsonify({"error": "La solicitud debe ser JSON"}), 400
    data = request.get_json()
    id_carrera = data.get('id_carrera')
    
    # Datos opcionales (pueden venir unos, otros o ambos)
    posiciones_carrera = data.get('posiciones') # Array carrera
    posiciones_clasificacion = data.get('posiciones_clasificacion') # Array clasificación
    vrapida = data.get('vrapida') # String

    if not id_carrera or not isinstance(id_carrera, int):
        return jsonify({"error": "Falta id_carrera o es inválido"}), 400

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- User ID Conversion ---
        try: id_usuario_actual_int = int(id_usuario_actual)
        except (ValueError, TypeError): return jsonify({"error": "Error interno autorización."}), 500

        # --- 1. Info Porra ---
        sql_porra = "SELECT tipo_porra FROM porra WHERE id_porra = %s;"
        cur.execute(sql_porra, (id_porra,))
        porra_info = cur.fetchone()
        if porra_info is None: return jsonify({"error": "Porra no encontrada"}), 404
        tipo_porra_actual = porra_info['tipo_porra']

        # --- 2. Info Carrera y Fechas Límite ---
        cur.execute("SELECT ano, fecha_limite_apuesta, fecha_limite_carrera FROM carrera WHERE id_carrera = %s;", (id_carrera,))
        carrera_info = cur.fetchone()
        if carrera_info is None: return jsonify({"error": "Carrera no encontrada"}), 404
        
        fecha_limite_clasif_db = carrera_info['fecha_limite_apuesta']
        fecha_limite_carrera_db = carrera_info['fecha_limite_carrera']
        
        # Gestión de zonas horarias
        try: from zoneinfo import ZoneInfo
        except ImportError: from pytz import timezone as ZoneInfo
        now_utc = datetime.now(timezone.utc)

        # Validar fechas si existen en DB
        is_clasif_open = True
        is_race_open = True

        if fecha_limite_clasif_db:
            if fecha_limite_clasif_db.tzinfo is None: fecha_limite_clasif_db = fecha_limite_clasif_db.replace(tzinfo=timezone.utc)
            if now_utc > fecha_limite_clasif_db: is_clasif_open = False
        
        if fecha_limite_carrera_db:
            if fecha_limite_carrera_db.tzinfo is None: fecha_limite_carrera_db = fecha_limite_carrera_db.replace(tzinfo=timezone.utc)
            if now_utc > fecha_limite_carrera_db: is_race_open = False
        else:
            if not is_clasif_open: is_race_open = False 

        # --- 3. Membresía ---
        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual_int))
        if cur.fetchone() is None: return jsonify({"error": "No eres miembro activo."}), 403

        # --- 4. Preparar Upsert ---
        cur.execute("SELECT posiciones, posiciones_clasificacion, vrapida, estado_apuesta FROM apuesta WHERE id_porra=%s AND id_carrera=%s AND id_usuario=%s", 
                    (id_porra, id_carrera, id_usuario_actual_int))
        apuesta_previa = cur.fetchone()

        # Valores finales a guardar (default a lo que ya había o null)
        final_pos_carrera = apuesta_previa['posiciones'] if apuesta_previa else None
        final_pos_clasif = apuesta_previa['posiciones_clasificacion'] if apuesta_previa else None
        final_vr = apuesta_previa['vrapida'] if apuesta_previa else None

        # --- CORRECCIÓN CRÍTICA: Convertir a JSON String si vienen de DB como lista/dict ---
        # Psycopg2 devuelve jsonb como listas de Python. Si las pasamos a la query con ::jsonb,
        # intenta hacer cast de Array a Jsonb y falla. Debemos pasarlas como STRING JSON.
        if isinstance(final_pos_carrera, (list, dict)):
            final_pos_carrera = json.dumps(final_pos_carrera)
        
        if isinstance(final_pos_clasif, (list, dict)):
            final_pos_clasif = json.dumps(final_pos_clasif)
        # ----------------------------------------------------------------------------------
        
        # Validar y asignar CLASIFICACIÓN (Sobrescribe si hay input nuevo)
        if posiciones_clasificacion is not None:
            if not is_clasif_open:
                return jsonify({"error": "La clasificación ya ha cerrado."}), 409
            if not isinstance(posiciones_clasificacion, list):
                return jsonify({"error": "Formato inválido para clasificación"}), 400
            final_pos_clasif = json.dumps(posiciones_clasificacion)

        # Validar y asignar CARRERA (Sobrescribe si hay input nuevo)
        if posiciones_carrera is not None:
            if not is_race_open:
                return jsonify({"error": "La carrera ya ha cerrado."}), 409
            if not isinstance(posiciones_carrera, list):
                return jsonify({"error": "Formato inválido para carrera"}), 400
            final_pos_carrera = json.dumps(posiciones_carrera)
            
        # VR va ligado a carrera
        if vrapida is not None:
            if not is_race_open:
                 return jsonify({"error": "La carrera ya ha cerrado (VR)."}), 409
            final_vr = vrapida

        # Lógica Estado Apuesta
        estado_final = 'PENDIENTE'
        fecha_estado = None
        
        if tipo_porra_actual in ['PUBLICA', 'PRIVADA_AMISTOSA']:
            estado_final = 'ACEPTADA'
            fecha_estado = now_utc
        elif tipo_porra_actual == 'PRIVADA_ADMINISTRADA':
            if apuesta_previa and apuesta_previa['estado_apuesta'] == 'ACEPTADA':
                estado_final = 'ACEPTADA' 
                fecha_estado = now_utc
            else:
                estado_final = 'PENDIENTE'

        # Lógica Trofeo Primera Apuesta
        should_award_first_bet_trophy = False
        if not apuesta_previa:
            cur.execute("SELECT COUNT(*) FROM apuesta WHERE id_usuario = %s;", (id_usuario_actual_int,))
            if cur.fetchone()[0] == 0: should_award_first_bet_trophy = True

        # --- EJECUTAR UPSERT ---
        sql_upsert = """
            INSERT INTO apuesta (id_porra, id_carrera, id_usuario, posiciones, posiciones_clasificacion, vrapida, estado_apuesta, fecha_estado_apuesta, fecha_modificacion)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
            ON CONFLICT (id_porra, id_carrera, id_usuario) DO UPDATE SET
                posiciones = EXCLUDED.posiciones,
                posiciones_clasificacion = EXCLUDED.posiciones_clasificacion,
                vrapida = EXCLUDED.vrapida,
                estado_apuesta = EXCLUDED.estado_apuesta,
                fecha_modificacion = EXCLUDED.fecha_modificacion;
        """
        valores = (id_porra, id_carrera, id_usuario_actual_int, final_pos_carrera, final_pos_clasif, final_vr, estado_final, fecha_estado, now_utc)
        cur.execute(sql_upsert, valores)

        if should_award_first_bet_trophy:
             _award_trophy(id_usuario_actual_int, 'PRIMERA_APUESTA', conn, cur)

        conn.commit()
        return jsonify({"mensaje": "Apuesta guardada correctamente."}), 201

    except Exception as error:
        print(f"ERROR General [Registrar Apuesta]: {error}")
        import traceback; traceback.print_exc() # Imprimir detalle para debug
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al registrar apuesta"}), 500
    finally:
        if cur is not None: cur.close()
        if conn is not None: conn.close()
# --- FIN Endpoint POST Apuestas MODIFICADO ---

# --- NUEVO Endpoint POST /api/login ---
# --- Endpoint POST /api/login (MODIFICADO para requerir Email Verificado y añadir claim de admin Y NOMBRE) ---
@app.route('/api/login', methods=['POST'])
def login_usuario():
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')
    if not email or not password:
        return jsonify({"error": "Faltan campos requeridos (email, password)"}), 400
    email = email.strip().lower()
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Buscamos al usuario y OBTENEMOS email_verificado, es_admin, nombre y language_code
        sql = "SELECT id_usuario, nombre, email, password_hash, email_verificado, es_admin, language_code FROM usuario WHERE email = %s;"
        cur.execute(sql, (email,))
        user = cur.fetchone()
        cur.close()

        if user and check_password_hash(user['password_hash'], password):
            if not user['email_verificado']:
                return jsonify({"error": "Email no verificado. Por favor, revisa tu bandeja de entrada y haz clic en el enlace de verificación."}), 403

            # Email verificado y contraseña correcta: Proceder a crear token
            admin_status = user['es_admin']
            user_name = user['nombre']
            language_code = user['language_code'] or 'es' # Obtener idioma, con 'es' como fallback
            additional_claims = {
                "is_admin": admin_status,
                "nombre_usuario": user_name,
                "language_code": language_code # Añadir idioma al token
            }
            access_token = create_access_token(
                identity=str(user['id_usuario']),
                additional_claims=additional_claims
            )
            return jsonify(access_token=access_token), 200
        else:
            return jsonify({"error": "Credenciales inválidas"}), 401

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en login_usuario: {error}")
        return jsonify({"error": "Error interno durante el login"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()

# --- Endpoint POST /api/porras (MODIFICADO para tipo_porra y admin check) ---
@app.route('/api/porras', methods=['POST'])
@jwt_required()
def crear_porra():
    id_creador = get_jwt_identity() # ID como string del token
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    nombre_porra = data.get('nombre_porra')
    ano = data.get('ano')
    # --- NUEVO: Obtener tipo_porra ---
    tipo_porra = data.get('tipo_porra') # Ej: 'PRIVADA_AMISTOSA', 'PRIVADA_ADMINISTRADA', 'PUBLICA'

    # Validación (incluye tipo_porra)
    allowed_types = ['PRIVADA_AMISTOSA', 'PRIVADA_ADMINISTRADA', 'PUBLICA']
    if not all([nombre_porra, ano, tipo_porra]) or \
       not isinstance(nombre_porra, str) or not isinstance(ano, str) or not isinstance(tipo_porra, str) or \
       len(nombre_porra.strip()) == 0 or len(ano.strip()) == 0 or \
       tipo_porra not in allowed_types: # Validar tipo
         return jsonify({"error": "Faltan campos (nombre_porra, ano, tipo_porra) o son inválidos. Tipos permitidos: PRIVADA_AMISTOSA, PRIVADA_ADMINISTRADA, PUBLICA"}), 400

    nombre_porra = nombre_porra.strip()
    ano = ano.strip()
    tipo_porra = tipo_porra.strip().upper() # Guardar en mayúsculas por consistencia

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- NUEVO: Comprobación Admin para Porras Públicas ---
        is_admin = False
        try:
            # Obtener claims adicionales del token
            current_user_claims = get_jwt() # Necesitas importar get_jwt from flask_jwt_extended
            is_admin = current_user_claims.get("is_admin", False)
            print(f"DEBUG [crear_porra]: User {id_creador} is_admin claim: {is_admin}")
        except Exception as e_claims:
            print(f"WARN [crear_porra]: No se pudieron obtener claims del token: {e_claims}")

        if tipo_porra == 'PUBLICA' and not is_admin:
            cur.close()
            conn.close()
            return jsonify({"error": "Solo los administradores pueden crear porras públicas"}), 403 # Forbidden
        # --- FIN Comprobación Admin ---

        # Convertir id_creador a int para la BD
        try:
            id_creador_int = int(id_creador)
        except (ValueError, TypeError):
             print(f"ERROR: ID de creador inválido en token: {id_creador}")
             cur.close(); conn.close()
             return jsonify({"error": "Error interno de autorización"}), 500

        # --- INICIO NUEVA MEDIDA DE SEGURIDAD ---
        # Contar cuántas porras tiene este usuario como CREADOR
        sql_count = "SELECT COUNT(*) FROM porra WHERE id_creador = %s;"
        cur.execute(sql_count, (id_creador_int,))
        count_result = cur.fetchone()
        num_porras_creadas = count_result[0] if count_result else 0

        if num_porras_creadas >= 5:
            # Si ya tiene 5 o más, rechazamos la creación
            cur.close(); conn.close()
            # Devolvemos un código de error específico "LIMIT_REACHED" para que el Flutter lo detecte
            return jsonify({"error": "LIMIT_REACHED"}), 409 # 409 Conflict
        # --- FIN NUEVA MEDIDA DE SEGURIDAD ---

        # 1. Insertar la nueva porra (incluyendo tipo_porra)
        # Asegúrate que tu tabla 'porra' tiene la columna 'tipo_porra'
        sql_insert_porra = """
            INSERT INTO porra (nombre_porra, ano, id_creador, tipo_porra)
            VALUES (%s, %s, %s, %s) RETURNING id_porra, fecha_creacion;
            """
        cur.execute(sql_insert_porra, (nombre_porra, ano, id_creador_int, tipo_porra)) # Usar id_creador_int
        nueva_porra = cur.fetchone()
        id_nueva_porra = nueva_porra['id_porra']

        # 2. Añadir automáticamente al creador como participante (sin cambios aquí)
        sql_insert_participacion = "INSERT INTO participacion (id_porra, id_usuario, estado) VALUES (%s, %s, %s);"
        cur.execute(sql_insert_participacion, (id_nueva_porra, id_creador_int, 'CREADOR')) # Usar id_creador_int

        conn.commit()
        cur.close()

        # Devolver tipo_porra en la respuesta
        return jsonify({
            "id_porra": id_nueva_porra,
            "nombre_porra": nombre_porra,
            "ano": ano,
            "id_creador": id_creador_int, # Devolver como int
            "fecha_creacion": nueva_porra['fecha_creacion'].isoformat() if nueva_porra['fecha_creacion'] else None,
            "tipo_porra": tipo_porra, # <<< NUEVO
            "mensaje": "Porra creada con éxito."
        }), 201

    except psycopg2.Error as db_error:
        print(f"Error DB en crear_porra: {db_error}")
        if conn: conn.rollback()
        # Podría ser un UniqueViolation si nombre+año ya existe?
        return jsonify({"error": "Error de base de datos al crear la porra"}), 500
    except Exception as error:
        print(f"Error inesperado en crear_porra: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al crear la porra"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()

# --- Endpoint GET /api/porras/<id_porra>/carreras/<id_carrera>/apuesta (MODIFICADO v2 con estado_apuesta) ---
@app.route('/api/porras/<int:id_porra>/carreras/<int:id_carrera>/apuesta', methods=['GET'])
@jwt_required()
def obtener_mi_apuesta(id_porra, id_carrera):
    id_usuario_actual = get_jwt_identity() 
    try: id_usuario_actual_int = int(id_usuario_actual)
    except: return jsonify({"error": "Token inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual_int))
        if cur.fetchone() is None: return jsonify({"error": "No eres miembro activo."}), 403

        # Seleccionamos también posiciones_clasificacion
        sql_get_bet = """
            SELECT id_apuesta, id_porra, id_carrera, id_usuario, posiciones, posiciones_clasificacion, vrapida, estado_apuesta
            FROM apuesta
            WHERE id_porra = %s AND id_carrera = %s AND id_usuario = %s;
        """
        cur.execute(sql_get_bet, (id_porra, id_carrera, id_usuario_actual_int))
        apuesta = cur.fetchone()
        cur.close()

        if apuesta:
            # Parsear JSONB posiciones (Carrera)
            pos_data = apuesta['posiciones']
            posiciones_list = []
            if isinstance(pos_data, str): posiciones_list = json.loads(pos_data)
            elif isinstance(pos_data, list): posiciones_list = pos_data
            
            # Parsear JSONB posiciones_clasificacion (NUEVO)
            pos_class_data = apuesta['posiciones_clasificacion']
            posiciones_class_list = []
            if isinstance(pos_class_data, str): posiciones_class_list = json.loads(pos_class_data)
            elif isinstance(pos_class_data, list): posiciones_class_list = pos_class_data

            resultado_json = {
                "id_apuesta": apuesta["id_apuesta"],
                "id_porra": apuesta["id_porra"],
                "id_carrera": apuesta["id_carrera"],
                "id_usuario": apuesta["id_usuario"],
                "posiciones": posiciones_list,
                "posiciones_clasificacion": posiciones_class_list, # NUEVO
                "vrapida": apuesta["vrapida"],
                "estado_apuesta": apuesta["estado_apuesta"]
            }
            return jsonify(resultado_json), 200
        else:
            return jsonify({"error": "No se encontró apuesta"}), 404

    except Exception as error:
        print(f"Error en obtener_mi_apuesta: {error}")
        return jsonify({"error": "Error interno"}), 500
    finally:
        if conn: conn.close()

# --- Endpoint GET /api/porras/.../apuestas/todas (MODIFICADO v2 con estado_apuesta) ---
# --- Endpoint GET /api/porras/.../apuestas/todas (MODIFICADO v3 con posiciones_clasificacion) ---
@app.route('/api/porras/<int:id_porra>/carreras/<int:id_carrera>/apuestas/todas', methods=['GET'])
@jwt_required()
def obtener_todas_apuestas_carrera(id_porra, id_carrera):
    id_usuario_actual = get_jwt_identity() # String
    try:
        id_usuario_actual_int = int(id_usuario_actual)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- Validaciones (Carrera, Membresía, Fecha Límite - sin cambios) ---
        cur.execute("SELECT fecha_limite_apuesta FROM carrera WHERE id_carrera = %s;", (id_carrera,))
        carrera_info = cur.fetchone() 
        if carrera_info is None: 
            return jsonify({"error": "Carrera no encontrada"}), 404
        fecha_limite_db = carrera_info['fecha_limite_apuesta'] 
        if fecha_limite_db is None: 
            return jsonify({"error": "Fecha límite no definida."}), 409
        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual_int)) 
        if cur.fetchone() is None: 
            return jsonify({"error": "No eres miembro activo."}), 403
        try: tz_madrid = ZoneInfo("Europe/Madrid")
        except Exception: tz_madrid = timezone.utc
        now_local = datetime.now(tz_madrid) 
        if fecha_limite_db.tzinfo is None: 
            fecha_limite_db = fecha_limite_db.replace(tzinfo=timezone.utc)
        
        # Validamos contra la fecha de clasificación (la primera fecha de corte)
        if now_local.astimezone(timezone.utc) <= fecha_limite_db.astimezone(timezone.utc): 
            return jsonify({ "error": "No se pueden ver apuestas hasta después de la fecha límite.", "fecha_limite": fecha_limite_db.isoformat(), }), 403
        # --- Fin Validaciones ---

        # --- Obtener TODAS las apuestas (Añadir estado_apuesta y posiciones_clasificacion) ---
        sql_get_all_bets = """
            SELECT
                u.id_usuario,
                u.nombre AS nombre_usuario,
                a.id_apuesta,
                a.posiciones,
                a.posiciones_clasificacion, -- <<< AÑADIDO
                a.vrapida,
                a.estado_apuesta
            FROM apuesta a
            JOIN usuario u ON a.id_usuario = u.id_usuario
            WHERE a.id_porra = %s AND a.id_carrera = %s
            ORDER BY u.nombre ASC;
            """
        cur.execute(sql_get_all_bets, (id_porra, id_carrera))
        todas_apuestas = cur.fetchall()
        cur.close()

        # --- Formatear la respuesta ---
        lista_apuestas_formateada = []
        for apuesta in todas_apuestas:
            try:
                # Parsear JSONB Carrera
                pos_data = apuesta['posiciones']
                posiciones_list = [] 
                if isinstance(pos_data, str): posiciones_list = json.loads(pos_data)
                elif isinstance(pos_data, list): posiciones_list = pos_data
                elif isinstance(pos_data, dict) and all(isinstance(k, int) for k in pos_data.keys()): posiciones_list = [pos_data[k] for k in sorted(pos_data.keys())]
                
                # Parsear JSONB Clasificación
                pos_class_data = apuesta['posiciones_clasificacion']
                posiciones_class_list = []
                if isinstance(pos_class_data, str): posiciones_class_list = json.loads(pos_class_data)
                elif isinstance(pos_class_data, list): posiciones_class_list = pos_class_data

                apuesta_formateada = {
                    "id_apuesta": apuesta["id_apuesta"],
                    "id_usuario": apuesta["id_usuario"],
                    "nombre_usuario": apuesta["nombre_usuario"],
                    "posiciones": posiciones_list,
                    "posiciones_clasificacion": posiciones_class_list, # <<< AÑADIDO
                    "vrapida": apuesta["vrapida"],
                    "estado_apuesta": apuesta["estado_apuesta"]
                }
                lista_apuestas_formateada.append(apuesta_formateada)
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                 print(f"Error formateando apuesta TODAS (ID Apuesta: {apuesta.get('id_apuesta')}): {e}")
                 continue

        return jsonify(lista_apuestas_formateada), 200

    except psycopg2.DatabaseError as db_error:
        print(f"Error DB en obtener_todas_apuestas_carrera: {db_error}")
        return jsonify({"error": "Error de base de datos al obtener apuestas"}), 500
    except Exception as error:
        print(f"Error general en obtener_todas_apuestas_carrera: {error}")
        import traceback; traceback.print_exc()
        return jsonify({"error": "Error interno al obtener todas las apuestas"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()

# --- Endpoint GET /api/carreras/<id_carrera>/resultado (MODIFICADO v3 para incluir clasificación) ---
@app.route('/api/carreras/<int:id_carrera>/resultado', methods=['GET'])
def obtener_resultado_carrera(id_carrera):
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Obtener resultado detallado JSONB y AÑO de la carrera
        # SELECCIONAMOS TAMBIÉN resultado_detallado_clasificacion
        sql_race = "SELECT ano, resultado_detallado, resultado_detallado_clasificacion FROM carrera WHERE id_carrera = %s;"
        cur.execute(sql_race, (id_carrera,))
        resultado_db = cur.fetchone()

        if not resultado_db:
            cur.close(); conn.close()
            return jsonify({"error": "Carrera no encontrada"}), 404

        ano_carrera = resultado_db.get("ano")
        resultado_detallado_json = resultado_db.get("resultado_detallado")
        resultado_clasif_json = resultado_db.get("resultado_detallado_clasificacion")

        # --- Obtener Detalles de Pilotos (Lógica existente) ---
        pilotos_map = {} 
        sql_pilotos_carrera = """
            SELECT codigo_piloto, nombre_completo_carrera, escuderia_carrera, color_fondo_hex_carrera
            FROM piloto_carrera_detalle
            WHERE id_carrera = %s;
        """
        cur.execute(sql_pilotos_carrera, (id_carrera,))
        pilotos_info_carrera_db = cur.fetchall()

        if pilotos_info_carrera_db:
            for p in pilotos_info_carrera_db:
                pilotos_map[p['codigo_piloto']] = {
                    'nombre_completo': p.get('nombre_completo_carrera', p['codigo_piloto']),
                    'escuderia': p.get('escuderia_carrera', ''),
                    'color_escuderia_hex': p.get('color_fondo_hex_carrera', '#CCCCCC')
                }
        else:
            sql_pilotos_temporada = """
                SELECT codigo_piloto, nombre_completo, escuderia, color_fondo_hex
                FROM piloto_temporada
                WHERE ano = %s;
            """
            cur.execute(sql_pilotos_temporada, (ano_carrera,))
            pilotos_info_temporada_db = cur.fetchall()
            for p in pilotos_info_temporada_db:
                 pilotos_map[p['codigo_piloto']] = {
                    'nombre_completo': p.get('nombre_completo', p['codigo_piloto']),
                    'escuderia': p.get('escuderia', ''),
                    'color_escuderia_hex': p.get('color_fondo_hex', '#CCCCCC')
                 }

        # --- Construir Respuesta ---
        respuesta_final = {
            "id_carrera": id_carrera,
            "status": "pendiente",
            "carrera": None,
            "clasificacion": None
        }

        has_race_result = False
        has_qualy_result = False

        # Procesar Carrera
        if resultado_detallado_json and isinstance(resultado_detallado_json, dict):
            try:
                posiciones_detalle_db = resultado_detallado_json.get('posiciones_detalle')
                vrapida_piloto_db = resultado_detallado_json.get('vrapida_piloto')
                vrapida_tiempo_db = resultado_detallado_json.get('vrapida_tiempo')

                if posiciones_detalle_db and vrapida_piloto_db:
                    resultado_final_posiciones = []
                    for piloto_res_db in posiciones_detalle_db:
                        if not isinstance(piloto_res_db, dict): continue
                        codigo = piloto_res_db.get('codigo')
                        if not codigo: continue
                        piloto_detalle_final = pilotos_map.get(codigo, {'nombre_completo': codigo, 'escuderia': '?', 'color_escuderia_hex': '#CCCCCC'})
                        resultado_final_posiciones.append({
                            "posicion": piloto_res_db.get('posicion'),
                            "codigo": codigo,
                            "tiempo_str": piloto_res_db.get('tiempo_str'),
                            "nombre_completo": piloto_detalle_final['nombre_completo'],
                            "escuderia": piloto_detalle_final['escuderia'],
                            "color_escuderia_hex": piloto_detalle_final['color_escuderia_hex']
                        })
                    
                    vrapida_piloto_detalle = pilotos_map.get(vrapida_piloto_db, {'nombre_completo': vrapida_piloto_db, 'escuderia': '?', 'color_escuderia_hex': '#CCCCCC'})
                    
                    respuesta_final["carrera"] = {
                        "posiciones_detalle": resultado_final_posiciones,
                        "vrapida_detalle": {
                            "codigo": vrapida_piloto_db,
                            "tiempo_vr": vrapida_tiempo_db,
                            "nombre_completo": vrapida_piloto_detalle['nombre_completo'],
                            "color_escuderia_hex": vrapida_piloto_detalle['color_escuderia_hex']
                        }
                    }
                    has_race_result = True
            except Exception as e:
                print(f"Error procesando resultado carrera: {e}")

        # Procesar Clasificación
        if resultado_clasif_json and isinstance(resultado_clasif_json, list):
            try:
                # resultado_clasif_json es una LISTA de objetos con {posicion, codigo, q1, q2, q3}
                resultado_final_clasif = []
                for item in resultado_clasif_json:
                    codigo = item.get('codigo')
                    piloto_detalle = pilotos_map.get(codigo, {'nombre_completo': codigo, 'escuderia': '?', 'color_escuderia_hex': '#CCCCCC'})
                    
                    # Determinar el mejor tiempo a mostrar (Q3 > Q2 > Q1)
                    tiempo_display = item.get('q3') or item.get('q2') or item.get('q1') or '-'
                    
                    resultado_final_clasif.append({
                        "posicion": item.get('posicion'),
                        "codigo": codigo,
                        "tiempo_str": tiempo_display, # Tiempo de su última sesión
                        "q1": item.get('q1'),
                        "q2": item.get('q2'),
                        "q3": item.get('q3'),
                        "nombre_completo": piloto_detalle['nombre_completo'],
                        "escuderia": piloto_detalle['escuderia'],
                        "color_escuderia_hex": piloto_detalle['color_escuderia_hex']
                    })
                
                respuesta_final["clasificacion"] = {
                    "posiciones_detalle": resultado_final_clasif
                }
                has_qualy_result = True
            except Exception as e:
                print(f"Error procesando resultado clasificacion: {e}")

        if has_race_result or has_qualy_result:
            respuesta_final["status"] = "finalizada" # Al menos uno está disponible
        
        # Compatibilidad hacia atrás (para que la app no rompa antes de actualizarse)
        if has_race_result:
            respuesta_final["posiciones_detalle"] = respuesta_final["carrera"]["posiciones_detalle"]
            respuesta_final["vrapida_detalle"] = respuesta_final["carrera"]["vrapida_detalle"]

        cur.close(); conn.close()
        return jsonify(respuesta_final), 200

    except psycopg2.DatabaseError as db_error:
        print(f"Error DB en obtener_resultado_carrera: {db_error}")
        if conn: conn.close()
        return jsonify({"error": "Error de base de datos"}), 500
    except Exception as error:
        print(f"Error general en obtener_resultado_carrera: {error}")
        if conn: conn.close()
        return jsonify({"error": "Error interno"}), 500

# --- Endpoint PUT /api/carreras/<id_carrera>/resultado (MODIFICADO v9 - Condición 5+ miembros para trofeos GP) ---
# --- Endpoint PUT /api/carreras/<id_carrera>/resultado (MODIFICADO v11 - Añadido trofeo Madrid) ---
@app.route('/api/carreras/<int:id_carrera>/resultado', methods=['PUT'])
@jwt_required()
def actualizar_resultado_carrera(id_carrera):
    id_usuario_admin = get_jwt_identity()
    if not request.is_json: return jsonify({"error": "La solicitud debe ser JSON"}), 400
    data = request.get_json()
    resultado_detallado_input = data

    # Validación del JSON de entrada 'resultado_detallado_input'
    try:
        posiciones_detalle_input = resultado_detallado_input.get('posiciones_detalle')
        vrapida_piloto_input = resultado_detallado_input.get('vrapida_piloto')
        vrapida_tiempo_input = resultado_detallado_input.get('vrapida_tiempo')
        if not isinstance(posiciones_detalle_input, list) or not posiciones_detalle_input or \
           not isinstance(vrapida_piloto_input, str) or not vrapida_piloto_input or \
           not isinstance(vrapida_tiempo_input, str): 
            raise ValueError("Faltan campos clave (posiciones_detalle, vrapida_piloto, vrapida_tiempo) o tipo incorrecto.")

        posiciones_resultado_codigos = []
        for i, piloto_res in enumerate(posiciones_detalle_input):
            if not isinstance(piloto_res, dict) or 'codigo' not in piloto_res or 'tiempo_str' not in piloto_res:
                raise ValueError(f"Formato inválido en 'posiciones_detalle' en índice {i}.")
            posiciones_resultado_codigos.append(piloto_res['codigo'])
        if not posiciones_resultado_codigos:
             raise ValueError("'posiciones_detalle' no contiene códigos válidos.")
    except (ValueError, KeyError, TypeError) as e:
        return jsonify({"error": f"Datos de resultado inválidos: {e}"}), 400

    conn = None 
    total_puntuaciones_calculadas = 0
    
    # --- ACTUALIZACIÓN AQUÍ: Añadido 'Madrid': 'GANA_MADRID' al mapa ---
    map_carrera_trofeo = { 
        'Australia': 'GANA_AUSTRALIA', 'China': 'GANA_CHINA', 'Japan': 'GANA_JAPON', 
        'Bahrein': 'GANA_BAREIN', 'Saudi Arabia': 'GANA_ARABIA_SAUDI', 'Miami': 'GANA_MIAMI', 
        'Emilia-Romagna': 'GANA_EMILIA_ROMANA', 'Monaco': 'GANA_MONACO', 'Spain': 'GANA_ESPANA', 
        'Canada': 'GANA_CANADA', 'Austria': 'GANA_AUSTRIA', 'Great Bretain': 'GANA_GRAN_BRETANA', 
        'Belgium': 'GANA_BELGICA', 'Hungary': 'GANA_HUNGRIA', 'Netherlands': 'GANA_PAISES_BAJOS', 
        'Italy': 'GANA_ITALIA', 'Azerbaijan': 'GANA_AZERBAYAN', 'Singapore': 'GANA_SINGAPUR', 
        'United States': 'GANA_ESTADOS_UNIDOS', 'Mexico': 'GANA_MEXICO', 'Brazil': 'GANA_BRASIL', 
        'Las Vegas': 'GANA_LAS_VEGAS', 'Qatar': 'GANA_CATAR', 'Abu Dhabi': 'GANA_ABU_DABI',
        'Barcelona': 'GANA_ESPANA', # Mapeamos Barcelona al trofeo de España existente
        'Madrid': 'GANA_MADRID'     # Nuevo mapeo para Madrid
    }
    
    map_piloto_trofeo = { 'VER': 'ACIERTA_VER', 'LEC': 'ACIERTA_LEC', 'ALO': 'ACIERTA_ALO', 'SAI': 'ACIERTA_SAI', 'HAM': 'ACIERTA_HAM', 'RUS': 'ACIERTA_RUS', 'NOR': 'ACIERTA_NOR' }

    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Verificación Admin
        cur.execute("SELECT es_admin FROM usuario WHERE id_usuario = %s;", (id_usuario_admin,))
        user_info = cur.fetchone()
        if not user_info or not user_info['es_admin']:
             cur.close(); conn.close(); return jsonify({"error": "No autorizado (se requiere admin)"}), 403

        # Obtener año y desc carrera
        cur.execute("SELECT ano, desc_carrera FROM carrera WHERE id_carrera = %s;", (id_carrera,))
        carrera_info_row = cur.fetchone()
        if not carrera_info_row:
             cur.close(); conn.close(); return jsonify({"error": f"Carrera con id {id_carrera} no encontrada"}), 404
        ano_carrera = carrera_info_row['ano']
        desc_carrera = carrera_info_row['desc_carrera'] 

        # Validación contra Pilotos Activos
        active_drivers_for_race = set(); expected_driver_count_for_race = 0
        sql_active_race_drivers = "SELECT codigo_piloto FROM piloto_carrera_detalle WHERE id_carrera = %s AND activo_para_apuesta = TRUE;"
        cur.execute(sql_active_race_drivers, (id_carrera,))
        pilotos_activos_db = cur.fetchall()
        if pilotos_activos_db:
            active_drivers_for_race = {p['codigo_piloto'] for p in pilotos_activos_db}; expected_driver_count_for_race = len(active_drivers_for_race)
        else: 
            sql_season_drivers = "SELECT codigo_piloto FROM piloto_temporada WHERE ano = %s;"
            cur.execute(sql_season_drivers, (ano_carrera,))
            pilotos_temporada_db = cur.fetchall()
            if not pilotos_temporada_db: cur.close(); conn.close(); return jsonify({"error": f"Config error: No hay pilotos definidos para temporada {ano_carrera}."}), 409
            active_drivers_for_race = {p['codigo_piloto'] for p in pilotos_temporada_db}; expected_driver_count_for_race = len(active_drivers_for_race)

        # Validar resultado recibido
        if len(posiciones_resultado_codigos) != expected_driver_count_for_race: cur.close(); conn.close(); return jsonify({"error": f"El resultado enviado tiene {len(posiciones_resultado_codigos)} pilotos, pero se esperaban {expected_driver_count_for_race} (activos para apuesta)."}), 400
        if not all(p_code in active_drivers_for_race for p_code in posiciones_resultado_codigos): invalid_codes = [p for p in posiciones_resultado_codigos if p not in active_drivers_for_race]; cur.close(); conn.close(); return jsonify({"error": f"El resultado enviado incluye pilotos inválidos o inactivos: {invalid_codes}"}), 400
        if vrapida_piloto_input not in active_drivers_for_race: cur.close(); conn.close(); return jsonify({"error": f"El piloto de VR '{vrapida_piloto_input}' es inválido o inactivo."}), 400
        print(f"DEBUG [PUT Result Validation]: Input validado contra pilotos activos ({expected_driver_count_for_race}) OK.")

        # --- 1. Actualizar tabla 'carrera' ---
        sql_update_carrera = """ UPDATE carrera SET resultado_detallado = %s::jsonb, posiciones = %s::jsonb, vrapida = %s WHERE id_carrera = %s; """
        resultado_detallado_json_db = json.dumps(resultado_detallado_input)
        posiciones_codigos_json_db = json.dumps(posiciones_resultado_codigos)
        valores_update_carrera = (resultado_detallado_json_db, posiciones_codigos_json_db, vrapida_piloto_input, id_carrera)
        cur.execute(sql_update_carrera, valores_update_carrera)
        print(f"DEBUG: Resultado carrera {id_carrera} actualizado en tabla carrera.")

        # --- 2. Poblar piloto_carrera_detalle ---
        sql_pilotos_temp_details = "SELECT codigo_piloto, nombre_completo, escuderia, color_fondo_hex, color_texto_hex FROM piloto_temporada WHERE ano = %s;"
        cur.execute(sql_pilotos_temp_details, (ano_carrera,))
        pilotos_temporada_db_details = cur.fetchall()
        piloto_details_map_season = { p['codigo_piloto']: p for p in pilotos_temporada_db_details }

        sql_upsert_piloto_detalle = """
            INSERT INTO piloto_carrera_detalle (
                id_carrera, codigo_piloto, nombre_completo_carrera, escuderia_carrera,
                color_fondo_hex_carrera, color_texto_hex_carrera, activo_para_apuesta
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_carrera, codigo_piloto) DO UPDATE SET
                nombre_completo_carrera = EXCLUDED.nombre_completo_carrera,
                escuderia_carrera = EXCLUDED.escuderia_carrera,
                color_fondo_hex_carrera = EXCLUDED.color_fondo_hex_carrera,
                color_texto_hex_carrera = EXCLUDED.color_texto_hex_carrera,
                activo_para_apuesta = EXCLUDED.activo_para_apuesta;
        """
        valores_pilotos_detalle = []
        pilotos_procesados = set() 

        for piloto_res in posiciones_detalle_input:
            codigo = piloto_res.get('codigo')
            if codigo and codigo in piloto_details_map_season:
                details = piloto_details_map_season[codigo]
                valores_pilotos_detalle.append((
                    id_carrera, codigo,
                    details.get('nombre_completo', codigo), details.get('escuderia', ''),
                    details.get('color_fondo_hex', '#CCCCCC'), details.get('color_texto_hex', '#000000'),
                    False 
                ))
                pilotos_procesados.add(codigo)

        if vrapida_piloto_input not in pilotos_procesados and vrapida_piloto_input in piloto_details_map_season:
             details = piloto_details_map_season[vrapida_piloto_input]
             valores_pilotos_detalle.append((
                 id_carrera, vrapida_piloto_input,
                 details.get('nombre_completo', vrapida_piloto_input), details.get('escuderia', ''),
                 details.get('color_fondo_hex', '#CCCCCC'), details.get('color_texto_hex', '#000000'),
                 False
             ))

        if valores_pilotos_detalle:
            cur.executemany(sql_upsert_piloto_detalle, valores_pilotos_detalle)

        # --- 3. Buscar la SIGUIENTE carrera ---
        next_race_id = None
        next_race_name = None
        sql_find_next_race = """
            SELECT id_carrera, desc_carrera FROM carrera
            WHERE ano = %s AND id_carrera > %s ORDER BY id_carrera ASC LIMIT 1;
            """
        cur.execute(sql_find_next_race, (ano_carrera, id_carrera))
        next_race_row = cur.fetchone()
        if next_race_row:
            next_race_id = next_race_row['id_carrera']
            next_race_name = next_race_row['desc_carrera']

        # --- 4. Bucle Principal Cálculo Puntuaciones ---
        resultado_para_calculo = {"posiciones": posiciones_resultado_codigos, "vrapida": vrapida_piloto_input}
        sql_get_porras = """
            SELECT p.id_porra, p.tipo_porra, COUNT(pa.id_usuario) as member_count
            FROM porra p
            LEFT JOIN participacion pa ON p.id_porra = pa.id_porra AND pa.estado IN ('CREADOR', 'ACEPTADA')
            WHERE p.ano = %s
            GROUP BY p.id_porra, p.tipo_porra;
            """
        cur.execute(sql_get_porras, (ano_carrera,))
        porras_del_ano = cur.fetchall()

        if not porras_del_ano:
            conn.commit(); cur.close(); conn.close()
            return jsonify({ "mensaje": f"Resultado guardado. No hay porras activas.", "puntuaciones_calculadas": 0 }), 200

        posiciones_resultado_map = {piloto: index for index, piloto in enumerate(posiciones_resultado_codigos)}
        users_notified_about_result = set()
        users_notified_about_next_race = set()

        for porra_row in porras_del_ano:
            id_porra_actual = porra_row['id_porra']
            tipo_porra_actual = porra_row['tipo_porra']
            member_count_porra = porra_row['member_count']
            print(f"DEBUG: Procesando porra {id_porra_actual}...")

            sql_select_apuestas = """
                SELECT id_usuario, posiciones, vrapida
                FROM apuesta
                WHERE id_porra = %s AND id_carrera = %s AND estado_apuesta = 'ACEPTADA';
            """
            cur.execute(sql_select_apuestas, (id_porra_actual, id_carrera))
            lista_apuestas_db_raw = cur.fetchall()
            lista_apuestas_para_calculo = []
            map_apuestas_usuario = {}

            for apuesta_raw in lista_apuestas_db_raw:
                 try:
                     pos_data = apuesta_raw['posiciones']
                     apuesta_pos_list = []
                     if isinstance(pos_data, str): apuesta_pos_list = json.loads(pos_data)
                     elif isinstance(pos_data, list): apuesta_pos_list = pos_data
                     
                     if len(apuesta_pos_list) == expected_driver_count_for_race:
                         apuesta_dict = { 'id_usuario': apuesta_raw['id_usuario'], 'posiciones': apuesta_pos_list, 'vrapida': apuesta_raw['vrapida'] }
                         lista_apuestas_para_calculo.append(apuesta_dict)
                         map_apuestas_usuario[apuesta_raw['id_usuario']] = apuesta_dict
                 except Exception: pass

            puntuaciones_calculadas = []
            if lista_apuestas_para_calculo:
                puntuaciones_calculadas = calcular_puntuaciones_api(resultado_para_calculo, lista_apuestas_para_calculo)

            # --- CAMBIO IMPORTANTE: NO BORRAMOS, HACEMOS UPSERT ---
            # sql_delete_puntuaciones = "DELETE FROM puntuacion WHERE id_porra = %s AND id_carrera = %s;"
            # cur.execute(sql_delete_puntuaciones, (id_porra_actual, id_carrera))

            participants_to_notify_result = set()
            participants_to_notify_next_race = set()

            if puntuaciones_calculadas:
                 puntuaciones_calculadas.sort(key=lambda x: x.get('puntos', 0), reverse=True)
                 valores_upsert_puntuaciones = []
                 current_rank = 0; last_score = -1; rank_counter = 0
                 map_rank_usuario = {}

                 for p in puntuaciones_calculadas:
                     rank_counter += 1
                     user_id = p['id_usuario']
                     if p['puntos'] != last_score: current_rank = rank_counter; last_score = p['puntos']
                     
                     # Preparamos valores para UPSERT
                     valores_upsert_puntuaciones.append((id_porra_actual, id_carrera, user_id, p['puntos'], ano_carrera))
                     
                     map_rank_usuario[user_id] = current_rank
                     if user_id not in users_notified_about_result: participants_to_notify_result.add(user_id)
                     if next_race_id is not None and user_id not in users_notified_about_next_race: participants_to_notify_next_race.add(user_id)

                 # UPSERT Query: Si existe, actualiza 'puntos' pero NO 'puntos_clasificacion'
                 # Si no existe, crea con puntos_clasificacion = 0 por defecto.
                 sql_upsert_puntuacion = """
                    INSERT INTO puntuacion (id_porra, id_carrera, id_usuario, puntos, ano, puntos_clasificacion)
                    VALUES (%s, %s, %s, %s, %s, 0)
                    ON CONFLICT (id_porra, id_carrera, id_usuario)
                    DO UPDATE SET puntos = EXCLUDED.puntos;
                 """
                 cur.executemany(sql_upsert_puntuacion, valores_upsert_puntuaciones)
                 num_insertadas = cur.rowcount; total_puntuaciones_calculadas += len(valores_upsert_puntuaciones)

                 detalles_trofeo = {"ano": ano_carrera, "id_porra": id_porra_actual, "id_carrera": id_carrera}
                 trofeo_carrera_especifico = map_carrera_trofeo.get(desc_carrera)

                 for user_id, rank in map_rank_usuario.items():
                     if rank == 1: 
                         if member_count_porra >= 5:
                             _award_trophy(user_id, 'GANA_CARRERA_CUALQUIERA', conn, cur, detalles=detalles_trofeo)
                         if trofeo_carrera_especifico and member_count_porra >= 5:
                             _award_trophy(user_id, trofeo_carrera_especifico, conn, cur, detalles=detalles_trofeo)
                         if tipo_porra_actual == 'PUBLICA':
                             _award_trophy(user_id, 'GANA_CARRERA_PUBLICA', conn, cur, detalles=detalles_trofeo)

                     if member_count_porra >= 5:
                         apuesta_usuario = map_apuestas_usuario.get(user_id)
                         if apuesta_usuario:
                             apuesta_pos_list = apuesta_usuario.get('posiciones', [])
                             for piloto_code, trofeo_code in map_piloto_trofeo.items():
                                 pos_real_idx = posiciones_resultado_map.get(piloto_code)
                                 if pos_real_idx is not None and \
                                    pos_real_idx < len(apuesta_pos_list) and \
                                    apuesta_pos_list[pos_real_idx] == piloto_code:
                                      detalles_piloto = {**detalles_trofeo, "piloto": piloto_code}
                                      _award_trophy(user_id, trofeo_code, conn, cur, detalles=detalles_piloto)
            else:
                 # Si no hay puntuaciones, igual notificamos
                 cur.execute("SELECT id_usuario FROM participacion WHERE id_porra = %s AND estado IN ('CREADOR', 'ACEPTADA');", (id_porra_actual,))
                 all_participants = cur.fetchall()
                 for participant in all_participants:
                     user_id = participant['id_usuario']
                     if user_id not in users_notified_about_result: participants_to_notify_result.add(user_id)
                     if next_race_id is not None and user_id not in users_notified_about_next_race: participants_to_notify_next_race.add(user_id)

            # --- Notificaciones (igual que antes) ---
            if participants_to_notify_result:
                 user_ids_list = list(participants_to_notify_result)
                 if user_ids_list:
                     placeholders = ','.join(['%s'] * len(user_ids_list))
                     sql_tokens = f"SELECT id_usuario, fcm_token, language_code FROM usuario WHERE id_usuario IN ({placeholders}) AND fcm_token IS NOT NULL AND fcm_token != '';"
                     cur.execute(sql_tokens, tuple(user_ids_list))
                     tokens = cur.fetchall()
                     if tokens and thread_pool_executor:
                        for t in tokens:
                            try:
                                thread_pool_executor.submit(send_fcm_result_notification_task, t['id_usuario'], t['fcm_token'], desc_carrera, id_porra_actual, (t.get('language_code') or 'es').strip().lower())
                                users_notified_about_result.add(t['id_usuario'])
                            except: pass

            if next_race_id is not None and participants_to_notify_next_race:
                user_ids_list = list(participants_to_notify_next_race)
                if user_ids_list:
                    placeholders = ','.join(['%s'] * len(user_ids_list))
                    sql_tokens = f"SELECT id_usuario, fcm_token, language_code FROM usuario WHERE id_usuario IN ({placeholders}) AND fcm_token IS NOT NULL AND fcm_token != '';"
                    cur.execute(sql_tokens, tuple(user_ids_list))
                    tokens = cur.fetchall()
                    if tokens and thread_pool_executor:
                        for t in tokens:
                            try:
                                thread_pool_executor.submit( send_fcm_next_race_notification_task, t['id_usuario'], t['fcm_token'], desc_carrera, next_race_name, id_porra_actual, next_race_id, (t.get('language_code') or 'es').strip().lower() )
                                users_notified_about_next_race.add(t['id_usuario'])
                            except: pass
        # --- Fin bucle porras ---

        # --- 5. Comprobar fin temporada ---
        cur.execute("SELECT COUNT(*) FROM carrera WHERE ano = %s;", (ano_carrera,))
        total_races_year = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM carrera WHERE ano = %s AND resultado_detallado IS NOT NULL;", (ano_carrera,))
        finished_races_year = cur.fetchone()[0]
        
        if total_races_year > 0 and finished_races_year >= total_races_year:
             for porra_row in porras_del_ano:
                 id_porra_actual = porra_row['id_porra']
                 tipo_porra_actual = porra_row['tipo_porra']
                 member_count_porra = porra_row['member_count']
                 detalles_temporada = {"ano": ano_carrera, "id_porra": id_porra_actual}

                 sql_clasificacion = "SELECT id_usuario, SUM(puntos) as total_puntos FROM puntuacion WHERE id_porra = %s AND ano = %s GROUP BY id_usuario ORDER BY total_puntos DESC;"
                 cur.execute(sql_clasificacion, (id_porra_actual, ano_carrera))
                 clasificacion_final = cur.fetchall()
                 if clasificacion_final:
                    winner_score = clasificacion_final[0]['total_puntos']
                    processed_winners = set()
                    for clasif_row in clasificacion_final:
                         if clasif_row['total_puntos'] == winner_score:
                            winner_user_id = clasif_row['id_usuario']
                            if winner_user_id not in processed_winners:
                                if member_count_porra >= 5:
                                    _award_trophy(winner_user_id, 'CAMPEON_TEMPORADA', conn, cur, detalles=detalles_temporada)
                                if tipo_porra_actual == 'PUBLICA':
                                    _award_trophy(winner_user_id, 'CAMPEON_TEMPORADA_PUBLICA', conn, cur, detalles=detalles_temporada)
                                processed_winners.add(winner_user_id)
                         else: break 

                 cur.execute("SELECT id_usuario FROM participacion WHERE id_porra = %s AND estado IN ('CREADOR', 'ACEPTADA');", (id_porra_actual,))
                 participantes = cur.fetchall()
                 for participante in participantes:
                     user_id_part = participante['id_usuario']
                     sql_count_bets = "SELECT COUNT(DISTINCT a.id_carrera) FROM apuesta a JOIN carrera c ON a.id_carrera = c.id_carrera WHERE a.id_usuario = %s AND a.id_porra = %s AND c.ano = %s AND a.estado_apuesta = 'ACEPTADA';"
                     cur.execute(sql_count_bets, (user_id_part, id_porra_actual, ano_carrera))
                     if cur.fetchone()[0] >= total_races_year:
                         _award_trophy(user_id_part, 'APLICADO', conn, cur, detalles=detalles_temporada)

        conn.commit()
        cur.close()
        return jsonify({"mensaje": f"Resultado detallado guardado. Puntuaciones upserted.", "puntuaciones_actualizadas": total_puntuaciones_calculadas}), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR DETALLADO en actualizar_resultado_carrera: {error}")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al actualizar resultado"}), 500
    finally:
        if conn is not None and not conn.closed: conn.close()


# --- NUEVO Endpoint para OBTENER las puntuaciones de una carrera específica ---
# --- Endpoint GET /api/porras/<id_porra>/carreras/<id_carrera>/puntuaciones (MODIFICADO v2 con desglose) ---
@app.route('/api/porras/<int:id_porra>/carreras/<int:id_carrera>/puntuaciones', methods=['GET'])
@jwt_required()
def obtener_puntuaciones_porra_carrera(id_porra, id_carrera):
    id_usuario_actual_str = get_jwt_identity()
    try: id_usuario_actual = int(id_usuario_actual_str)
    except: return jsonify({"error": "Token inválido"}), 400

    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 25))
    offset = (page - 1) * page_size

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        sql_check = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check, (id_porra, id_usuario_actual))
        if cur.fetchone() is None: return jsonify({"error": "No eres miembro activo"}), 403

        # CONSULTA MODIFICADA: Desglosamos puntos_clasificacion y puntos (carrera)
        sql_get_scores_page = f"""
            WITH RankedRaceScores AS (
                SELECT
                    u.id_usuario,
                    u.nombre,
                    COALESCE(p.puntos_clasificacion, 0) as pts_qualy,
                    COALESCE(p.puntos, 0) as pts_race,
                    (COALESCE(p.puntos_clasificacion, 0) + COALESCE(p.puntos, 0)) as pts_total,
                    RANK() OVER (ORDER BY (COALESCE(p.puntos_clasificacion, 0) + COALESCE(p.puntos, 0)) DESC, u.nombre ASC) as posicion
                FROM usuario u
                JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s AND p.id_carrera = %s
                WHERE pa.id_porra = %s
            )
            SELECT id_usuario, nombre, pts_qualy, pts_race, pts_total, posicion
            FROM RankedRaceScores
            ORDER BY posicion ASC
            LIMIT %s OFFSET %s;
        """
        cur.execute(sql_get_scores_page, (id_porra, id_porra, id_carrera, id_porra, page_size, offset))
        puntuaciones_pagina = cur.fetchall()

        sql_count = "SELECT COUNT(DISTINCT u.id_usuario) FROM usuario u JOIN participacion pa ON u.id_usuario = pa.id_usuario WHERE pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_count, (id_porra,))
        total_items = cur.fetchone()[0]

        # Datos usuario actual
        my_rank, my_score_total, my_score_qualy, my_score_race = None, None, None, None
        
        sql_my_rank = """
             WITH RankedRaceScores AS (
                 SELECT
                    u.id_usuario,
                    COALESCE(p.puntos_clasificacion, 0) as pts_qualy,
                    COALESCE(p.puntos, 0) as pts_race,
                    (COALESCE(p.puntos_clasificacion, 0) + COALESCE(p.puntos, 0)) as pts_total,
                    RANK() OVER (ORDER BY (COALESCE(p.puntos_clasificacion, 0) + COALESCE(p.puntos, 0)) DESC, u.nombre ASC) as posicion
                 FROM usuario u
                 JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                 LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s AND p.id_carrera = %s
                 WHERE pa.id_porra = %s
            )
            SELECT posicion, pts_total, pts_qualy, pts_race
            FROM RankedRaceScores WHERE id_usuario = %s;
        """
        cur.execute(sql_my_rank, (id_porra, id_porra, id_carrera, id_porra, id_usuario_actual))
        user_rank_data = cur.fetchone()
        if user_rank_data:
            my_rank = user_rank_data['posicion']
            my_score_total = user_rank_data['pts_total']
            my_score_qualy = user_rank_data['pts_qualy']
            my_score_race = user_rank_data['pts_race']

        cur.close()

        lista_puntuaciones_pagina = []
        for row in puntuaciones_pagina:
            lista_puntuaciones_pagina.append({
                "posicion": row["posicion"],
                "nombre": row["nombre"],
                "puntos_total": row["pts_total"],
                "puntos_qualy": row["pts_qualy"],
                "puntos_race": row["pts_race"],
                "id_usuario": row["id_usuario"]
            })

        return jsonify({
            "my_rank": my_rank,
            "my_score_total": my_score_total,
            "my_score_qualy": my_score_qualy,
            "my_score_race": my_score_race,
            "total_items": total_items,
            "page": page,
            "page_size": page_size,
            "items": lista_puntuaciones_pagina
        }), 200

    except Exception as error:
        print(f"Error obtener_puntuaciones: {error}")
        if conn: conn.close()
        return jsonify({"error": "Error interno"}), 500
    finally:
        if conn: conn.close()


# --- NUEVO Endpoint para OBTENER la clasificación general de un año ---
# --- Endpoint GET /api/porras/<id_porra>/clasificacion (MODIFICADO con Paginación y Datos Usuario) ---
# --- Endpoint GET /api/porras/<id_porra>/clasificacion (MODIFICADO con suma correcta de puntos) ---
@app.route('/api/porras/<int:id_porra>/clasificacion', methods=['GET'])
@jwt_required()
def obtener_clasificacion_porra(id_porra):
    id_usuario_actual_str = get_jwt_identity()
    try:
        id_usuario_actual = int(id_usuario_actual_str)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    # --- Paginación ---
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 25)) # Tamaño de página por defecto 25
        if page < 1: page = 1
        if page_size < 1: page_size = 25
        if page_size > 100: page_size = 100 # Limitar tamaño máximo de página
        offset = (page - 1) * page_size
    except ValueError:
        return jsonify({"error": "Parámetros 'page' y 'page_size' deben ser números enteros"}), 400
    # --- Fin Paginación ---

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Verificar Autorización (¿Usuario es miembro activo?) ---
        sql_check_membership = """
            SELECT 1 FROM participacion
            WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');
        """
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual))
        if cur.fetchone() is None:
             return jsonify({"error": "No eres miembro activo de esta porra"}), 403

        # --- 2. Obtener Clasificación PAGINADA ---
        # CORRECCIÓN: Sumamos (p.puntos + p.puntos_clasificacion) manejando nulos con COALESCE
        sql_get_standings_page = f"""
            WITH RankedScores AS (
                SELECT
                    u.id_usuario,
                    u.nombre,
                    COALESCE(SUM(COALESCE(p.puntos, 0) + COALESCE(p.puntos_clasificacion, 0)), 0) as puntos_totales,
                    RANK() OVER (
                        ORDER BY COALESCE(SUM(COALESCE(p.puntos, 0) + COALESCE(p.puntos_clasificacion, 0)), 0) DESC, 
                        u.nombre ASC
                    ) as posicion
                FROM usuario u
                LEFT JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s
                WHERE pa.id_porra = %s -- Asegura que solo contamos participantes activos
                GROUP BY u.id_usuario, u.nombre
            )
            SELECT id_usuario, nombre, puntos_totales, posicion
            FROM RankedScores
            ORDER BY posicion ASC
            LIMIT %s OFFSET %s;
        """
        cur.execute(sql_get_standings_page, (id_porra, id_porra, id_porra, page_size, offset))
        clasificacion_pagina = cur.fetchall()

        # --- 3. Obtener Total de Items ---
        sql_count_total = """
             SELECT COUNT(DISTINCT u.id_usuario)
             FROM usuario u
             JOIN participacion pa ON u.id_usuario = pa.id_usuario
             WHERE pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA');
        """
        cur.execute(sql_count_total, (id_porra,))
        total_items = cur.fetchone()[0]

        # --- 4. Obtener Datos del Usuario Actual (Rank y Puntos) ---
        # CORRECCIÓN: Misma lógica de suma para el usuario actual
        my_rank = None
        my_score = None
        sql_get_user_rank = """
            WITH RankedScores AS (
                 SELECT
                    u.id_usuario,
                    COALESCE(SUM(COALESCE(p.puntos, 0) + COALESCE(p.puntos_clasificacion, 0)), 0) as puntos_totales,
                    RANK() OVER (
                        ORDER BY COALESCE(SUM(COALESCE(p.puntos, 0) + COALESCE(p.puntos_clasificacion, 0)), 0) DESC, 
                        u.nombre ASC
                    ) as posicion
                 FROM usuario u
                 LEFT JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                 LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s
                 WHERE pa.id_porra = %s
                 GROUP BY u.id_usuario, u.nombre
            )
            SELECT posicion, puntos_totales
            FROM RankedScores
            WHERE id_usuario = %s;
        """
        cur.execute(sql_get_user_rank, (id_porra, id_porra, id_porra, id_usuario_actual))
        user_rank_data = cur.fetchone()
        if user_rank_data:
            my_rank = user_rank_data['posicion']
            my_score = user_rank_data['puntos_totales']

        cur.close()

        # --- 5. Formatear Respuesta ---
        lista_clasificacion_pagina = []
        for row in clasificacion_pagina:
            lista_clasificacion_pagina.append({
                "posicion": row["posicion"],
                "nombre": row["nombre"],
                "puntos_totales": row["puntos_totales"],
                "id_usuario": row["id_usuario"] 
            })

        return jsonify({
            "my_rank": my_rank,
            "my_score": my_score,
            "total_items": total_items,
            "page": page,
            "page_size": page_size,
            "items": lista_clasificacion_pagina 
        }), 200 

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_clasificacion_porra (paginado): {error}")
        import traceback
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al obtener la clasificación de la porra"}), 500
    finally:
        if conn is not None:
            conn.close()


# --- Endpoint GET /api/porras (MODIFICADO para incluir tipo_porra) ---
@app.route('/api/porras', methods=['GET'])
@jwt_required()
def listar_porras_usuario():
    id_usuario_actual = get_jwt_identity() # ID como string del token
    try:
        id_usuario_actual_int = int(id_usuario_actual)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- MODIFICADO: Seleccionar tipo_porra en lugar de es_publica ---
        sql = """
            SELECT
                p.id_porra,
                p.nombre_porra,
                p.ano,
                p.id_creador,
                u_creator.nombre as nombre_creador,
                p.fecha_creacion,
                p.tipo_porra -- <<< SELECCIONAR NUEVA COLUMNA
            FROM porra p
            JOIN participacion pa ON p.id_porra = pa.id_porra
            JOIN usuario u_creator ON p.id_creador = u_creator.id_usuario
            WHERE pa.id_usuario = %s AND pa.estado IN ('CREADOR', 'ACEPTADA') -- Mostrar solo si es miembro activo
            ORDER BY p.ano DESC, p.nombre_porra ASC;
        """
        cur.execute(sql, (id_usuario_actual_int,)) # Usar ID int
        porras_usuario = cur.fetchall()
        cur.close()

        lista_porras = []
        for row in porras_usuario:
            porra_dict = dict(row)
            # Formatear fecha si existe
            if 'fecha_creacion' in porra_dict and isinstance(porra_dict['fecha_creacion'], datetime):
                 porra_dict['fecha_creacion'] = porra_dict['fecha_creacion'].isoformat()
            lista_porras.append(porra_dict)

        return jsonify(lista_porras), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en listar_porras_usuario: {error}")
        return jsonify({"error": "Error interno al obtener las porras del usuario"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()

# --- NUEVO Endpoint GET /api/porras/<id_porra>/miembros (Protegido con JWT) ---
# Devuelve la lista de usuarios participantes en una porra específica.
# Solo los miembros de la porra pueden ver esta lista.
# --- Endpoint GET /api/porras/<id_porra>/miembros (MODIFICADO con Paginación y Orden Especial) ---
@app.route('/api/porras/<int:id_porra>/miembros', methods=['GET'])
@jwt_required()
def listar_miembros_porra(id_porra):
    id_usuario_actual_str = get_jwt_identity()
    try:
        id_usuario_actual = int(id_usuario_actual_str)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    # --- Paginación ---
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 30)) # Un poco más grande para miembros
        if page < 1: page = 1
        if page_size < 1: page_size = 30
        if page_size > 100: page_size = 100
    except ValueError:
        return jsonify({"error": "Parámetros 'page' y 'page_size' deben ser números enteros"}), 400
    # --- Fin Paginación ---

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Verificar Autorización (¿Usuario es miembro activo?) ---
        sql_check_membership = """
            SELECT 1 FROM participacion
            WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');
        """
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual))
        es_miembro_activo = cur.fetchone()

        if not es_miembro_activo:
            cur.close()
            conn.close()
            return jsonify({"error": "No autorizado para ver los miembros de esta porra"}), 403

        # --- 2. Obtener ID del Creador ---
        cur.execute("SELECT id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if not porra_info:
            # Esto no debería pasar si la comprobación de membresía funcionó, pero por si acaso
            cur.close()
            conn.close()
            return jsonify({"error": "Porra no encontrada"}), 404
        id_creador = porra_info['id_creador']

        # --- 3. Obtener TODOS los Miembros ACTIVOS de la BD ---
        sql_get_all_members = """
            SELECT u.id_usuario, u.nombre
            FROM usuario u
            JOIN participacion pa ON u.id_usuario = pa.id_usuario
            WHERE pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
        """
        cur.execute(sql_get_all_members, (id_porra,))
        all_members_db = cur.fetchall()
        cur.close()
        conn.close() # Cerramos conexión antes de procesar en Python

        # --- 4. Ordenar en Python ---
        all_members = [dict(m) for m in all_members_db]
        total_items = len(all_members)

        creator_member = None
        current_user_member = None
        other_members = []

        for member in all_members:
            member_id = member['id_usuario']
            if member_id == id_creador:
                creator_member = member
            # Comprobar si es el usuario actual, *incluso si también es el creador*
            if member_id == id_usuario_actual:
                 current_user_member = member
            # Añadir a otros *solo si NO es el creador Y NO es el usuario actual*
            if member_id != id_creador and member_id != id_usuario_actual:
                other_members.append(member)

        # Ordenar alfabéticamente los otros miembros
        other_members.sort(key=lambda x: x.get('nombre', '').lower())

        # Construir lista final ordenada
        final_sorted_list = []
        if creator_member:
            final_sorted_list.append(creator_member)
        # Añadir usuario actual si existe y NO es el creador (para evitar duplicados si user == creator)
        if current_user_member and current_user_member['id_usuario'] != id_creador:
             final_sorted_list.append(current_user_member)
        final_sorted_list.extend(other_members)


        # --- 5. Aplicar Paginación a la Lista Ordenada ---
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        members_page = final_sorted_list[start_index:end_index]

        # --- 6. Devolver Respuesta JSON ---
        return jsonify({
            "total_items": total_items,
            "page": page,
            "page_size": page_size,
            "items": members_page # La página de miembros ordenada
        }), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en listar_miembros_porra (paginado): {error}")
        import traceback
        traceback.print_exc()
        if conn and not conn.closed: # Asegurarse de cerrar si falló antes
             conn.close()
        return jsonify({"error": "Error interno al obtener los miembros de la porra"}), 500
    finally:
        # Asegurar que la conexión se cierra si sigue abierta
        if conn is not None and not conn.closed:
            conn.close()

# --- Fin del endpoint modificado en mi_api.py ---


# --- Endpoint POST /api/porras/<id_porra>/invitaciones (MODIFICADO para Notificación FCM) ---
@app.route('/api/porras/<int:id_porra>/invitaciones', methods=['POST'])
@jwt_required() # Requiere token
def invitar_usuario_porra(id_porra):
    id_usuario_actual_str = get_jwt_identity() # ID del creador (string)
    current_user_claims = get_jwt() # Obtener todos los claims del token
    nombre_invitador = current_user_claims.get("nombre_usuario", "El creador de la porra") # Nombre del creador

    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    nombre_invitado_req = data.get('nombre_invitado')

    if not nombre_invitado_req or not isinstance(nombre_invitado_req, str) or len(nombre_invitado_req.strip()) == 0:
        return jsonify({"error": "Falta 'nombre_invitado' o es inválido"}), 400
    
    nombre_invitado = nombre_invitado_req.strip()

    conn = None
    cur = None # Declarar cur aquí para poder cerrarlo en finally
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Verificar que la porra existe y que el usuario actual es el creador
        cur.execute("SELECT id_creador, nombre_porra FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if not porra_info:
            return jsonify({"error": f"Porra con id {id_porra} no encontrada"}), 404

        id_db_creator = porra_info['id_creador']
        nombre_porra_actual = porra_info['nombre_porra'] # Nombre de la porra
        
        try:
            id_token_int = int(id_usuario_actual_str)
        except (ValueError, TypeError):
            print(f"ERROR: Identidad de token inválida para convertir a int: {id_usuario_actual_str}")
            cur.close(); conn.close()
            return jsonify({"error": "Error interno de autorización"}), 500

        if id_db_creator != id_token_int:
             cur.close(); conn.close()
             return jsonify({"error": "Solo el creador puede enviar invitaciones para esta porra"}), 403

        # 2. Buscar al usuario invitado por NOMBRE y obtener su fcm_token
        cur.execute("SELECT id_usuario, nombre, fcm_token, language_code FROM usuario WHERE nombre = %s;", (nombre_invitado,))
        usuario_invitado_data = cur.fetchone()

        if not usuario_invitado_data:
            return jsonify({"error": f"Usuario con nombre '{nombre_invitado}' no encontrado"}), 404

        id_usuario_invitado = usuario_invitado_data['id_usuario']
        nombre_usuario_invitado_confirmado = usuario_invitado_data['nombre']
        fcm_token_invitado = usuario_invitado_data.get('fcm_token') # Puede ser None
        user_lang = (usuario_invitado_data.get('language_code') or 'es').strip().lower()

        # 3. Validaciones adicionales
        if id_usuario_invitado == id_token_int:
             return jsonify({"error": "No puedes invitarte a ti mismo"}), 400

        cur.execute("SELECT estado FROM participacion WHERE id_porra = %s AND id_usuario = %s;", (id_porra, id_usuario_invitado))
        participacion_existente = cur.fetchone()
        if participacion_existente:
            estado_actual = participacion_existente['estado']
            if estado_actual in ['CREADOR', 'ACEPTADA']:
                return jsonify({"error": f"El usuario '{nombre_usuario_invitado_confirmado}' ya es miembro de esta porra"}), 409
            elif estado_actual == 'PENDIENTE':
                 return jsonify({"error": f"El usuario '{nombre_usuario_invitado_confirmado}' ya tiene una invitación pendiente para esta porra"}), 409

        # 4. Insertar la Invitación
        sql_insert = "INSERT INTO participacion (id_porra, id_usuario, estado) VALUES (%s, %s, %s);"
        cur.execute(sql_insert, (id_porra, id_usuario_invitado, 'PENDIENTE'))

        # --- 5. Enviar Notificación FCM ---
        if fcm_token_invitado:
            print(f"DEBUG [Invitar Usuario]: Intentando enviar notificación de invitación a user {id_usuario_invitado} (Token: ...{fcm_token_invitado[-10:] if fcm_token_invitado else 'N/A'})...")
            global thread_pool_executor
            if thread_pool_executor:
                thread_pool_executor.submit(
                    send_fcm_invitation_notification_task,
                    id_usuario_invitado,
                    fcm_token_invitado,
                    id_porra,
                    nombre_porra_actual,
                    nombre_invitador, # Nombre del usuario que hace la invitación (creador)
                    user_lang  
                )
                print(f"DEBUG [Invitar Usuario]: Tarea de notificación de invitación FCM enviada al executor.")
            else:
                print("WARN [Invitar Usuario]: ThreadPoolExecutor no disponible, no se pudo enviar tarea FCM para invitación.")
        else:
            print(f"DEBUG [Invitar Usuario]: No se envía notificación de invitación (token FCM del invitado es nulo o vacío) para user {id_usuario_invitado}.")
        # --- Fin Enviar Notificación FCM ---

        conn.commit()
        cur.close()

        return jsonify({"mensaje": f"Invitación enviada correctamente al usuario '{nombre_usuario_invitado_confirmado}' para la porra '{nombre_porra_actual}'"}), 201

    except psycopg2.Error as db_error:
        print(f"Error de base de datos en invitar_usuario_porra: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al enviar la invitación"}), 500
    except Exception as error:
        print(f"Error inesperado en invitar_usuario_porra: {error}")
        import traceback
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al enviar la invitación"}), 500
    finally:
        if cur is not None and not cur.closed: cur.close()
        if conn is not None and not conn.closed: conn.close()
# --- Fin endpoint MODIFICADO ---
# --- NUEVO Endpoint POST /api/participaciones/<id_participacion>/respuesta (Protegido con JWT) ---
# Permite al usuario autenticado aceptar o rechazar una invitación PENDIENTE dirigida a él.
@app.route('/api/participaciones/<int:id_participacion>/respuesta', methods=['POST'])
@jwt_required()
def responder_invitacion(id_participacion):
    id_usuario_actual = get_jwt_identity()

    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    aceptar = data.get('aceptar') # Esperamos un booleano: true para aceptar, false para rechazar

    # Validar que 'aceptar' sea un booleano
    if aceptar is None or not isinstance(aceptar, bool):
         return jsonify({"error": "Falta el campo 'aceptar' (true/false) o es inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Validar la invitación/participación ---
        # Obtener la fila de participación para verificar usuario y estado
        sql_check = "SELECT id_usuario, estado FROM participacion WHERE id_participacion = %s;"
        cur.execute(sql_check, (id_participacion,))
        participacion_info = cur.fetchone()

        if not participacion_info:
            return jsonify({"error": "Invitación/Participación no encontrada"}), 404

        # Verificar que la invitación es para el usuario actual
        # --- Autorización ---
        id_db_creator = participacion_info['id_usuario'] # Esto es un INT (ej: 12)
        id_token_str = id_usuario_actual        # Esto es un STR (ej: '12')

        # --- *** CORRECCIÓN AQUÍ *** ---
        # Intentar convertir el ID del token (string) a entero
        try:
            id_token_int = int(id_token_str)
        except (ValueError, TypeError):
            # Si la identidad del token no es un número válido por alguna razón
            print(f"ERROR: Identidad de token inválida para convertir a int: {id_token_str}")
            cur.close()
            conn.close()
            return jsonify({"error": "Error interno de autorización"}), 500

        # Ahora comparar ENTERO con ENTERO
        if id_db_creator != id_token_int:
             print(f"DEBUG AUTH FAIL: DB Creator ID: {id_db_creator} (Type: {type(id_db_creator)})")
             print(f"DEBUG AUTH FAIL: Token User ID (int): {id_token_int} (Type: {type(id_token_int)})")
             cur.close()
             conn.close()
             return jsonify({"error": "No autorizado para responder a esta invitación"}), 403
        else:
             print(f"DEBUG AUTH OK: DB Creator ID ({id_db_creator}) == Token User ID ({id_token_int})")
             

        # Verificar que la invitación está realmente pendiente
        if participacion_info['estado'] != 'PENDIENTE':
             return jsonify({"error": "Esta invitación ya no está pendiente"}), 409 # Conflict

        # --- 2. Procesar la Respuesta ---
        if aceptar:
            # El usuario ACEPTA la invitación
            sql_update = """
                UPDATE participacion
                SET estado = 'ACEPTADA', fecha_union = CURRENT_TIMESTAMP
                WHERE id_participacion = %s;
            """
            cur.execute(sql_update, (id_participacion,))
            mensaje = "Invitación aceptada con éxito."
        else:
            # El usuario RECHAZA la invitación - la borramos
            sql_delete = "DELETE FROM participacion WHERE id_participacion = %s;"
            cur.execute(sql_delete, (id_participacion,))
            mensaje = "Invitación rechazada con éxito."

        conn.commit()
        cur.close()

        return jsonify({"mensaje": mensaje}), 200 # 200 OK

    except (Exception, psycopg2.DatabaseError) as error:
        import traceback
        print(f"ERROR DETALLADO en responder_invitacion:")
        traceback.print_exc() # <--- ESTO IMPRIME EL ERROR REAL
        print(f"ERROR (resumen) en responder_invitacion: {error}")
        print(f"Error en responder_invitacion: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al responder a la invitación"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint GET /api/invitaciones (Protegido con JWT) ---
# Devuelve la lista de invitaciones pendientes para el usuario autenticado.
@app.route('/api/invitaciones', methods=['GET'])
@jwt_required()
def listar_invitaciones_pendientes():
    id_usuario_actual = get_jwt_identity()

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Seleccionar la info de la invitación (incluyendo id_participacion) y detalles de la porra/creador
        sql = """
            SELECT
                pa.id_participacion, -- ¡ID necesario para aceptar/rechazar!
                p.id_porra,
                p.nombre_porra,
                p.ano,
                p.id_creador,
                u_creator.nombre AS nombre_creador,
                pa.fecha_union AS fecha_invitacion -- Renombrar para claridad semántica aquí
            FROM participacion pa
            JOIN porra p ON pa.id_porra = p.id_porra
            JOIN usuario u_creator ON p.id_creador = u_creator.id_usuario
            WHERE pa.id_usuario = %s AND pa.estado = 'PENDIENTE' -- Filtrar por usuario actual y estado PENDIENTE
            ORDER BY pa.fecha_union DESC; -- Ordenar por fecha de invitación (o creación de fila)
        """

        cur.execute(sql, (id_usuario_actual,))
        invitaciones = cur.fetchall()
        cur.close()

        lista_invitaciones = [dict(row) for row in invitaciones]

        return jsonify(lista_invitaciones), 200 # Devuelve la lista (puede ser vacía)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en listar_invitaciones_pendientes: {error}")
        return jsonify({"error": "Error interno al obtener las invitaciones pendientes"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint DELETE /api/porras/<id_porra>/participacion (Protegido) ---
# Permite al usuario autenticado salir de una porra (elimina su participación).
# No permite al creador salir de su propia porra por esta vía.
@app.route('/api/porras/<int:id_porra>/participacion', methods=['DELETE'])
@jwt_required()
def salir_de_porra(id_porra):
    # Obtener el ID del usuario del token JWT (el que intenta salir)
    id_usuario_actual_str = get_jwt_identity() # Sigue siendo string aquí

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Verificar que la porra existe y obtener el creador ---
        cur.execute("SELECT id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if not porra_info:
            cur.close()
            conn.close()
            return jsonify({"error": f"Porra con id {id_porra} no encontrada"}), 404

        # --- 2. Impedir que el creador salga ---
        id_db_creator = porra_info['id_creador'] # ID del creador (int)

        # Convertir el ID del usuario actual (del token) a entero para comparar
        try:
            id_usuario_actual_int = int(id_usuario_actual_str)
        except (ValueError, TypeError):
            print(f"ERROR: Identidad de token inválida para convertir a int: {id_usuario_actual_str}")
            cur.close()
            conn.close()
            return jsonify({"error": "Error interno de autorización"}), 500

        # --- *** CORRECCIÓN REALIZADA AQUÍ *** ---
        # Comprobar SI el usuario actual ES el creador
        if id_db_creator == id_usuario_actual_int:
             # Si son iguales, es el creador intentando salir, devolver error
             print(f"DEBUG: Intento de salida del creador (ID: {id_usuario_actual_int}) de la porra {id_porra}.")
             cur.close()
             conn.close()
             return jsonify({"error": "El creador no puede salir de la porra por esta vía. Considere eliminar la porra."}), 403 # Forbidden

        # --- 3. Si NO es el creador, intentar eliminar la participación del usuario actual ---
        # (El código llega aquí solo si el usuario actual NO es el creador)
        print(f"DEBUG: Usuario {id_usuario_actual_int} (no creador) intentando salir de la porra {id_porra}.")
        sql_delete = "DELETE FROM participacion WHERE id_porra = %s AND id_usuario = %s;"
        cur.execute(sql_delete, (id_porra, id_usuario_actual_int)) # Usar el ID entero

        rows_affected = cur.rowcount # Verificar si se eliminó algo

        conn.commit()
        cur.close()

        if rows_affected > 0:
            return jsonify({"mensaje": f"Has salido correctamente de la porra {id_porra}"}), 200 # 200 OK
        else:
            # Si no se afectaron filas, el usuario no era miembro (o ya había salido)
            # Esto podría pasar si alguien intenta salir dos veces, por ejemplo.
            return jsonify({"error": "No se encontró tu participación en esta porra o ya has salido"}), 404 # Not Found

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en salir_de_porra: {error}")
        import traceback
        traceback.print_exc() # Imprime más detalles del error en la consola de la API
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al intentar salir de la porra"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint DELETE /api/porras/<id_porra>/miembros/<id_usuario_a_eliminar> (Protegido) ---
# Permite al CREADOR de la porra eliminar a otro participante.
@app.route('/api/porras/<int:id_porra>/miembros/<int:id_usuario_a_eliminar>', methods=['DELETE'])
@jwt_required()
def eliminar_miembro_porra(id_porra, id_usuario_a_eliminar):
    id_usuario_actual = get_jwt_identity() # ID del usuario que hace la petición (debe ser el creador)

    # --- Validación: No eliminarse a sí mismo por esta vía ---
    if id_usuario_actual == id_usuario_a_eliminar:
         return jsonify({"error": "No puedes eliminarte a ti mismo usando este endpoint"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Verificar que la porra existe y que el usuario actual es el creador ---
        cur.execute("SELECT id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if not porra_info:
            return jsonify({"error": f"Porra con id {id_porra} no encontrada"}), 404
        # --- Autorización ---
        id_db_creator = porra_info['id_creador'] # Esto es un INT (ej: 12)
        id_token_str = id_usuario_actual        # Esto es un STR (ej: '12')

        # --- *** CORRECCIÓN AQUÍ *** ---
        # Intentar convertir el ID del token (string) a entero
        try:
            id_token_int = int(id_token_str)
        except (ValueError, TypeError):
            # Si la identidad del token no es un número válido por alguna razón
            print(f"ERROR: Identidad de token inválida para convertir a int: {id_token_str}")
            cur.close()
            conn.close()
            return jsonify({"error": "Error interno de autorización"}), 500

        # Ahora comparar ENTERO con ENTERO
        if id_db_creator != id_token_int:
             print(f"DEBUG AUTH FAIL: DB Creator ID: {id_db_creator} (Type: {type(id_db_creator)})")
             print(f"DEBUG AUTH FAIL: Token User ID (int): {id_token_int} (Type: {type(id_token_int)})")
             cur.close()
             conn.close()
             return jsonify({"error": "No autorizado para eliminar miembros (solo el creador)"}), 403
        else:
             print(f"DEBUG AUTH OK: DB Creator ID ({id_db_creator}) == Token User ID ({id_token_int})")
            

        # --- 2. Intentar eliminar la participación del miembro especificado ---
        sql_delete = "DELETE FROM participacion WHERE id_porra = %s AND id_usuario = %s;"
        cur.execute(sql_delete, (id_porra, id_usuario_a_eliminar))

        rows_affected = cur.rowcount

        conn.commit()
        cur.close()

        if rows_affected > 0:
            return jsonify({"mensaje": f"Usuario {id_usuario_a_eliminar} eliminado correctamente de la porra {id_porra}"}), 200 # o 204
        else:
            # Si no se afectaron filas, el usuario a eliminar no era miembro
            return jsonify({"error": f"No se encontró al usuario {id_usuario_a_eliminar} como miembro en esta porra"}), 404

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en eliminar_miembro_porra: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al eliminar miembro de la porra"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint PUT /api/porras/<id_porra> (Protegido) ---
# Permite al CREADOR de la porra editar su nombre.
@app.route('/api/porras/<int:id_porra>', methods=['PUT'])
@jwt_required()
def editar_nombre_porra(id_porra):
    id_usuario_actual = get_jwt_identity()

    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    nuevo_nombre = data.get('nombre_porra')

    # Validación
    if not nuevo_nombre or not isinstance(nuevo_nombre, str) or len(nuevo_nombre.strip()) == 0:
         return jsonify({"error": "Falta el campo 'nombre_porra' o está vacío"}), 400

    nuevo_nombre = nuevo_nombre.strip()

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Verificar que la porra existe y que el usuario actual es el creador ---
        cur.execute("SELECT id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if not porra_info:
            return jsonify({"error": f"Porra con id {id_porra} no encontrada"}), 404

        # --- Autorización ---
        id_db_creator = porra_info['id_creador'] # Esto es un INT (ej: 12)
        id_token_str = id_usuario_actual        # Esto es un STR (ej: '12')

        # --- *** CORRECCIÓN AQUÍ *** ---
        # Intentar convertir el ID del token (string) a entero
        try:
            id_token_int = int(id_token_str)
        except (ValueError, TypeError):
            # Si la identidad del token no es un número válido por alguna razón
            print(f"ERROR: Identidad de token inválida para convertir a int: {id_token_str}")
            cur.close()
            conn.close()
            return jsonify({"error": "Error interno de autorización"}), 500

        # Ahora comparar ENTERO con ENTERO
        if id_db_creator != id_token_int:
             print(f"DEBUG AUTH FAIL: DB Creator ID: {id_db_creator} (Type: {type(id_db_creator)})")
             print(f"DEBUG AUTH FAIL: Token User ID (int): {id_token_int} (Type: {type(id_token_int)})")
             cur.close()
             conn.close()
             return jsonify({"error": "No autorizado para editar esta porra (solo el creador)"}), 403
        else:
             print(f"DEBUG AUTH OK: DB Creator ID ({id_db_creator}) == Token User ID ({id_token_int})")

        # --- 2. Actualizar el nombre ---
        sql_update = "UPDATE porra SET nombre_porra = %s WHERE id_porra = %s;"
        cur.execute(sql_update, (nuevo_nombre, id_porra))

        rows_affected = cur.rowcount # Debería ser 1 si todo fue bien

        conn.commit()
        cur.close()

        if rows_affected > 0:
            # Devolver el objeto porra actualizado podría ser útil
            return jsonify({"mensaje": "Nombre de la porra actualizado con éxito", "id_porra": id_porra, "nuevo_nombre": nuevo_nombre}), 200
        else:
             # Esto no debería ocurrir si la verificación inicial funcionó, pero por si acaso
             return jsonify({"error": "No se pudo actualizar la porra (posiblemente ID incorrecto)"}), 404


    except (Exception, psycopg2.DatabaseError) as error:
        # Podría haber un error si intentas poner un nombre que viole un UNIQUE constraint (si lo tuvieras)
        print(f"Error en editar_nombre_porra: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al editar la porra"}), 500
    finally:
        if conn is not None:
            conn.close()


# --- NUEVO Endpoint DELETE /api/porras/<id_porra> (Protegido) ---
# Permite al CREADOR de la porra eliminarla (y sus datos asociados por CASCADE).
@app.route('/api/porras/<int:id_porra>', methods=['DELETE'])
@jwt_required()
def eliminar_porra(id_porra):
    id_usuario_actual = get_jwt_identity()

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Verificar que la porra existe y que el usuario actual es el creador ---
        cur.execute("SELECT id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if not porra_info:
            return jsonify({"error": f"Porra con id {id_porra} no encontrada"}), 404
          
        # --- Autorización ---
        id_db_creator = porra_info['id_creador'] # Esto es un INT (ej: 12)
        id_token_str = id_usuario_actual        # Esto es un STR (ej: '12')

        # --- *** CORRECCIÓN AQUÍ *** ---
        # Intentar convertir el ID del token (string) a entero
        try:
            id_token_int = int(id_token_str)
        except (ValueError, TypeError):
            # Si la identidad del token no es un número válido por alguna razón
            print(f"ERROR: Identidad de token inválida para convertir a int: {id_token_str}")
            cur.close()
            conn.close()
            return jsonify({"error": "Error interno de autorización"}), 500

        # Ahora comparar ENTERO con ENTERO
        if id_db_creator != id_token_int:
             print(f"DEBUG AUTH FAIL: DB Creator ID: {id_db_creator} (Type: {type(id_db_creator)})")
             print(f"DEBUG AUTH FAIL: Token User ID (int): {id_token_int} (Type: {type(id_token_int)})")
             cur.close()
             conn.close()
             return jsonify({"error": "No autorizado para eliminar esta porra (solo el creador)"}), 403
        else:
             print(f"DEBUG AUTH OK: DB Creator ID ({id_db_creator}) == Token User ID ({id_token_int})")

        # --- 2. Eliminar la porra (CASCADE se encargará del resto) ---
        sql_delete = "DELETE FROM porra WHERE id_porra = %s;"
        cur.execute(sql_delete, (id_porra,))

        rows_affected = cur.rowcount

        conn.commit()
        cur.close()

        if rows_affected > 0:
            return jsonify({"mensaje": f"Porra {id_porra} y todos sus datos asociados eliminados correctamente."}), 200 # O 204 No Content
        else:
             # No debería ocurrir si la verificación inicial pasó
             return jsonify({"error": "No se pudo eliminar la porra (posiblemente ID incorrecto)"}), 404

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en eliminar_porra: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al eliminar la porra"}), 500
    finally:
        if conn is not None:
            conn.close()
# --- NUEVO Endpoint GET /api/invitaciones/count (Protegido con JWT) ---
# Devuelve el número de invitaciones pendientes para el usuario autenticado.
@app.route('/api/invitaciones/count', methods=['GET'])
@jwt_required()
def contar_invitaciones_pendientes():
    id_usuario_actual = get_jwt_identity()

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor() # No necesitamos DictCursor aquí, solo un número

        # Consulta COUNT simple y eficiente
        sql = """
            SELECT COUNT(*)
            FROM participacion
            WHERE id_usuario = %s AND estado = 'PENDIENTE';
        """

        cur.execute(sql, (id_usuario_actual,))
        # fetchone() devolverá una tupla como (3,) o (0,)
        count = cur.fetchone()[0]
        cur.close()

        return jsonify({"pending_count": count}), 200 # Devolver el conteo

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en contar_invitaciones_pendientes: {error}")
        return jsonify({"error": "Error interno al contar las invitaciones"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint POST /api/password-reset/request ---
# Inicia el proceso de reseteo de contraseña para un email dado.
@app.route('/api/password-reset/request', methods=['POST'])
def request_password_reset():
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    email = data.get('email')

    if not email or not isinstance(email, str) or len(email.strip()) == 0:
         return jsonify({"error": "Falta el campo 'email' o es inválido"}), 400

    email = email.strip().lower()

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Buscar usuario por email ---
        print(f"DEBUG: Buscando usuario con email: {email}") # Debug
        cur.execute("SELECT id_usuario FROM usuario WHERE email = %s;", (email,))
        user = cur.fetchone()

        if user:
            # --- Usuario encontrado ---
            id_usuario = user['id_usuario']
            print(f"DEBUG: Usuario encontrado: id={id_usuario}") # Debug

            # --- 2. Generar Token Seguro ---
            token = secrets.token_urlsafe(32)
            print(f"DEBUG: Token generado (primeros 5 chars): {token[:5]}...") # Debug (NO imprimir token completo)

            # --- 3. Calcular Fecha de Expiración (ej: 30 minutos desde ahora) ---
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
            print(f"DEBUG: Token expira (UTC): {expires_at}") # Debug

            # --- 4. Invalidar tokens anteriores para este usuario ---
            print(f"DEBUG: Borrando tokens antiguos para usuario {id_usuario}...") # Debug
            sql_delete_old = "DELETE FROM password_reset_token WHERE id_usuario = %s;"
            cur.execute(sql_delete_old, (id_usuario,))
            print(f"DEBUG: Tokens antiguos borrados: {cur.rowcount}") # Debug

            # --- 5. Guardar el nuevo token en la BD ---
            print(f"DEBUG: Insertando nuevo token para usuario {id_usuario}...") # Debug
            sql_insert_token = """
                INSERT INTO password_reset_token (id_usuario, token, fecha_expiracion)
                VALUES (%s, %s, %s);
            """
            cur.execute(sql_insert_token, (id_usuario, token, expires_at))
            print("DEBUG: Nuevo token insertado.") # Debug

            # --- CAMBIO DEEP LINK ---
            # Usamos el esquema personalizado para el enlace de reseteo
            reset_link = f"https://f1-porra-app-links.web.app/reset-password?token={token}" # <-- USA TU DOMINIO
            # --- FIN CAMBIO DEEP LINK ---

            try:
                msg = Message(subject="Restablece tu contraseña / Reset your password / Réinitialisez votre mot de passe / Redefine a tua palavra-passe / Restableix la teva contrasenya - F1 Porra App",
                              recipients=[email])

                msg.body = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ES • Restablecimiento de contraseña
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Hola,

Has solicitado restablecer tu contraseña de F1 Porra App.
Haz clic en el siguiente enlace para crear una nueva contraseña (el enlace caduca en 24 horas):
{reset_link}

Si no solicitaste este cambio, ignora este correo.

Saludos,
El equipo de F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EN • Password reset
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Hi,

You requested to reset your F1 Porra App password.
Click the link below to set a new password (the link expires in 24 hours):
{reset_link}

If you didn’t request this change, please ignore this email.

Regards,
The F1 Porra App Team


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FR • Réinitialisation du mot de passe
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bonjour,

Vous avez demandé à réinitialiser votre mot de passe F1 Porra App.
Cliquez sur le lien ci-dessous pour définir un nouveau mot de passe (le lien expire dans 24 heures) :
{reset_link}

Si vous n’êtes pas à l’origine de cette demande, ignorez cet e-mail.

Cordialement,
L’équipe F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PT • Redefinição de palavra-passe
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Olá,

Pediste redefinir a tua palavra-passe da F1 Porra App.
Clica no link abaixo para definir uma nova palavra-passe (o link expira em 24 horas):
{reset_link}

Se não pediste esta alteração, ignora este e-mail.

Cumprimentos,
A equipa da F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CA • Restabliment de contrasenya
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Hola,

Has sol·licitat restablir la teva contrasenya de F1 Porra App.
Fes clic a l’enllaç següent per crear-ne una de nova (l’enllaç caduca en 24 hores):
{reset_link}

Si no has sol·licitat aquest canvi, ignora aquest correu.

Salutacions,
L’equip de F1 Porra App
"""

                print(f"DEBUG: Intentando enviar email de reseteo a {email}...") # Debug
                mail.send(msg)
                print(f"DEBUG: Email de reseteo enviado (aparentemente) a {email}.") # Debug

            except Exception as e_mail:
                 print(f"ERROR al enviar email de reseteo a {email}: {e_mail}")
                 # Aún así hacemos commit del token y devolvemos éxito genérico
                 pass

            # --- 7. Confirmar transacción (guardar token) ---
            print("DEBUG: Ejecutando commit (token)...") # Debug
            conn.commit()
            print("DEBUG: Commit de token exitoso.") # Debug

        else:
            # --- Usuario NO encontrado ---
            print(f"DEBUG: Solicitud de reseteo para email no registrado: {email}")
            pass # No hacer nada

        # --- 8. Respuesta Genérica ---
        print("DEBUG: Devolviendo respuesta genérica al cliente.") # Debug
        cur.close()
        return jsonify({"mensaje": "Si tu correo está registrado, recibirás instrucciones para restablecer tu contraseña en breve."}), 200

    except (Exception, psycopg2.DatabaseError) as error:
        import traceback
        print(f"ERROR DETALLADO en request_password_reset:")
        traceback.print_exc()
        print(f"ERROR (resumen) en request_password_reset: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al procesar la solicitud de reseteo"}), 500
    finally:
        if conn is not None:
            if not conn.closed:
                conn.close()
                print("DEBUG: Conexión cerrada en finally (request_password_reset).") # Debug

# --- NUEVO Endpoint POST /api/password-reset/confirm ---
# Completa el proceso de reseteo usando el token y la nueva contraseña.
@app.route('/api/password-reset/confirm', methods=['POST'])
def confirm_password_reset():
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    token = data.get('token')
    new_password = data.get('new_password')

    # Validación básica
    if not token or not new_password:
        return jsonify({"error": "Faltan campos requeridos (token, new_password)"}), 400
    if not isinstance(token, str) or not isinstance(new_password, str) or len(new_password) < 6: # Añadir mínima longitud
         return jsonify({"error": "Token o nueva contraseña inválidos (mínimo 6 caracteres para contraseña)"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Buscar y Validar el Token ---
        # Buscamos un token que coincida, NO esté usado y NO haya expirado
        now_utc = datetime.now(timezone.utc)
        sql_find_token = """
            SELECT id_usuario, fecha_expiracion
            FROM password_reset_token
            WHERE token = %s AND utilizado = FALSE AND fecha_expiracion > %s;
        """
        cur.execute(sql_find_token, (token, now_utc))
        token_data = cur.fetchone()

        if not token_data:
            # No se encontró token válido (no existe, ya se usó o expiró)
            return jsonify({"error": "Token inválido o expirado"}), 400 # O 404

        id_usuario = token_data['id_usuario']

        # --- 2. Hashear la Nueva Contraseña ---
        new_password_hash = generate_password_hash(new_password)

        # --- 3. Actualizar la Contraseña del Usuario ---
        sql_update_pass = "UPDATE usuario SET password_hash = %s WHERE id_usuario = %s;"
        cur.execute(sql_update_pass, (new_password_hash, id_usuario))

        # --- 4. Invalidar el Token (eliminándolo o marcándolo como usado) ---
        # Eliminar es más simple y mantiene la tabla limpia
        sql_delete_token = "DELETE FROM password_reset_token WHERE token = %s;"
        cur.execute(sql_delete_token, (token,))
        # Alternativa (marcar como usado):
        # sql_invalidate_token = "UPDATE password_reset_token SET utilizado = TRUE WHERE token = %s;"
        # cur.execute(sql_invalidate_token, (token,))

        conn.commit()
        cur.close()

        return jsonify({"mensaje": "Contraseña actualizada correctamente."}), 200 # 200 OK

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en confirm_password_reset: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al confirmar el reseteo de contraseña"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint GET /api/verify-email ---
# --- REEMPLAZA ESTA FUNCIÓN COMPLETA en mi_api.py ---
@app.route('/api/verify-email', methods=['GET'])
def verify_email():
    token = request.args.get('token') # Obtener token de los parámetros query
    print(f"DEBUG [verify_email]: Received verification request with token (first 5 chars): {token[:5]}..." if token else "No token received.") # Log token recibido (parcialmente)

    # --- HTML para respuestas (sin cambios) ---
    html_success = """
    <!DOCTYPE html><html><head><title>Verificación Exitosa</title><style>body{font-family: sans-serif; padding: 20px; text-align: center; background-color: #e8f5e9;} .card{background-color: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); display: inline-block;} h1{color: #2e7d32;} p{font-size: 1.1em;}</style></head>
    <body><div class="card"><h1>&#10004; ¡Email Verificado!</h1><p>Tu dirección de correo ha sido verificada correctamente.</p><p>Ya puedes cerrar esta pestaña e iniciar sesión en la aplicación F1 Porra.</p></div></body></html>
    """
    def create_html_error(message):
        return f"""
        <!DOCTYPE html><html><head><title>Error de Verificación</title><style>body{{font-family: sans-serif; padding: 20px; text-align: center; background-color: #ffebee;}} .card{{background-color: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); display: inline-block;}} h1{{color: #c62828;}} p{{font-size: 1.1em;}}</style></head>
        <body><div class="card"><h1>&#10060; Error de Verificación</h1><p>No se pudo verificar tu email.</p><p><strong>Motivo:</strong> {message}</p><p>Por favor, intenta registrarte de nuevo o contacta con el soporte si el problema persiste.</p></div></body></html>
        """
    # --- Fin HTML ---

    if not token:
        print("ERROR [verify_email]: No token provided in the request.")
        response = make_response(create_html_error("Falta el token de verificación en el enlace."), 400)
        response.headers['Content-Type'] = 'text/html'
        return response

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("DEBUG [verify_email]: Database connection established.")

        # --- 1. Buscar el token y verificar su expiración ---
        now_utc = datetime.now(timezone.utc)
        print(f"DEBUG [verify_email]: Searching for token (first 5: {token[:5]}...)")
        sql_find_token = """
            SELECT id_usuario, token_verificacion_expira, email_verificado
            FROM usuario
            WHERE token_verificacion = %s;
        """
        cur.execute(sql_find_token, (token,))
        user_data = cur.fetchone()

        if not user_data:
            print(f"WARN [verify_email]: Token not found or already used/cleared (first 5: {token[:5]}...).")
            response = make_response(create_html_error("El token de verificación es inválido o ya ha sido utilizado."), 400)
            response.headers['Content-Type'] = 'text/html'
            cur.close()
            conn.close()
            return response

        id_usuario_a_verificar = user_data['id_usuario']
        print(f"DEBUG [verify_email]: Token found for user ID: {id_usuario_a_verificar}.")

        # Verificar si el email ya está verificado
        if user_data['email_verificado']:
             print(f"INFO [verify_email]: Email for user {id_usuario_a_verificar} is already verified.")
             response = make_response(html_success, 200) # Ya está verificado, mostrar éxito
             response.headers['Content-Type'] = 'text/html'
             cur.close()
             conn.close()
             return response

        # Verificar si el token ha expirado
        expiry_time = user_data['token_verificacion_expira']
        print(f"DEBUG [verify_email]: Token expires at (UTC): {expiry_time}. Current time (UTC): {now_utc}")
        if expiry_time is None or now_utc > expiry_time:
             print(f"WARN [verify_email]: Token expired for user {id_usuario_a_verificar}.")
             # Limpiar token expirado para evitar reutilización
             print(f"DEBUG [verify_email]: Clearing expired token for user {id_usuario_a_verificar}.")
             sql_clear_expired = "UPDATE usuario SET token_verificacion = NULL, token_verificacion_expira = NULL WHERE id_usuario = %s;"
             cur.execute(sql_clear_expired, (id_usuario_a_verificar,))
             conn.commit() # <-- IMPORTANTE: Commit la limpieza del token expirado
             print(f"DEBUG [verify_email]: Expired token cleared and committed for user {id_usuario_a_verificar}.")
             cur.close()
             conn.close()
             response = make_response(create_html_error("El token de verificación ha expirado."), 400)
             response.headers['Content-Type'] = 'text/html'
             return response

        # --- Token válido, no expirado y usuario no verificado ---
        print(f"DEBUG [verify_email]: Token is valid for user {id_usuario_a_verificar}. Proceeding with verification.")

        # --- 2. Marcar email como verificado y limpiar token ---
        sql_verify = """
            UPDATE usuario
            SET email_verificado = TRUE,
                token_verificacion = NULL,
                token_verificacion_expira = NULL
            WHERE id_usuario = %s;
        """
        print(f"DEBUG [verify_email]: Executing UPDATE statement for user {id_usuario_a_verificar}...")
        cur.execute(sql_verify, (id_usuario_a_verificar,))
        print(f"DEBUG [verify_email]: UPDATE executed. Rows affected: {cur.rowcount}")

        # --- 3. COMMIT DE LA TRANSACCIÓN ---
        # ¡Este es el paso crítico! Asegurarse de que los cambios se guardan en la BD.
        print(f"DEBUG [verify_email]: Committing transaction for user {id_usuario_a_verificar}...")
        conn.commit()
        print(f"DEBUG [verify_email]: Transaction committed successfully for user {id_usuario_a_verificar}.")

        cur.close()
        print("DEBUG [verify_email]: Cursor closed.")
        conn.close()
        print("DEBUG [verify_email]: Connection closed.")

        response = make_response(html_success, 200)
        response.headers['Content-Type'] = 'text/html'
        print(f"INFO [verify_email]: Verification successful for user {id_usuario_a_verificar}. Returning success HTML.")
        return response

    except psycopg2.Error as db_err: # Capturar errores específicos de BD
        print(f"!!!!!!!! DATABASE ERROR [verify_email] !!!!!!!!")
        print(f"Error Type: {type(db_err)}")
        print(f"Error Details: {db_err}")
        if conn: conn.rollback() # Deshacer cambios si hubo error DB
        response = make_response(create_html_error(f"Error de base de datos durante la verificación."), 500)
        response.headers['Content-Type'] = 'text/html'
        return response

    except Exception as error:
        import traceback
        print(f"!!!!!!!! UNEXPECTED ERROR [verify_email] !!!!!!!!")
        traceback.print_exc() # Imprime el stack trace completo en los logs de la API
        if conn: conn.rollback()
        response = make_response(create_html_error(f"Error interno del servidor durante la verificación."), 500)
        response.headers['Content-Type'] = 'text/html'
        return response
    finally:
        # Asegurar que la conexión se cierra si todavía está abierta
        if conn is not None and not conn.closed:
            try:
                cur.close() # Intenta cerrar cursor si existe
            except: pass
            conn.close()
            print("DEBUG [verify_email]: Connection closed in finally block.")
# --- FIN FUNCIÓN verify_email MODIFICADA ---

# --- NUEVO Endpoint POST /api/resend-verification ---
# Reenvía el email de verificación si la cuenta existe y no está verificada.
@app.route('/api/resend-verification', methods=['POST'])
def resend_verification_email():
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    email = data.get('email')

    if not email or not isinstance(email, str) or len(email.strip()) == 0:
         return jsonify({"error": "Falta el campo 'email' o es inválido"}), 400

    email = email.strip().lower()

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Buscar usuario por email y verificar su estado ---
        # Seleccionamos también el nombre para personalizar el email
        sql_find_user = "SELECT id_usuario, nombre, email_verificado FROM usuario WHERE email = %s;"
        cur.execute(sql_find_user, (email,))
        user = cur.fetchone()

        # --- 2. Comprobar si se debe reenviar ---
        if user and not user['email_verificado']:
            # --- Usuario existe y NO está verificado: Proceder a reenviar ---
            id_usuario = user['id_usuario']
            nombre_usuario = user['nombre']

            # --- 3. Generar NUEVO Token y Expiración ---
            nuevo_token = secrets.token_urlsafe(32)
            nueva_expiracion = datetime.now(timezone.utc) + timedelta(days=1) # Nueva validez de 1 día

            # --- 4. Actualizar el token en la BD ---
            sql_update_token = """
                UPDATE usuario
                SET token_verificacion = %s, token_verificacion_expira = %s
                WHERE id_usuario = %s;
            """
            cur.execute(sql_update_token, (nuevo_token, nueva_expiracion, id_usuario))
            # --- DEBUG PRINT 1 ---
            print(f"DEBUG [registrar_usuario]: Token generado: {nuevo_token}")

            # --- CAMBIO DEEP LINK ---
            # Usamos el esquema personalizado para el enlace de verificación
            verification_link = f"https://f1-porra-app-links.web.app/verify-email?token={nuevo_token}" # <-- USA TU DOMINIO
            # --- FIN CAMBIO DEEP LINK ---

            print(f"DEBUG [resend_verification]: Enlace generado para email (Deep Link): {verification_link}")


            try:
                msg = Message(subject="Reenvío verificación / Resend verification / Renvoi de vérification / Reenviar verificação / Reenviament de verificació - F1 Porra App",
                              recipients=[email])
                msg.body = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ES • Nuevo enlace de verificación
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Hola {nombre_usuario},

Te enviamos un nuevo enlace para verificar tu correo electrónico (el enlace caduca en 24 horas):
{verification_link}

Si ya verificaste tu cuenta o no solicitaste este correo, ignóralo.

Saludos,
El equipo de F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EN • New verification link
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Hi {nombre_usuario},

Here is a new link to verify your email address (the link expires in 24 hours):
{verification_link}

If you have already verified your account or didn’t request this email, just ignore it.

Regards,
The F1 Porra App Team


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FR • Nouveau lien de vérification
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bonjour {nombre_usuario},

Voici un nouveau lien pour vérifier votre adresse e-mail (le lien expire dans 24 heures) :
{verification_link}

Si vous avez déjà vérifié votre compte ou n’avez pas demandé cet e-mail, ignorez-le.

Cordialement,
L’équipe F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PT • Novo link de verificação
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Olá {nombre_usuario},

Aqui tens um novo link para verificar o teu e-mail (o link expira em 24 horas):
{verification_link}

Se já verificaste a tua conta ou não pediste este e-mail, ignora-o.

Cumprimentos,
A equipa da F1 Porra App


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CA • Nou enllaç de verificació
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Hola {nombre_usuario},

T’enviem un nou enllaç per verificar el teu correu electrònic (l’enllaç caduca en 24 hores):
{verification_link}

Si ja has verificat el compte o no has sol·licitat aquest correu, ignora’l.

Salutacions,
L’equip de F1 Porra App
"""



                print(f"DEBUG: Intentando REenviar email de verificación a {email}...") # Debug
                mail.send(msg)
                print(f"DEBUG: Email de verificación REenviado (aparentemente) a {email}.") # Debug

            except Exception as e_mail:
                 print(f"ERROR al REenviar email de verificación a {email}: {e_mail}")
                 # Si falla el email, DESHACEMOS el cambio del token en la BD
                 conn.rollback()
                 cur.close()
                 conn.close()
                 # Devolvemos error interno porque el proceso falló a medio camino
                 return jsonify({"error": "No se pudo reenviar el email de verificación. Inténtalo de nuevo más tarde."}), 500

            # --- 6. Confirmar transacción (guardar nuevo token) ---
            conn.commit()
            print(f"DEBUG: Nuevo token de verificación guardado para usuario {id_usuario}.") # Debug

        else:
            # --- Usuario NO encontrado o YA verificado ---
            # No hacemos nada en la BD, no enviamos email.
            if user:
                 print(f"DEBUG: Solicitud de reenvío para email ya verificado: {email}")
            else:
                 print(f"DEBUG: Solicitud de reenvío para email no registrado: {email}")
            pass

        # --- 7. Respuesta Genérica (SIEMPRE igual) ---
        cur.close()
        return jsonify({"mensaje": "Si tu cuenta existe y aún no está verificada, se ha reenviado un email de verificación."}), 200

    except (Exception, psycopg2.DatabaseError) as error:
        import traceback
        print(f"ERROR DETALLADO en resend_verification_email:")
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al procesar la solicitud de reenvío"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()


# --- Endpoint GET /api/drivers/race/<id_carrera> (MODIFICADO v2 con flag ?for_betting) ---
# Devuelve los pilotos para una carrera específica.
# Si ?for_betting=true, devuelve solo los activos para apuesta.
# Si no, devuelve todos los registrados para esa carrera en piloto_carrera_detalle,
# o como fallback, todos los de piloto_temporada.
@app.route('/api/drivers/race/<int:id_carrera>', methods=['GET'])
def get_drivers_for_race(id_carrera):
    conn = None
    pilotos_final = []
    # Obtener el flag ?for_betting=true/false (o cualquier valor para true)
    for_betting_flag = request.args.get('for_betting', default="false").lower() == 'true'

    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Obtener año (necesario para fallback)
        cur.execute("SELECT ano FROM carrera WHERE id_carrera = %s;", (id_carrera,))
        carrera_info = cur.fetchone()
        if not carrera_info:
            cur.close(); conn.close()
            return jsonify({"error": f"Carrera con id {id_carrera} no encontrada"}), 404
        ano_carrera = carrera_info['ano']

        # 2. Intentar obtener pilotos específicos de la carrera
        # Modificamos la query para incluir el filtro 'activo_para_apuesta' SOLO si for_betting=true
        where_clause = "WHERE id_carrera = %s"
        params = [id_carrera]
        if for_betting_flag:
            where_clause += " AND activo_para_apuesta = TRUE"
            print(f"DEBUG [GetDriversRace]: Buscando pilotos activos para apuesta para carrera {id_carrera}")
        else:
             print(f"DEBUG [GetDriversRace]: Buscando TODOS los pilotos registrados para carrera {id_carrera}")

        sql_race_specific_drivers = f"""
            SELECT
                codigo_piloto,
                nombre_completo_carrera AS nombre_completo,
                escuderia_carrera AS escuderia,
                color_fondo_hex_carrera AS color_fondo_hex,
                color_texto_hex_carrera AS color_texto_hex
            FROM piloto_carrera_detalle
            {where_clause}
            ORDER BY codigo_piloto ASC;
        """
        cur.execute(sql_race_specific_drivers, tuple(params))
        pilotos_db = cur.fetchall()

        # 3. Fallback a piloto_temporada SI NO SE ENCONTRÓ NADA en piloto_carrera_detalle
        #    OJO: Si for_betting=true y no se encontró nada, el fallback devolverá TODOS los de temporada.
        #         Esto es intencional para el caso de principio de temporada sin datos específicos.
        if not pilotos_db:
            print(f"DEBUG [GetDriversRace]: No se encontraron pilotos en piloto_carrera_detalle ({'activos' if for_betting_flag else 'todos'}). Usando piloto_temporada {ano_carrera}.")
            sql_season_drivers = """
                SELECT codigo_piloto, nombre_completo, escuderia, color_fondo_hex, color_texto_hex
                FROM piloto_temporada WHERE ano = %s ORDER BY codigo_piloto ASC;
            """
            cur.execute(sql_season_drivers, (ano_carrera,))
            pilotos_db = cur.fetchall()

            if not pilotos_db:
                 cur.close(); conn.close()
                 return jsonify({"error": f"No hay pilotos definidos para carrera {id_carrera} ni temporada {ano_carrera}"}), 404

        # 4. Formatear salida (sin cambios)
        for piloto in pilotos_db:
            pilotos_final.append({
                "code": piloto['codigo_piloto'],
                "nombre_completo": piloto.get('nombre_completo', piloto['codigo_piloto']),
                "escuderia": piloto.get('escuderia', ''),
                "bgColorHex": piloto.get('color_fondo_hex', '#CCCCCC'),
                "textColorHex": piloto.get('color_texto_hex', '#000000')
            })

        cur.close()
        conn.close()
        return jsonify(pilotos_final), 200

    except psycopg2.DatabaseError as db_error: # Resto sin cambios
        print(f"Error DB en get_drivers_for_race: {db_error}");
        if conn: conn.close()
        return jsonify({"error": "Error DB obteniendo pilotos"}), 500
    except Exception as error: # Resto sin cambios
        print(f"Error general en get_drivers_for_race: {error}"); import traceback; traceback.print_exc()
        if conn: conn.close()
        return jsonify({"error": "Error interno obteniendo pilotos"}), 500
# --- FIN Endpoint GET Drivers/Race MODIFICADO ---

# --- Endpoint GET /api/drivers/<year> (CORREGIDO - Falta una coma) ---
@app.route('/api/drivers/<string:year>', methods=['GET'])
def get_drivers_for_year(year):
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Consultar la tabla piloto_temporada, AÑADIENDO nombre_completo
        sql = """
            SELECT
                codigo_piloto,
                nombre_completo,
                color_fondo_hex,
                color_texto_hex
            FROM piloto_temporada
            WHERE ano = %s
            ORDER BY codigo_piloto ASC;
        """
        cur.execute(sql, (year,))
        pilotos_db = cur.fetchall()
        cur.close()

        if not pilotos_db:
            # No cerrar conexión aquí todavía si no hay error
            return jsonify({"error": f"No hay lista de pilotos definida para el año {year}"}), 404

        lista_pilotos = []
        for piloto in pilotos_db:
            lista_pilotos.append({
                "code": piloto['codigo_piloto'],
                # Usar .get() para seguridad si 'nombre_completo' pudiera no existir
                "nombre_completo": piloto.get('nombre_completo', piloto['codigo_piloto']),
                # <<< CORRECCIÓN: Faltaba una coma aquí >>>
                "bgColorHex": piloto['color_fondo_hex'],
                "textColorHex": piloto['color_texto_hex']
            })

        # Cerrar conexión antes de retornar éxito
        conn.close()
        return jsonify(lista_pilotos), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en get_drivers_for_year (BD): {error}")
        # Asegurarse de cerrar conexión en caso de error
        if conn: conn.close()
        return jsonify({"error": "Error interno al obtener la lista de pilotos"}), 500
    finally:
        # Doble verificación por si la conexión sigue abierta
        if conn is not None and not conn.closed:
            conn.close()
# --- FIN Endpoint CORREGIDO ---

# --- NUEVO Endpoint PUT /api/profile/password (Protegido) ---
# Permite al usuario autenticado cambiar su propia contraseña
@app.route('/api/profile/password', methods=['PUT'])
@jwt_required()
def change_password():
    id_usuario_actual = get_jwt_identity() # Obtiene ID del token (string)

    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    # Validación básica
    if not current_password or not new_password:
        return jsonify({"error": "Faltan campos requeridos (current_password, new_password)"}), 400
    if not isinstance(current_password, str) or not isinstance(new_password, str) or len(new_password) < 6:
        return jsonify({"error": "Contraseñas inválidas (nueva debe tener mín. 6 caracteres)"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Obtener hash de contraseña actual del usuario ---
        cur.execute("SELECT password_hash FROM usuario WHERE id_usuario = %s;", (id_usuario_actual,))
        user = cur.fetchone()

        if not user:
             # Esto no debería pasar si el token es válido, pero por seguridad
             cur.close()
             conn.close()
             # Devolvemos 401 o 404, 401 parece más apropiado si el token era válido pero el user no existe
             return jsonify({"error": "Usuario no encontrado"}), 401

        current_hash = user['password_hash']

        # --- 2. Verificar la contraseña actual ---
        if not check_password_hash(current_hash, current_password):
            cur.close()
            conn.close()
            # ¡Importante! Devolver 401 Unauthorized si la contraseña actual no coincide
            return jsonify({"error": "La contraseña actual es incorrecta"}), 401

        # --- 3. Hashear y actualizar la nueva contraseña ---
        new_password_hash = generate_password_hash(new_password)
        sql_update = "UPDATE usuario SET password_hash = %s WHERE id_usuario = %s;"
        cur.execute(sql_update, (new_password_hash, id_usuario_actual))

        conn.commit()
        cur.close()

        return jsonify({"mensaje": "Contraseña actualizada correctamente."}), 200 # 200 OK

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en change_password: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al cambiar la contraseña"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- FIN NUEVO Endpoint ---


# --- Endpoint GET /api/porras/<id_porra>/my-races-bet-status (MODIFICADO v2 con estado_apuesta) ---
# --- Endpoint GET /api/porras/<id_porra>/my-races-bet-status (MODIFICADO v3 con detalle qualy/carrera) ---
@app.route('/api/porras/<int:id_porra>/my-races-bet-status', methods=['GET'])
@jwt_required()
def get_my_races_with_bet_status(id_porra):
    id_usuario_actual_str = get_jwt_identity()
    try:
        id_usuario_actual = int(id_usuario_actual_str)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Obtener año y verificar membresía (sin cambios)
        cur.execute("SELECT ano FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone() 
        if not porra_info: 
            return jsonify({"error": "Porra no encontrada"}), 404
        ano_porra = porra_info['ano']
        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual)) 
        if cur.fetchone() is None: 
            return jsonify({"error": "No eres miembro activo."}), 403

        # --- MODIFICADO: Obtener fecha carrera y desglose de apuestas (qualy vs carrera) ---
        sql_get_races_and_status = """
            SELECT
                c.id_carrera, c.ano, c.desc_carrera, 
                c.fecha_limite_apuesta, -- Límite Clasificación
                c.fecha_limite_carrera, -- Límite Carrera (NUEVO)
                
                -- Check si tiene apuesta general (fila existe)
                (CASE WHEN a.id_apuesta IS NOT NULL THEN TRUE ELSE FALSE END) as has_bet_entry,
                
                -- Check específico Clasificación (NUEVO)
                (CASE WHEN a.posiciones_clasificacion IS NOT NULL THEN TRUE ELSE FALSE END) as has_bet_qualy,
                
                -- Check específico Carrera (NUEVO)
                (CASE WHEN a.posiciones IS NOT NULL THEN TRUE ELSE FALSE END) as has_bet_race,
                
                a.estado_apuesta,
                (CASE WHEN c.resultado_detallado IS NOT NULL THEN TRUE ELSE FALSE END) as has_results
            FROM carrera c
            LEFT JOIN apuesta a ON c.id_carrera = a.id_carrera
                                AND a.id_porra = %s
                                AND a.id_usuario = %s
            WHERE c.ano = %s
            ORDER BY c.id_carrera;
        """
        cur.execute(sql_get_races_and_status, (id_porra, id_usuario_actual, ano_porra))
        races_with_status_raw = cur.fetchall()
        cur.close()

        # Formatear fechas y devolver
        lista_resultado = []
        for row_raw in races_with_status_raw:
            row = dict(row_raw)
            # Formatear fechas a ISO string si existen
            if 'fecha_limite_apuesta' in row and isinstance(row['fecha_limite_apuesta'], datetime):
                 row['fecha_limite_apuesta'] = row['fecha_limite_apuesta'].isoformat()
            
            if 'fecha_limite_carrera' in row and isinstance(row['fecha_limite_carrera'], datetime):
                 row['fecha_limite_carrera'] = row['fecha_limite_carrera'].isoformat()
            
            row['has_results'] = bool(row.get('has_results', False))
            row['has_bet_entry'] = bool(row.get('has_bet_entry', False))
            row['has_bet_qualy'] = bool(row.get('has_bet_qualy', False))
            row['has_bet_race'] = bool(row.get('has_bet_race', False))
            row['estado_apuesta'] = row.get('estado_apuesta')
            
            lista_resultado.append(row)

        return jsonify(lista_resultado), 200

    except psycopg2.DatabaseError as db_error:
        print(f"Error DB en get_my_races_with_bet_status: {db_error}")
        if conn: conn.close()
        return jsonify({"error": "Error DB obteniendo estado carreras"}), 500
    except Exception as error:
        print(f"ERROR DETALLADO en get_my_races_with_bet_status:"); import traceback; traceback.print_exc()
        if conn: conn.close()
        return jsonify({"error": "Error interno al obtener estado de apuestas de carreras"}), 500
    finally:
        if conn is not None and not conn.closed:
             conn.close()

# --- Endpoint GET /api/porras/publicas (MODIFICADO para usar tipo_porra) ---
@app.route('/api/porras/publicas', methods=['GET'])
def obtener_porras_publicas():
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- MODIFICADO: Filtrar por tipo_porra = 'PUBLICA' ---
        sql = """
            SELECT
                p.id_porra,
                p.nombre_porra,
                p.ano,
                p.id_creador,
                u_creator.nombre as nombre_creador,
                p.fecha_creacion,
                p.tipo_porra -- Devolver tipo_porra
            FROM porra p
            JOIN usuario u_creator ON p.id_creador = u_creator.id_usuario
            WHERE p.tipo_porra = 'PUBLICA' -- <<< Filtrar por el nuevo tipo
            ORDER BY p.ano DESC, p.nombre_porra ASC;
        """
        cur.execute(sql)
        porras_publicas = cur.fetchall()
        cur.close()

        lista_porras = []
        for row in porras_publicas:
            porra_dict = dict(row)
            # Formatear fecha si existe
            if 'fecha_creacion' in porra_dict and isinstance(porra_dict['fecha_creacion'], datetime):
                 porra_dict['fecha_creacion'] = porra_dict['fecha_creacion'].isoformat()
            lista_porras.append(porra_dict)

        return jsonify(lista_porras), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_porras_publicas: {error}")
        return jsonify({"error": "Error interno al obtener las porras públicas"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()


# --- Endpoint POST /api/porras/publica/<id_porra>/join (CORREGIDO) ---
# Permite a un usuario autenticado unirse a una porra pública específica
@app.route('/api/porras/publica/<int:id_porra>/join', methods=['POST'])
@jwt_required() # Requiere que el usuario esté logueado
def unirse_porra_publica(id_porra):
    id_usuario_actual_str = get_jwt_identity()
    try:
         id_usuario_actual = int(id_usuario_actual_str)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Verificar si la porra existe y es pública (CORREGIDO: Usar tipo_porra)
        cur.execute("SELECT tipo_porra, id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()

        if not porra_info:
            return jsonify({"error": "Porra no encontrada"}), 404
        
        # --- CORRECCIÓN: Verificar tipo_porra en lugar de es_publica ---
        if porra_info['tipo_porra'] != 'PUBLICA':
            return jsonify({"error": "Esta porra no es pública"}), 403 # Forbidden
        # ---------------------------------------------------------------

        # 2. (Opcional) Impedir que el creador se una a sí mismo
        if porra_info['id_creador'] == id_usuario_actual:
            return jsonify({"mensaje": "Ya eres el creador de esta porra"}), 200 

        # 3. Verificar si el usuario ya es miembro
        cur.execute("SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s;", (id_porra, id_usuario_actual))
        ya_es_miembro = cur.fetchone()

        if ya_es_miembro:
            return jsonify({"error": "Ya eres miembro de esta porra"}), 409 # Conflict

        # 4. Si es pública y no es miembro, añadirlo
        sql_insert = """
            INSERT INTO participacion (id_porra, id_usuario, estado)
            VALUES (%s, %s, 'ACEPTADA')
            ON CONFLICT (id_porra, id_usuario) DO NOTHING;
        """
        cur.execute(sql_insert, (id_porra, id_usuario_actual))

        conn.commit()
        cur.close()

        return jsonify({"mensaje": "Te has unido a la porra pública correctamente"}), 200 

    except psycopg2.Error as db_error: 
        print(f"Error de base de datos en unirse_porra_publica: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al intentar unirse a la porra"}), 500
    except Exception as error:
        print(f"Error inesperado en unirse_porra_publica: {error}")
        import traceback
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al procesar la solicitud"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- Endpoint GET /api/porras/<id_porra>/my-bets (MODIFICADO v2 con estado_apuesta) ---
# --- Endpoint GET /api/porras/<id_porra>/my-bets (MODIFICADO v3 con posiciones_clasificacion) ---
@app.route('/api/porras/<int:id_porra>/my-bets', methods=['GET'])
@jwt_required()
def get_all_my_bets_in_porra(id_porra):
    id_usuario_actual_str = get_jwt_identity()
    try:
        id_usuario_actual = int(id_usuario_actual_str)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Verificar membresía (sin cambios)
        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual))
        if cur.fetchone() is None: return jsonify({"error": "No eres miembro activo."}), 403

        # --- MODIFICADO: Obtener también posiciones_clasificacion ---
        sql_get_bets = """
            SELECT id_apuesta, id_porra, id_carrera, id_usuario, posiciones, posiciones_clasificacion, vrapida, estado_apuesta
            FROM apuesta
            WHERE id_porra = %s AND id_usuario = %s
            ORDER BY id_carrera ASC;
        """
        cur.execute(sql_get_bets, (id_porra, id_usuario_actual))
        my_bets = cur.fetchall()
        cur.close()

        # Formatear la respuesta
        lista_apuestas_formateada = []
        for apuesta in my_bets:
            try:
                # Parsear JSONB Carrera
                pos_data = apuesta['posiciones']
                posiciones_list = []
                if isinstance(pos_data, str): posiciones_list = json.loads(pos_data)
                elif isinstance(pos_data, list): posiciones_list = pos_data
                elif isinstance(pos_data, dict) and all(isinstance(k, int) for k in pos_data.keys()): posiciones_list = [pos_data[k] for k in sorted(pos_data.keys())]
                
                # Parsear JSONB Clasificación (NUEVO)
                pos_class_data = apuesta['posiciones_clasificacion']
                posiciones_class_list = []
                if isinstance(pos_class_data, str): posiciones_class_list = json.loads(pos_class_data)
                elif isinstance(pos_class_data, list): posiciones_class_list = pos_class_data

                apuesta_formateada = {
                    "id_apuesta": apuesta["id_apuesta"], "id_porra": apuesta["id_porra"],
                    "id_carrera": apuesta["id_carrera"], "id_usuario": apuesta["id_usuario"],
                    "posiciones": posiciones_list, 
                    "posiciones_clasificacion": posiciones_class_list, # <<< AÑADIDO
                    "vrapida": apuesta["vrapida"],
                    "estado_apuesta": apuesta["estado_apuesta"]
                }
                lista_apuestas_formateada.append(apuesta_formateada)
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                 print(f"Error formateando my-bets (ID Apuesta: {apuesta.get('id_apuesta')}): {e}")
                 continue

        return jsonify(lista_apuestas_formateada), 200

    except psycopg2.DatabaseError as db_error:
        print(f"Error DB en get_all_my_bets_in_porra: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al obtener mis apuestas"}), 500
    except Exception as error:
        print(f"Error general en get_all_my_bets_in_porra: {error}")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al obtener mis apuestas"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()


# --- NUEVO Endpoint DELETE /api/profile (Protegido) ---
# Permite al usuario autenticado eliminar su propia cuenta
# --- Endpoint DELETE /api/profile (MODIFICADO v3 - Impide borrado si es creador) ---
@app.route('/api/profile', methods=['DELETE'])
@jwt_required()
def delete_account():
    id_usuario_actual = get_jwt_identity()

    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    password = data.get('password')

    if not password or not isinstance(password, str):
        return jsonify({"error": "Falta o es inválido el campo 'password'"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- 1. Obtener datos del usuario ---
        cur.execute("SELECT password_hash, es_admin FROM usuario WHERE id_usuario = %s;", (id_usuario_actual,))
        user = cur.fetchone()

        if not user:
            cur.close()
            conn.close()
            return jsonify({"error": "Usuario no encontrado"}), 404

        # --- 2. Impedir que un admin se borre ---
        if user['es_admin']:
            cur.close()
            conn.close()
            print(f"INFO: Intento de eliminación bloqueado para admin {id_usuario_actual}.")
            return jsonify({"error": "Usuario Administrador no puede eliminar su cuenta por esta vía."}), 403

        # --- 3. Verificar la contraseña ---
        current_hash = user['password_hash']
        if not check_password_hash(current_hash, password):
            cur.close()
            conn.close()
            return jsonify({"error": "Contraseña incorrecta"}), 401

        # --- <<<< NUEVA VERIFICACIÓN CREADOR >>>> ---
        # --- 4. Comprobar si el usuario es creador de alguna porra ---
        cur.execute("SELECT 1 FROM porra WHERE id_creador = %s LIMIT 1;", (id_usuario_actual,))
        es_creador = cur.fetchone()

        if es_creador:
            # Si la consulta devuelve algo, significa que es creador
            cur.close()
            conn.close()
            print(f"INFO: Intento de eliminación bloqueado para usuario {id_usuario_actual} porque es creador.")
            # Devolver 403 Forbidden (o 409 Conflict)
            return jsonify({"error": "No se puede eliminar la cuenta porque es creador de una o más porras. Elimine sus porras primero."}), 403
        # --- <<<< FIN NUEVA VERIFICACIÓN CREADOR >>>> ---

        # --- 5. Si contraseña ok, no admin y no creador -> Eliminar ---
        print(f"INFO: Procediendo a eliminar usuario {id_usuario_actual} (no admin, no creador)...")
        sql_delete = "DELETE FROM usuario WHERE id_usuario = %s;"
        cur.execute(sql_delete, (id_usuario_actual,))
        rows_affected = cur.rowcount

        if rows_affected == 1:
             conn.commit()
             cur.close()
             print(f"INFO: Usuario {id_usuario_actual} eliminado exitosamente.")
             return jsonify({"mensaje": "Cuenta eliminada correctamente."}), 200
        else:
             conn.rollback()
             cur.close()
             print(f"WARN: El DELETE para el usuario {id_usuario_actual} no afectó filas.")
             return jsonify({"error": "No se pudo eliminar la cuenta (usuario no encontrado inesperadamente)."}), 404

    except psycopg2.Error as db_error:
        print(f"!!!! ERROR de Base de Datos en delete_account para usuario {id_usuario_actual} !!!!")
        print(f"Tipo de Error: {type(db_error)}")
        print(f"Mensaje Error DB: {db_error}")
        print(f"Código Error DB (pgcode): {db_error.pgcode}")
        print(f"Detalle Error DB (diag): {db_error.diag}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al eliminar la cuenta"}), 500
    except Exception as error:
        print(f"ERROR inesperado en delete_account para usuario {id_usuario_actual}: {error}")
        import traceback
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al eliminar la cuenta"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()
# --- FIN Endpoint MODIFICADO ---


# --- NUEVOS Endpoints para Trofeos ---

# Endpoint para obtener la lista de todos los trofeos activos
@app.route('/api/trofeos', methods=['GET'])
@jwt_required(optional=True)
def obtener_lista_trofeos():
    """
    Devuelve la lista de trofeos ya localizados según el idioma solicitado.
    Prioridad de idioma:
      1) ?lang=xx en la query
      2) language_code del usuario autenticado (si hay JWT)
      3) 'es' como fallback
    Fallback de texto:
      - Si no hay traducción en trofeo_traduccion para ese lang, usa trofeo.nombre/descripcion (español base).
    """
    conn = None
    try:
        # 1) Detectar idioma deseado
        lang_param = (request.args.get('lang') or '').strip().lower()
        user_lang = None
        try:
            # Si hay JWT, intentamos sacar el usuario y su language_code
            identity = get_jwt_identity()
            if identity:
                conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute("SELECT language_code FROM usuario WHERE id_usuario = %s;", (identity,))
                row = cur.fetchone()
                if row and row.get('language_code'):
                    user_lang = (row['language_code'] or '').strip().lower()
                cur.close()
                conn.close()
                conn = None
        except Exception:
            # si falla, seguimos sin romper
            pass

        lang = lang_param or user_lang or 'es'

        # 2) Query con LEFT JOIN a traducción + COALESCE
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = """
            SELECT
                t.id_trofeo,
                t.codigo_trofeo,
                COALESCE(tt.nombre, t.nombre) AS nombre,
                COALESCE(tt.descripcion, t.descripcion) AS descripcion,
                t.icono_url,
                t.categoria
            FROM trofeo t
            LEFT JOIN trofeo_traduccion tt
                   ON tt.id_trofeo = t.id_trofeo AND tt.lang = %s
            WHERE t.activo = TRUE
            ORDER BY t.categoria, nombre;
        """
        cur.execute(sql, (lang,))
        trofeos = cur.fetchall()
        cur.close()
        conn.close()
        conn = None

        # 3) Formato JSON
        lista = []
        for r in trofeos:
            lista.append({
                "id_trofeo": r["id_trofeo"],
                "codigo_trofeo": r["codigo_trofeo"],
                "nombre": r["nombre"],
                "descripcion": r["descripcion"],
                "icono_url": r["icono_url"],
                "categoria": r["categoria"],
                "lang": lang  # útil para depurar
            })
        return jsonify(lista), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_lista_trofeos: {error}")
        return jsonify({"error": "Error interno al obtener trofeos"}), 500
    finally:
        if conn is not None:
            conn.close()

# Endpoint para obtener los trofeos conseguidos por el usuario autenticado
@app.route('/api/profile/trofeos', methods=['GET'])
@jwt_required()
def obtener_mis_trofeos():
    """
    Devuelve los trofeos conseguidos por el usuario, localizados.
    Soporta ?lang=xx y fallback igual que /api/trofeos.
    """
    id_usuario_actual = get_jwt_identity()
    conn = None
    try:
        # 1) idioma deseado
        lang_param = (request.args.get('lang') or '').strip().lower()
        user_lang = None

        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT language_code FROM usuario WHERE id_usuario = %s;", (id_usuario_actual,))
        row = cur.fetchone()
        if row and row.get('language_code'):
            user_lang = (row['language_code'] or '').strip().lower()
        lang = lang_param or user_lang or 'es'

        # 2) Query con LEFT JOIN a traducción y COALESCE
        sql = """
            SELECT
                t.id_trofeo,
                t.codigo_trofeo,
                COALESCE(tt.nombre, t.nombre) AS nombre,
                COALESCE(tt.descripcion, t.descripcion) AS descripcion,
                t.icono_url,
                t.categoria,
                ut.fecha_conseguido,
                ut.detalles_adicionales
            FROM usuario_trofeo ut
            JOIN trofeo t ON t.id_trofeo = ut.id_trofeo
            LEFT JOIN trofeo_traduccion tt
                   ON tt.id_trofeo = t.id_trofeo AND tt.lang = %s
            WHERE ut.id_usuario = %s
            ORDER BY ut.fecha_conseguido DESC, t.categoria, nombre;
        """
        cur.execute(sql, (lang, id_usuario_actual))
        filas = cur.fetchall()
        cur.close()
        conn.close()
        conn = None

        lista_resultado = []
        for r in filas:
            item = {
                "id_trofeo": r["id_trofeo"],
                "codigo_trofeo": r["codigo_trofeo"],
                "nombre": r["nombre"],
                "descripcion": r["descripcion"],
                "icono_url": r["icono_url"],
                "categoria": r["categoria"],
                "fecha_conseguido": r["fecha_conseguido"].isoformat() if r["fecha_conseguido"] else None,
                "detalles_adicionales": r["detalles_adicionales"],
                "lang": lang
            }
            lista_resultado.append(item)

        return jsonify(lista_resultado), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_mis_trofeos: {error}")
        return jsonify({"error": "Error interno al obtener tus trofeos"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- FIN Nuevos Endpoints para Trofeos ---

# --- NUEVO Endpoint para registrar/actualizar token FCM del dispositivo ---
@app.route('/api/profile/fcm-token', methods=['POST'])
@jwt_required()
def update_fcm_token():
    id_usuario_actual = get_jwt_identity()
    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    fcm_token = data.get('fcm_token')

    if not fcm_token or not isinstance(fcm_token, str):
        return jsonify({"error": "Falta o es inválido el campo 'fcm_token'"}), 400

    conn = None
    try:
        id_usuario_actual_int = int(id_usuario_actual) # Convertir a int para la query
    except (ValueError, TypeError):
         return jsonify({"error": "Error interno de autorización (ID usuario)."}), 500

    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor() # No necesitamos DictCursor

        # Actualizar el token para el usuario actual
        # Usamos ON CONFLICT para manejar el caso de que el token ya exista para otro usuario (poco probable, pero seguro)
        # o simplemente actualizar el del usuario actual.
        # Considera si necesitas una lógica más compleja (ej: un usuario puede tener múltiples dispositivos)
        # Por ahora, asumimos un token por usuario.
        sql_update = """
            UPDATE usuario SET fcm_token = %s WHERE id_usuario = %s;
        """
        cur.execute(sql_update, (fcm_token, id_usuario_actual_int))
        conn.commit()
        cur.close()
        print(f"DEBUG [FCM Token]: Token actualizado para usuario {id_usuario_actual_int}")
        return jsonify({"mensaje": "Token FCM actualizado correctamente."}), 200

    except psycopg2.Error as db_error:
        print(f"ERROR DB [FCM Token]: Actualizando token para user {id_usuario_actual_int}. Error: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al guardar el token FCM"}), 500
    except Exception as error:
        print(f"ERROR General [FCM Token]: Actualizando token para user {id_usuario_actual_int}. Error: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al guardar el token FCM"}), 500
    finally:
        if cur is not None and not cur.closed: cur.close()
        if conn is not None and not conn.closed: conn.close()

# --- FIN Nuevo Endpoint ---

# --- NUEVO Endpoint GET para listar apuestas pendientes (Creador Porra Administrada) ---
@app.route('/api/porras/<int:id_porra>/carreras/<int:id_carrera>/apuestas/pendientes', methods=['GET'])
@jwt_required()
def listar_apuestas_pendientes(id_porra, id_carrera):
    id_usuario_actual = get_jwt_identity() # String

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Verificar que la porra existe, es administrada y el usuario es el creador
        sql_check_creator = """
            SELECT id_creador, tipo_porra FROM porra WHERE id_porra = %s;
            """
        cur.execute(sql_check_creator, (id_porra,))
        porra_info = cur.fetchone()

        if not porra_info:
            return jsonify({"error": "Porra no encontrada"}), 404

        if porra_info['tipo_porra'] != 'PRIVADA_ADMINISTRADA':
            return jsonify({"error": "Esta porra no es de tipo administrada"}), 403 # Forbidden

        try:
            id_creador_db = porra_info['id_creador']
            id_usuario_actual_int = int(id_usuario_actual)
            if id_creador_db != id_usuario_actual_int:
                return jsonify({"error": "Solo el creador puede ver las apuestas pendientes"}), 403 # Forbidden
        except (ValueError, TypeError):
             return jsonify({"error": "Error interno de autorización"}), 500

        # 2. Verificar que la carrera existe (opcional pero bueno)
        cur.execute("SELECT 1 FROM carrera WHERE id_carrera = %s;", (id_carrera,))
        if cur.fetchone() is None:
            return jsonify({"error": "Carrera no encontrada"}), 404

        # 3. Obtener apuestas pendientes para esta carrera/porra
        sql_get_pending = """
            SELECT
                a.id_apuesta,
                a.id_usuario,
                u.nombre AS nombre_usuario,
                a.fecha_creacion -- Fecha en que se realizó/modificó la apuesta
            FROM apuesta a
            JOIN usuario u ON a.id_usuario = u.id_usuario
            WHERE a.id_porra = %s AND a.id_carrera = %s AND a.estado_apuesta = 'PENDIENTE'
            ORDER BY a.fecha_creacion ASC; -- O por nombre de usuario?
            """
        cur.execute(sql_get_pending, (id_porra, id_carrera))
        pending_bets = cur.fetchall()
        cur.close()

        lista_pendientes = []
        for bet in pending_bets:
            bet_dict = dict(bet)
            if 'fecha_creacion' in bet_dict and isinstance(bet_dict['fecha_creacion'], datetime):
                bet_dict['fecha_creacion'] = bet_dict['fecha_creacion'].isoformat()
            lista_pendientes.append(bet_dict)

        return jsonify(lista_pendientes), 200 # Devuelve la lista (puede ser vacía)

    except psycopg2.Error as db_error:
        print(f"Error DB en listar_apuestas_pendientes: {db_error}")
        return jsonify({"error": "Error de base de datos"}), 500
    except Exception as error:
        print(f"Error inesperado en listar_apuestas_pendientes: {error}")
        return jsonify({"error": "Error interno al listar apuestas pendientes"}), 500
    finally:
        if conn is not None and not conn.closed:
            conn.close()
# --- FIN NUEVO Endpoint GET ---

# --- INICIO: NUEVO ENDPOINT PARA TEMPORADAS DE CLASIFICACIÓN ---
# Este endpoint obtiene la lista de temporadas disponibles para el filtro
# desde la nueva tabla f1_available_seasons que creaste.
@app.route('/api/f1/standings/seasons', methods=['GET'])
def obtener_temporadas_disponibles_f1():
    """
    Devuelve la lista de temporadas disponibles para los filtros de clasificación,
    leyendo desde la tabla 'f1_available_seasons'.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Consulta a la nueva tabla, ordenada por 'sort_order'
        sql_query = """
            SELECT season_value, season_label_key
            FROM f1_available_seasons
            ORDER BY sort_order ASC;
        """
        cur.execute(sql_query)
        
        seasons_db = cur.fetchall()
        cur.close()

        # Formatear la salida como una lista de diccionarios
        lista_seasons = []
        for row in seasons_db:
            lista_seasons.append({
                "value": row["season_value"],
                "labelKey": row["season_label_key"]
            })

        return jsonify(lista_seasons), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_temporadas_disponibles_f1: {error}")
        return jsonify({"error": "No se pudieron obtener las temporadas disponibles"}), 500
    finally:
        if conn is not None:
            conn.close()
# --- FIN: NUEVO ENDPOINT ---

# --- Endpoint POST para aceptar/rechazar apuesta (MODIFICADO para Notificación) ---
@app.route('/api/apuestas/<int:id_apuesta>/respuesta', methods=['POST'])
@jwt_required()
def responder_apuesta_pendiente(id_apuesta):
    id_usuario_actual = get_jwt_identity() # String (El creador que responde)

    if not request.is_json: return jsonify({"error": "La solicitud debe ser JSON"}), 400
    data = request.get_json()
    aceptar = data.get('aceptar') # Booleano
    if aceptar is None or not isinstance(aceptar, bool): return jsonify({"error": "Falta 'aceptar' (true/false) o inválido"}), 400

    conn = None
    cur = None 
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Obtener detalles (MODIFICADO: añadimos p.ano)
        sql_get_info = """
            SELECT
                a.id_usuario, a.id_carrera, a.estado_apuesta,
                p.id_porra, p.nombre_porra, p.id_creador, p.tipo_porra, p.ano, -- <--- AÑADIDO p.ano
                c.desc_carrera,
                u.fcm_token,
                u.language_code
            FROM apuesta a
            JOIN porra p ON a.id_porra = p.id_porra
            JOIN carrera c ON a.id_carrera = c.id_carrera
            JOIN usuario u ON a.id_usuario = u.id_usuario
            WHERE a.id_apuesta = %s;
            """
        cur.execute(sql_get_info, (id_apuesta,))
        info = cur.fetchone()

        if not info: return jsonify({"error": "Apuesta no encontrada"}), 404

        # 2. Validaciones
        if info['tipo_porra'] != 'PRIVADA_ADMINISTRADA': return jsonify({"error": "Apuesta no pertenece a porra administrada"}), 403
        try:
            id_creador_db = info['id_creador']
            id_usuario_actual_int = int(id_usuario_actual)
            if id_creador_db != id_usuario_actual_int: return jsonify({"error": "Solo el creador puede gestionar"}), 403
        except (ValueError, TypeError): return jsonify({"error": "Error interno autorización"}), 500
        if info['estado_apuesta'] != 'PENDIENTE': return jsonify({"error": "Apuesta ya no está pendiente"}), 409

        # 3. Actualizar estado
        nuevo_estado = 'ACEPTADA' if aceptar else 'RECHAZADA'
        fecha_decision = datetime.now(timezone.utc)
        sql_update_bet = "UPDATE apuesta SET estado_apuesta = %s, fecha_estado_apuesta = %s WHERE id_apuesta = %s;"
        cur.execute(sql_update_bet, (nuevo_estado, fecha_decision, id_apuesta))

        # 4. Enviar Notificación (MODIFICADO: Pasamos datos extra para navegación)
        fcm_token_apostador = info.get('fcm_token')
        id_usuario_apostador = info.get('id_usuario')
        
        if fcm_token_apostador and id_usuario_apostador:
            user_lang = (info.get('language_code') or 'es').strip().lower()
            global thread_pool_executor
            if thread_pool_executor:
                 thread_pool_executor.submit(
                     send_fcm_bet_status_notification_task,
                     id_usuario_apostador,
                     fcm_token_apostador,
                     info['desc_carrera'], # race_name
                     info['nombre_porra'], # porra_name
                     nuevo_estado,
                     info['id_porra'],     # <--- NUEVO
                     info['ano'],          # <--- NUEVO
                     info['id_creador'],   # <--- NUEVO
                     info['tipo_porra'],   # <--- NUEVO
                     user_lang
                 )

        conn.commit()
        mensaje = f"Apuesta {'aceptada' if aceptar else 'rechazada'} correctamente."
        return jsonify({"mensaje": mensaje}), 200

    except psycopg2.Error as db_error:
        print(f"Error DB en responder_apuesta_pendiente: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos"}), 500
    except Exception as error:
        print(f"Error inesperado en responder_apuesta_pendiente: {error}")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al responder a la apuesta"}), 500
    finally:
        if cur is not None and not cur.closed: cur.close()
        if conn is not None and not conn.closed: conn.close()
# --- FIN Endpoint POST Respuesta Apuesta MODIFICADO ---

# --- Endpoint PUT /api/profile/language (Protegido) ---
# Permite al usuario autenticado cambiar su preferencia de idioma
@app.route('/api/profile/language', methods=['PUT'])
@jwt_required()
def update_language():
    id_usuario_actual = get_jwt_identity()

    if not request.is_json:
        return jsonify({"error": "La solicitud debe ser JSON"}), 400

    data = request.get_json()
    language_code = data.get('language_code')

    # Validación
    if not language_code or language_code not in ['es', 'en', 'fr', 'pt', 'ca']:
        return jsonify({"error": "Falta 'language_code' o es inválido. Valores permitidos: es, en, fr, pt, ca"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()

        # Actualizar el idioma del usuario
        sql_update = "UPDATE usuario SET language_code = %s WHERE id_usuario = %s;"
        cur.execute(sql_update, (language_code, id_usuario_actual))

        conn.commit()
        cur.close()

        return jsonify({"mensaje": "Idioma actualizado correctamente."}), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en update_language: {error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al actualizar el idioma"}), 500
    finally:
        if conn is not None:
            conn.close()

# --- NUEVO Endpoint PUT /api/profile/timezone ---
@app.route('/api/profile/timezone', methods=['PUT'])
@jwt_required()
def update_timezone():
    id_usuario_actual = get_jwt_identity()
    if not request.is_json: return jsonify({"error": "JSON requerido"}), 400
    
    data = request.get_json()
    timezone_str = data.get('timezone')
    if not timezone_str: return jsonify({"error": "Falta timezone"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        # Guardamos la zona horaria
        cur.execute("UPDATE usuario SET timezone = %s WHERE id_usuario = %s;", (timezone_str, id_usuario_actual))
        conn.commit()
        cur.close()
        return jsonify({"mensaje": "Zona horaria actualizada."}), 200
    except Exception as e:
        print(f"Error update_timezone: {e}")
        return jsonify({"error": "Error interno"}), 500
    finally:
        if conn: conn.close()

# Token de seguridad (defínelo en tus variables de entorno o pon uno difícil aquí)
GPT_ACTION_SECRET = os.environ.get('GPT_ACTION_SECRET', 'F1_PORRA_SECRET_KEY_2025')

@app.route('/api/external/publish-news', methods=['POST'])
def publicar_noticia_gpt():
    # 1. Seguridad
    auth_header = request.headers.get('Authorization')
    SECRET = os.environ.get('GPT_ACTION_SECRET', 'F1_PORRA_SECRET_KEY_2025') 
    if not auth_header or auth_header != f"Bearer {SECRET}":
        return jsonify({"error": "No autorizado"}), 401

    data = request.get_json()
    final_image_url = None
    
    print("Recibida noticia del GPT...")

    # 2. GENERACIÓN DE IMAGEN (La Solución Definitiva)
    # Si el GPT nos manda una descripción visual ('image_prompt'), usamos una IA externa gratuita.
    if 'image_prompt' in data and data['image_prompt']:
        prompt = data['image_prompt']
        print(f"Generando imagen externa con prompt: {prompt}")
        
        # Codificamos el prompt para la URL
        encoded_prompt = urllib.parse.quote(prompt)
        # Usamos Pollinations.ai (Modelo Flux, alta calidad, gratis, sin API Key)
        # Forzamos 16:9 (1280x720)
        pollinations_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?width=1280&height=720&model=flux&nologo=true"
        
        # Reutilizamos tu función de descarga que ya tienes (upload_image_to_firebase)
        # Pollinations NO bloquea servidores, así que funcionará perfecto.
        final_image_url = upload_image_to_firebase(pollinations_url)

    # Fallback: Si no hay prompt pero hay URL (método antiguo), probamos
    if not final_image_url and 'imagen_url' in data:
        final_image_url = upload_image_to_firebase(data['imagen_url'])
    
    # Fallback Final: Imagen genérica
    if not final_image_url:
        final_image_url = "https://firebasestorage.googleapis.com/v0/b/f1-porra-app-links.firebasestorage.app/o/noticias%2Fgeneric.png?alt=media&token=0f3eb51a-a024-48d9-8251-eb737f1a50b5"

    # 3. Guardar en BD
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        sql = """
            INSERT INTO noticia (
                imagen_url,
                titulo_es, titulo_en, titulo_fr, titulo_pt, titulo_ca,
                cuerpo_es, cuerpo_en, cuerpo_fr, cuerpo_pt, cuerpo_ca
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            final_image_url,
            data.get('titulo_es'), data.get('titulo_en'), data.get('titulo_fr'), data.get('titulo_pt'), data.get('titulo_ca'),
            data.get('cuerpo_es'), data.get('cuerpo_en'), data.get('cuerpo_fr'), data.get('cuerpo_pt'), data.get('cuerpo_ca')
        ))
        conn.commit()
        cur.close()

        # 4. Notificar
        if thread_pool_executor:
            thread_pool_executor.submit(send_fcm_news_notification_task, data)

        return jsonify({"status": "success", "mensaje": "Noticia generada y publicada"}), 200

    except Exception as e:
        print(f"Error publicando noticia: {e}")
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# Endpoint para que la App lea las noticias
@app.route('/api/news', methods=['GET'])
@jwt_required()
def obtener_noticias():
    page = request.args.get('page', 1, type=int)
    limit = 10
    offset = (page - 1) * limit
    
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM noticia ORDER BY fecha_publicacion DESC LIMIT %s OFFSET %s", (limit, offset))
        rows = cur.fetchall()
        
        news_list = []
        for r in rows:
            item = dict(r)
            if item['fecha_publicacion']: item['fecha_publicacion'] = item['fecha_publicacion'].isoformat()
            news_list.append(item)
            
        return jsonify(news_list), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/carreras/<int:id_carrera>/fetch-clasificacion', methods=['POST'])
@jwt_required()
def fetch_clasificacion_result(id_carrera):
    """
    Descarga resultados de clasificación de Jolpica y calcula puntos.
    Requiere ser Admin.
    """
    id_usuario = get_jwt_identity()
    
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Verificar Admin
        cur.execute("SELECT es_admin FROM usuario WHERE id_usuario = %s", (id_usuario,))
        user = cur.fetchone()
        if not user or not user['es_admin']:
            return jsonify({"error": "Requiere permisos de administrador"}), 403

        # 2. Obtener datos carrera
        cur.execute("SELECT ano, desc_carrera FROM carrera WHERE id_carrera = %s", (id_carrera,))
        race = cur.fetchone()
        if not race: return jsonify({"error": "Carrera no encontrada"}), 404
        
        ano = race['ano']
        
        # --- MEJORA DE ROBUSTEZ ---
        # Calculamos la ronda basándonos en el ORDEN CRONOLÓGICO (fecha_limite_apuesta)
        # en lugar del ID, para evitar errores si las carreras se insertaron desordenadas.
        cur.execute("""
            SELECT id_carrera 
            FROM carrera 
            WHERE ano = %s 
            ORDER BY fecha_limite_apuesta ASC
        """, (ano,))
        rows = cur.fetchall()
        
        round_num = 0
        for idx, r in enumerate(rows):
            if r['id_carrera'] == id_carrera:
                round_num = idx + 1
                break
        
        if round_num == 0: 
            return jsonify({"error": "No se pudo determinar la ronda cronológica de la carrera"}), 500

        print(f"DEBUG: Fetching Qualifying for Year {ano}, Round {round_num}")

        # 3. Llamar a Jolpica (Qualifying)
        # Endpoint: {year}/{round}/qualifying.json
        path = f"{ano}/{round_num}/qualifying.json"
        try:
            jolpica_data = _jolpica_get(path)
        except JolpicaError as e:
            return jsonify({"error": f"Error Jolpica: {e}"}), 502

        # 4. Parsear respuesta Jolpica
        # Estructura: MRData -> RaceTable -> Races[0] -> QualifyingResults
        races = jolpica_data.get('MRData', {}).get('RaceTable', {}).get('Races', [])
        if not races:
            return jsonify({"error": "Jolpica no devolvió datos de carrera para esa ronda"}), 404
        
        qualy_results = races[0].get('QualifyingResults', [])
        if not qualy_results:
            return jsonify({"error": "Resultados de clasificación no disponibles aún en Jolpica"}), 404

        # Construir array de posiciones (códigos) y detalle
        posiciones_clasificacion_codes = []
        resultado_detallado_q = []

        for item in qualy_results:
            driver = item.get('Driver', {})
            code = driver.get('code') or driver.get('driverId') # Fallback
            # Normalizar código (ej: 'VER', 'HAM')
            if code:
                code = code.upper()[0:3]
                posiciones_clasificacion_codes.append(code)
            
            # Guardar tiempos
            resultado_detallado_q.append({
                "posicion": item.get('position'),
                "codigo": code,
                "q1": item.get('Q1', ''),
                "q2": item.get('Q2', ''),
                "q3": item.get('Q3', '')
            })

        if not posiciones_clasificacion_codes:
             return jsonify({"error": "Error parseando datos de pilotos"}), 500

        # 5. Guardar en BD (Tabla Carrera)
        sql_update_carrera = """
            UPDATE carrera 
            SET posiciones_clasificacion = %s::jsonb,
                resultado_detallado_clasificacion = %s::jsonb
            WHERE id_carrera = %s
        """
        cur.execute(sql_update_carrera, (
            json.dumps(posiciones_clasificacion_codes),
            json.dumps(resultado_detallado_q),
            id_carrera
        ))

        # 6. CALCULAR PUNTOS AUTOMÁTICAMENTE
        # Obtener reglas Q (cuántos eliminados en Q1/Q2)
        q_rules = _get_q_rules(ano, cur)
        
        # Obtener apuestas de clasificación ACEPTADAS para esta carrera
        cur.execute("""
            SELECT id_usuario, posiciones_clasificacion 
            FROM apuesta 
            WHERE id_carrera = %s AND estado_apuesta = 'ACEPTADA' AND posiciones_clasificacion IS NOT NULL
        """, (id_carrera,))
        apuestas = cur.fetchall()

        updates_puntuacion = 0
        for ap in apuestas:
            u_id = ap['id_usuario']
            u_pos = ap['posiciones_clasificacion'] # Es jsonb, psycopg2 lo convierte a list/dict
            
            # Calcular usando la lógica de negocio existente
            pts = _calculate_qualifying_points(u_pos, posiciones_clasificacion_codes, q_rules)
            
            # Actualizar Puntuacion (Solo columna puntos_clasificacion)
            # Usamos ON CONFLICT para crear la fila si no existe (por si el usuario no apostó a la carrera pero sí a la qualy, caso raro pero posible)
            sql_upsert_pts = """
                INSERT INTO puntuacion (id_porra, id_carrera, id_usuario, ano, puntos_clasificacion, puntos)
                SELECT id_porra, %s, %s, %s, %s, 0
                FROM apuesta WHERE id_carrera=%s AND id_usuario=%s
                ON CONFLICT (id_porra, id_carrera, id_usuario) 
                DO UPDATE SET puntos_clasificacion = EXCLUDED.puntos_clasificacion;
            """
            cur.execute(sql_upsert_pts, (id_carrera, u_id, ano, pts, id_carrera, u_id))
            updates_puntuacion += cur.rowcount

        conn.commit()
        return jsonify({
            "mensaje": "Resultados de clasificación actualizados y puntos calculados correctamente.",
            "ronda_detectada": round_num,
            "pilotos_orden": posiciones_clasificacion_codes,
            "puntuaciones_actualizadas": updates_puntuacion
        }), 200

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error fetch-clasificacion: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Excepción interna: {e}"}), 500
    finally:
        if conn: conn.close()

# ==========================================
#  INICIO: ENDPOINTS SALÓN DE LA FAMA (HALL OF FAME)
# ==========================================

@app.route('/api/hall_of_fame/champions', methods=['GET'])
@jwt_required()
def get_hall_of_fame_champions():
    """
    Obtiene los ganadores históricos de las Ligas Públicas por año.
    Devuelve: Lista de {ano, nombre_usuario, puntos_totales}
    Corregido: Usa la tabla 'puntuacion' sumando puntos + puntos_clasificacion.
    """
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Lógica corregida:
        # 1. Obtenemos la suma de puntos por usuario y año en porras PÚBLICAS.
        # 2. Usamos RANK() para ver quién quedó primero cada año.
        query = """
            WITH PuntosPorUsuario AS (
                SELECT 
                    p.ano,
                    u.nombre as nombre_usuario,
                    SUM(COALESCE(pu.puntos, 0) + COALESCE(pu.puntos_clasificacion, 0)) as puntos_totales
                FROM puntuacion pu
                JOIN porra p ON pu.id_porra = p.id_porra
                JOIN usuario u ON pu.id_usuario = u.id_usuario
                WHERE p.tipo_porra = 'PUBLICA'
                GROUP BY p.ano, u.id_usuario, u.nombre
            ),
            RankingAnual AS (
                SELECT 
                    ano,
                    nombre_usuario,
                    puntos_totales,
                    RANK() OVER (PARTITION BY ano ORDER BY puntos_totales DESC) as ranking
                FROM PuntosPorUsuario
            )
            SELECT 
                ano, 
                nombre_usuario, 
                puntos_totales 
            FROM RankingAnual
            WHERE ranking = 1
            ORDER BY ano DESC;
        """
        cursor.execute(query)
        champions = cursor.fetchall()
        
        return jsonify(champions), 200

    except Exception as e:
        print(f"Error en get_hall_of_fame_champions: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'message': 'Error al obtener campeones'}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/hall_of_fame/global_records', methods=['GET'])
@jwt_required()
def get_hall_of_fame_global_records():
    """
    Obtiene:
    1. Top 3 puntuaciones más altas en una sola carrera (Carrera + Clasificación) en ligas públicas.
    2. Lista de nombres de circuitos disponibles.
    Corregido: Elimina referencia a tabla inexistente 'participante'.
    """
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 1. Obtener Top 3 Récords Globales
        # Unimos puntuacion directamente con usuario, porra y carrera.
        query_records = """
            SELECT 
                u.nombre as nombre_usuario,
                c.ano,
                c.desc_carrera as nombre_carrera,
                (COALESCE(pt.puntos, 0) + COALESCE(pt.puntos_clasificacion, 0)) as puntos
            FROM puntuacion pt
            JOIN usuario u ON pt.id_usuario = u.id_usuario
            JOIN porra p ON pt.id_porra = p.id_porra
            JOIN carrera c ON pt.id_carrera = c.id_carrera
            WHERE p.tipo_porra = 'PUBLICA'
            ORDER BY (COALESCE(pt.puntos, 0) + COALESCE(pt.puntos_clasificacion, 0)) DESC
            LIMIT 3;
        """
        cursor.execute(query_records)
        top_3_global = cursor.fetchall()

        # 2. Obtener Lista de Circuitos
        query_circuits = """
            SELECT DISTINCT desc_carrera
            FROM carrera
            ORDER BY desc_carrera ASC;
        """
        cursor.execute(query_circuits)
        circuit_list = [item['desc_carrera'] for item in cursor.fetchall()]

        return jsonify({
            'top_3_global': top_3_global,
            'circuitos': circuit_list
        }), 200

    except Exception as e:
        print(f"Error en get_hall_of_fame_global_records: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'message': 'Error al obtener récords globales'}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/hall_of_fame/circuit_records', methods=['GET'])
@jwt_required()
def get_hall_of_fame_circuit_records():
    """
    Obtiene el Top 3 histórico para un circuito específico (Carrera + Clasificación).
    Corregido: Elimina referencia a tabla inexistente 'participante'.
    """
    race_name = request.args.get('race_name')
    
    if not race_name:
        return jsonify({'message': 'Falta el parámetro race_name'}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                u.nombre as nombre_usuario,
                c.ano,
                c.desc_carrera as nombre_carrera,
                (COALESCE(pt.puntos, 0) + COALESCE(pt.puntos_clasificacion, 0)) as puntos
            FROM puntuacion pt
            JOIN usuario u ON pt.id_usuario = u.id_usuario
            JOIN porra p ON pt.id_porra = p.id_porra
            JOIN carrera c ON pt.id_carrera = c.id_carrera
            WHERE p.tipo_porra = 'PUBLICA'
            AND c.desc_carrera = %s
            ORDER BY (COALESCE(pt.puntos, 0) + COALESCE(pt.puntos_clasificacion, 0)) DESC
            LIMIT 3;
        """
        cursor.execute(query, (race_name,))
        circuit_records = cursor.fetchall()

        return jsonify(circuit_records), 200

    except Exception as e:
        print(f"Error en get_hall_of_fame_circuit_records: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'message': 'Error al obtener récords del circuito'}), 500
    finally:
        if conn:
            conn.close()

# ==========================================
#  FIN: ENDPOINTS SALÓN DE LA FAMA
# ==========================================

# Inicializa la variable scheduler globalmente para que atexit pueda accederla
scheduler = None

# --- CONDITIONAL SCHEDULER START ---
if os.environ.get('RUN_SCHEDULER', 'false').lower() == 'true':
    print("INFO: RUN_SCHEDULER está activado. Iniciando APScheduler...")
    try:
        # Crear instancia del Scheduler de APScheduler
        # Necesitas tener ZoneInfo importado y configurado como lo tienes
        # from zoneinfo import ZoneInfo
        # o from pytz import timezone as ZoneInfo

        scheduler = BackgroundScheduler(daemon=True, timezone=str(ZoneInfo("Europe/Madrid")))

        scheduler.add_job(
            func=check_deadlines_and_notify,
            trigger=IntervalTrigger(hours=1), # O tu intervalo deseado
            id='deadline_check_job',
            name='Check race deadlines and notify users',
            replace_existing=True
        )

        scheduler.add_job(
            func=check_betting_closed_and_notify,
            trigger=IntervalTrigger(minutes=30), # O tu intervalo deseado
            id='betting_closed_check_job',
            name='Check betting closed and notify users',
            replace_existing=True
        )

        # --- Job para sincronizar clasificaciones F1 (pilotos + constructores) ---
        scheduler.add_job(
            func=sync_f1_standings_job,
            trigger=IntervalTrigger(hours=12),  # Ajusta si quieres más/menos frecuencia
            id='f1_standings_sync_job',
            name='Sync F1 standings (drivers & constructors) from Jolpica',
            replace_existing=True
        )

        print("SCHEDULER: Intentando iniciar el scheduler...")
        scheduler.start()
        print("SCHEDULER: Scheduler iniciado correctamente.")

    except Exception as e:
        print(f"!!!!!!!! SCHEDULER/EXECUTOR: ERROR CRÍTICO AL INICIAR !!!!!!!!")
        print(f"Error: {e}")
        scheduler = None # Asegurar que scheduler es None si falla el inicio
else:
    print("INFO: RUN_SCHEDULER no está activado. El APScheduler no se iniciará en esta instancia.")
# --- FIN CONDITIONAL SCHEDULER START ---


# --- Función de apagado (modificada para manejar scheduler condicional) ---
def shutdown_gracefully():
    print("SHUTDOWN: Iniciando apagado ordenado...")
    global thread_pool_executor # Acceder al executor global
    global scheduler # Acceder al scheduler global

    # 1. Apagar el pool de hilos
    if 'thread_pool_executor' in globals() and thread_pool_executor is not None:
        print("SHUTDOWN: Apagando ThreadPoolExecutor (esperando tareas)...")
        thread_pool_executor.shutdown(wait=True)
        print("SHUTDOWN: ThreadPoolExecutor apagado.")
    else:
        print("SHUTDOWN: ThreadPoolExecutor no fue inicializado o ya fue apagado.")

    # 2. Apagar el scheduler SI FUE INICIALIZADO Y ESTÁ CORRIENDO
    if scheduler is not None and scheduler.running:
        print("SHUTDOWN: Apagando Scheduler...")
        scheduler.shutdown()
        print("SHUTDOWN: Scheduler apagado.")
    else:
        print("SHUTDOWN: Scheduler no estaba iniciado o ya fue apagado.")
    print("SHUTDOWN: Apagado completado.")

# Registrar la función para que se ejecute al salir
if 'atexit' in globals(): # Comprobar si atexit fue importado
    atexit.register(shutdown_gracefully)
    print("SHUTDOWN: Función de apagado (Scheduler y ThreadPoolExecutor) registrada con atexit.")
# --- FIN Función de apagado ---


# ... (el resto de tus endpoints y la lógica de la API) ...

# if __name__ == '__main__':
#     # Considera usar use_reloader=False si el scheduler se inicia aquí y tienes problemas
#     app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000), debug=False, use_reloader=False)