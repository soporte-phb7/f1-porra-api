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
from flask_executor import Executor # <-- Añadir importación
from apscheduler.schedulers.background import BackgroundScheduler # <-- Añadir
from apscheduler.triggers.interval import IntervalTrigger       # <-- Añadir
import atexit                                                   # <-- Añadir (para apagar scheduler)
import logging                                                  # <-- Añadir (para logs del scheduler)
import concurrent.futures # <-- AÑADIR

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
        })
        # Añadimos log con el Project ID usado
        print(f"INFO: Firebase Admin SDK inicializado correctamente para proyecto '{FIREBASE_PROJECT_ID}'.")
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
                    # print(f"      TAREA DEADLINE: {len(users_needing_reminder_in_porra)} usuarios necesitan recordatorio en porra {id_porra}.")
                    # Obtener tokens FCM para estos usuarios si aún no los tenemos para esta carrera
                    user_ids_to_query_tokens = list(users_needing_reminder_in_porra - unique_users_needing_reminder_for_race.keys())
                    if user_ids_to_query_tokens:
                        placeholders = ','.join(['%s'] * len(user_ids_to_query_tokens))
                        sql_get_tokens = f"""
                            SELECT id_usuario, fcm_token, language_code
                            FROM usuario
                            WHERE id_usuario IN ({placeholders})
                              AND fcm_token IS NOT NULL AND fcm_token != '';
                        """
                        cur.execute(sql_get_tokens, tuple(user_ids_to_query_tokens))
                        tokens_found = cur.fetchall()
                        for row in tokens_found:
                            unique_users_needing_reminder_for_race[row['id_usuario']] = {
                                "token": row['fcm_token'],
                                "lang": (row['language_code'] or 'es').strip().lower()
                            }
            
            # Enviar notificaciones masivas UNA VEZ por carrera con los tokens únicos recolectados
            # Envío por usuario respetando el idioma
            pairs = [(v["token"], v["lang"]) for v in unique_users_needing_reminder_for_race.values()
                     if isinstance(v, dict) and v.get("token")]
            if pairs:
                print(f"  TAREA DEADLINE: Enviando recordatorio (i18n) para {len(pairs)} usuarios únicos para la carrera '{desc_carrera}'.")
                data_payload = {
                    'tipo_notificacion': 'deadline_reminder',
                    'race_name': desc_carrera,
                    'race_id': str(id_carrera),
                    'ano_carrera': str(ano_carrera)
                }
                for token, lang in pairs:
                    try:
                        title, body = _fcm_text('deadline_reminder', lang, race=desc_carrera, deadline=fecha_limite_str)
                        message = messaging.Message(
                            notification=messaging.Notification(title=title, body=body),
                            data=data_payload,
                            token=token
                        )
                        thread_pool_executor.submit(_send_single_reminder_task, message)
                    except Exception as err:
                        print(f"ERROR al programar envío deadline_reminder: {err}")
            else:
                print(f"  TAREA DEADLINE: No hay nuevos usuarios/tokens que notificar para la carrera '{desc_carrera}'.")


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
# Similar a send_fcm_result_notification_task
def send_fcm_bet_status_notification_task(user_id, fcm_token, race_name, porra_name, new_status, lang='es'):
    """
    Tarea en background para enviar notificación FCM sobre aceptación/rechazo de apuesta (multiidioma).
    """
    status_text = "ACEPTADA" if new_status == 'ACEPTADA' else "RECHAZADA"
    print(f"BACKGROUND TASK (Bet Status): Iniciando envío FCM para user {user_id}, apuesta {status_text} en '{race_name}'...")

    try:
        # --- INICIO: Verificación/Inicialización Firebase (Copiar bloque estándar) ---
        if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
            print(f"BACKGROUND TASK (Bet Status): Firebase no inicializado. Re-inicializando...")
            try:
                if os.path.exists(FIREBASE_CRED_PATH):
                    cred_task = credentials.Certificate(FIREBASE_CRED_PATH)
                    firebase_admin.initialize_app(
                        cred_task,
                        {'projectId': FIREBASE_PROJECT_ID},
                        name=f'firebase-task-betstatus-{user_id}-{datetime.now().timestamp()}'
                    )
                    print("BACKGROUND TASK (Bet Status): Firebase inicializado DENTRO de la tarea.")
                else:
                    print(f"BACKGROUND TASK ERROR (Bet Status): No se encontró credenciales en '{FIREBASE_CRED_PATH}'. Abortando.")
                    return
            except ValueError:
                print(f"BACKGROUND TASK INFO (Bet Status): Firebase ya inicializado por otro hilo.")
                pass
            except Exception as init_error:
                print(f"BACKGROUND TASK ERROR (Bet Status): Fallo al inicializar Firebase: {init_error}")
                return
        # --- FIN: Verificación/Inicialización Firebase ---

        if not fcm_token:
            print(f"BACKGROUND TASK (Bet Status): No hay token FCM para user {user_id}. Abortando.")
            return

        # --- Construir Mensaje Multiidioma ---
        lang = _pick_lang(lang)
        title, body = _fcm_text('bet_status_update', lang,
                                race=race_name, porra=porra_name,
                                status=('ACEPTADA' if new_status == 'ACEPTADA' else 'RECHAZADA'))

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={
                'tipo_notificacion': 'bet_status_update',
                'race_name': race_name,
                'porra_name': porra_name,
                'new_status': new_status
            },
            token=fcm_token
        )
        print(f"BACKGROUND TASK (Bet Status): Mensaje construido para token ...{fcm_token[-10:]}")

        # --- Envío del Mensaje ---
        response = messaging.send(message)
        print(f"--- BACKGROUND TASK SUCCESS (Bet Status)! MsgID: {response} ---")

    except firebase_admin.messaging.ApiCallError as fcm_api_error:
        print(f"!!!!!!!! BACKGROUND TASK FCM API ERROR (Bet Status) !!!!!!!!")
        print(f"ERROR: Código={fcm_api_error.code}, Mensaje='{fcm_api_error.message}'")
    except Exception as e:
        print(f"!!!!!!!! BACKGROUND TASK GENERAL ERROR (Bet Status) !!!!!!!!")
        import traceback; traceback.print_exc()
    finally:
        print(f"BACKGROUND TASK (Bet Status): Finalizado para user {user_id}, carrera '{race_name}'.")


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
                fecha_limite_apuesta,
                resultado_detallado, -- <<<< AÑADIDO
                -- También incluimos las columnas antiguas por si alguna parte aún las usa
                posiciones,
                vrapida
            FROM carrera
            ORDER BY ano DESC, id_carrera ASC; -- Ordenar año descendente
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
@app.route('/api/porras/<int:id_porra>/apuestas', methods=['POST'])
@jwt_required()
def registrar_o_actualizar_apuesta(id_porra):
    id_usuario_actual = get_jwt_identity() # ID String

    # --- Validaciones básicas input (sin cambios) ---
    if not request.is_json: return jsonify({"error": "La solicitud debe ser JSON"}), 400
    data = request.get_json()
    id_carrera = data.get('id_carrera')
    posiciones_input = data.get('posiciones')
    vrapida = data.get('vrapida')
    if not all([id_carrera, isinstance(posiciones_input, list), vrapida]): return jsonify({"error": "Faltan datos (id_carrera, posiciones, vrapida) o 'posiciones' no es lista"}), 400
    if not isinstance(id_carrera, int) or not isinstance(vrapida, str) or not vrapida: return jsonify({"error": "Tipos de datos inválidos"}), 400
    if not all(isinstance(p, str) and p for p in posiciones_input): return jsonify({"error": "'posiciones' debe contener strings no vacíos."}), 400

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # --- User ID Conversion (sin cambios) ---
        try: id_usuario_actual_int = int(id_usuario_actual)
        except (ValueError, TypeError): return jsonify({"error": "Error interno autorización."}), 500

        # --- Validaciones Previas ---
        # Porra existe y OBTENER TIPO PORRA? (sin cambios)
        cur.execute("SELECT ano, tipo_porra FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()
        if porra_info is None: return jsonify({"error": "Porra no encontrada"}), 404
        tipo_porra_actual = porra_info['tipo_porra']

        # Carrera existe y obtener año y fecha límite? (sin cambios)
        cur.execute("SELECT ano, fecha_limite_apuesta FROM carrera WHERE id_carrera = %s;", (id_carrera,))
        carrera_info = cur.fetchone()
        if carrera_info is None: return jsonify({"error": "Carrera no encontrada"}), 404
        ano_carrera = carrera_info['ano']
        fecha_limite_db = carrera_info['fecha_limite_apuesta']

        # Validación Pilotos Activos por Carrera (sin cambios)
        # ... (código idéntico para obtener active_drivers_for_race y expected_driver_count_for_race) ...
        # ... (validaciones de longitud y códigos de pilotos_input y vrapida contra active_drivers_for_race) ...
        active_drivers_for_race = set()
        expected_driver_count_for_race = 0
        sql_race_drivers = "SELECT codigo_piloto FROM piloto_carrera_detalle WHERE id_carrera = %s AND activo_para_apuesta = TRUE;"
        cur.execute(sql_race_drivers, (id_carrera,))
        pilotos_activos_db = cur.fetchall()
        if pilotos_activos_db: active_drivers_for_race = {p['codigo_piloto'] for p in pilotos_activos_db}; expected_driver_count_for_race = len(active_drivers_for_race)
        else:
            sql_season_drivers = "SELECT codigo_piloto FROM piloto_temporada WHERE ano = %s;"
            cur.execute(sql_season_drivers, (ano_carrera,))
            pilotos_temporada_db = cur.fetchall()
            if not pilotos_temporada_db: return jsonify({"error": f"No hay pilotos definidos para carrera {id_carrera} o temporada {ano_carrera}."}), 409
            active_drivers_for_race = {p['codigo_piloto'] for p in pilotos_temporada_db}; expected_driver_count_for_race = len(active_drivers_for_race)
        if len(posiciones_input) != expected_driver_count_for_race: return jsonify({"error": f"Número incorrecto de posiciones. Se esperaban {expected_driver_count_for_race}."}), 400
        if not all(p_code in active_drivers_for_race for p_code in posiciones_input): invalid_codes = [p for p in posiciones_input if p not in active_drivers_for_race]; return jsonify({"error": f"Códigos de piloto inválidos/inactivos en posiciones: {invalid_codes}"}), 400
        if vrapida not in active_drivers_for_race: return jsonify({"error": f"Piloto de vuelta rápida '{vrapida}' inválido/inactivo."}), 400

        # Membresía (sin cambios)
        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual_int))
        if cur.fetchone() is None: return jsonify({"error": "No eres miembro activo."}), 403

        # Fecha Límite (sin cambios)
        if fecha_limite_db is None: return jsonify({"error": "Fecha límite no definida."}), 409
        try: from zoneinfo import ZoneInfo
        except ImportError: from pytz import timezone as ZoneInfo
        try: tz_madrid = ZoneInfo("Europe/Madrid")
        except Exception: tz_madrid = timezone.utc
        now_local = datetime.now(tz_madrid)
        # Asegurar que ambas fechas tienen timezone para comparar
        if fecha_limite_db.tzinfo is None: fecha_limite_db = fecha_limite_db.replace(tzinfo=timezone.utc)
        if now_local.tzinfo is None: now_local = now_local.replace(tzinfo=timezone.utc)
        if now_local.astimezone(timezone.utc) > fecha_limite_db.astimezone(timezone.utc): return jsonify({"error": "Fecha límite pasada."}), 409

        # --- >>> NUEVA Lógica de Estado Apuesta <<< ---
        estado_final_apuesta = 'PENDIENTE' # Valor por defecto para administrada
        fecha_estado_final = None # Fecha aceptación/rechazo (se pone al responder)
        now_utc_db = datetime.now(timezone.utc) # Momento actual para fecha_modificacion

        # 1. Comprobar si existe apuesta previa para este usuario/carrera/porra
        cur.execute("SELECT estado_apuesta FROM apuesta WHERE id_porra = %s AND id_carrera = %s AND id_usuario = %s;",
                    (id_porra, id_carrera, id_usuario_actual_int))
        apuesta_previa = cur.fetchone()

        # 2. Determinar estado final
        if tipo_porra_actual in ['PUBLICA', 'PRIVADA_AMISTOSA']:
            estado_final_apuesta = 'ACEPTADA'
            fecha_estado_final = now_utc_db # Se acepta automáticamente
        elif tipo_porra_actual == 'PRIVADA_ADMINISTRADA':
            if apuesta_previa:
                # Si había apuesta previa en porra administrada...
                estado_previo = apuesta_previa['estado_apuesta']
                if estado_previo == 'ACEPTADA':
                    # Si estaba ACEPTADA, la modificación la MANTIENE ACEPTADA
                    estado_final_apuesta = 'ACEPTADA'
                    fecha_estado_final = now_utc_db # Actualizamos fecha de estado (o podríamos mantener la original?)
                                                  # -> Actualizarla parece más lógico para indicar que se tocó
                elif estado_previo == 'RECHAZADA':
                    # Si estaba RECHAZADA, la modificación la vuelve a poner PENDIENTE
                    estado_final_apuesta = 'PENDIENTE'
                    fecha_estado_final = None # El creador debe volver a decidir
                else: # PENDIENTE (o estado inesperado)
                    # Si estaba PENDIENTE, sigue PENDIENTE
                    estado_final_apuesta = 'PENDIENTE'
                    fecha_estado_final = None
            else:
                # Si no había apuesta previa (es la primera vez), queda PENDIENTE
                estado_final_apuesta = 'PENDIENTE'
                fecha_estado_final = None
        # --- >>> FIN NUEVA Lógica de Estado Apuesta <<< ---


        # Lógica Trofeo Primera Apuesta (sin cambios)
        should_award_first_bet_trophy = False
        cur.execute("SELECT COUNT(*) FROM apuesta WHERE id_usuario = %s;", (id_usuario_actual_int,))
        if cur.fetchone()[0] == 0 and not apuesta_previa : # Solo si realmente no tenía NINGUNA apuesta antes
            should_award_first_bet_trophy = True


        # --- Modificaciones BD (Upsert con manejo de estado) ---
        sql_upsert = """
            INSERT INTO apuesta (id_porra, id_carrera, id_usuario, posiciones, vrapida, estado_apuesta, fecha_estado_apuesta, fecha_modificacion)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            ON CONFLICT (id_porra, id_carrera, id_usuario) DO UPDATE SET
                posiciones = EXCLUDED.posiciones,
                vrapida = EXCLUDED.vrapida,
                estado_apuesta = EXCLUDED.estado_apuesta,
                fecha_estado_apuesta = EXCLUDED.fecha_estado_apuesta,
                fecha_modificacion = EXCLUDED.fecha_modificacion;
            """
        posiciones_json = json.dumps(posiciones_input)
        valores = (
            id_porra, id_carrera, id_usuario_actual_int,
            posiciones_json, vrapida,
            estado_final_apuesta,       # Estado calculado
            fecha_estado_final,         # Fecha estado calculada
            now_utc_db                  # Fecha modificación SIEMPRE se actualiza
        )
        cur.execute(sql_upsert, valores)

        # Otorgar Trofeo (sin cambios)
        if should_award_first_bet_trophy:
            # Usamos el estado final para decidir si el trofeo se otorga YA
            # En porras administradas, solo se otorga si la apuesta inicial ya fue aceptada
            # -> Mejor lo asociamos al evento de ACEPTACIÓN si es administrada.
            # -> Por ahora, mantenemos la lógica original: se otorga al primer registro exitoso
            #    independientemente del estado final, para simplificar.
             if not _award_trophy(id_usuario_actual_int, 'PRIMERA_APUESTA', conn, cur):
                print(f"WARN: _award_trophy (PRIMERA_APUESTA) retornó False.")

        conn.commit()
        mensaje_respuesta = "Apuesta registrada/actualizada correctamente."
        if tipo_porra_actual == 'PRIVADA_ADMINISTRADA' and estado_final_apuesta == 'PENDIENTE':
            mensaje_respuesta += " Pendiente de aprobación por el creador."

        return jsonify({"mensaje": mensaje_respuesta}), 201

    except psycopg2.Error as db_error: # Manejo de errores sin cambios
        print(f"ERROR DB [Registrar Apuesta]: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al registrar apuesta"}), 500
    except Exception as error: # Manejo de errores sin cambios
        print(f"ERROR General [Registrar Apuesta]: {error}")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al registrar apuesta"}), 500
    finally: # Sin cambios
        if cur is not None and not cur.closed: cur.close()
        if conn is not None and not conn.closed: conn.close()
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
    id_usuario_actual = get_jwt_identity() # String
    try:
        id_usuario_actual_int = int(id_usuario_actual)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Verificar membresía (sin cambios)
        sql_check_membership = "SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s AND estado IN ('CREADOR', 'ACEPTADA');"
        cur.execute(sql_check_membership, (id_porra, id_usuario_actual_int))
        if cur.fetchone() is None: return jsonify({"error": "No eres miembro activo."}), 403

        # --- MODIFICADO: Obtener estado_apuesta ---
        sql_get_bet = """
            SELECT id_apuesta, id_porra, id_carrera, id_usuario, posiciones, vrapida, estado_apuesta
            FROM apuesta
            WHERE id_porra = %s AND id_carrera = %s AND id_usuario = %s;
        """
        cur.execute(sql_get_bet, (id_porra, id_carrera, id_usuario_actual_int))
        apuesta = cur.fetchone()
        cur.close()

        if apuesta:
            try:
                # Parsear JSONB (sin cambios)
                pos_data = apuesta['posiciones']
                posiciones_list = []
                if isinstance(pos_data, str): posiciones_list = json.loads(pos_data)
                elif isinstance(pos_data, list): posiciones_list = pos_data
                elif isinstance(pos_data, dict) and all(isinstance(k, int) for k in pos_data.keys()): posiciones_list = [pos_data[k] for k in sorted(pos_data.keys())]
                else: raise TypeError("Tipo inesperado para 'posiciones'")

                resultado_json = {
                    "id_apuesta": apuesta["id_apuesta"], "id_porra": apuesta["id_porra"],
                    "id_carrera": apuesta["id_carrera"], "id_usuario": apuesta["id_usuario"],
                    "posiciones": posiciones_list, "vrapida": apuesta["vrapida"],
                    "estado_apuesta": apuesta["estado_apuesta"] # <<< AÑADIDO
                }
                return jsonify(resultado_json), 200
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                 print(f"Error procesando apuesta {apuesta.get('id_apuesta')}: {e}")
                 return jsonify({"error": "Error procesando datos de apuesta recuperados"}), 500
        else:
            return jsonify({"error": "No se encontró apuesta para esta carrera/porra"}), 404

    except psycopg2.DatabaseError as db_error: # Sin cambios manejo error
        print(f"Error DB en obtener_mi_apuesta: {db_error}")
        return jsonify({"error": "Error de base de datos al obtener la apuesta"}), 500
    except Exception as error: # Sin cambios manejo error
        print(f"Error general en obtener_mi_apuesta: {error}")
        import traceback; traceback.print_exc()
        return jsonify({"error": "Error interno al obtener la apuesta"}), 500
    finally: # Sin cambios finally
        if conn is not None and not conn.closed:
            conn.close()

# --- Endpoint GET /api/porras/.../apuestas/todas (MODIFICADO v2 con estado_apuesta) ---
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
        # ... (código de validaciones existente) ...
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
        if now_local.astimezone(timezone.utc) <= fecha_limite_db.astimezone(timezone.utc): 
            return jsonify({ "error": "No se pueden ver apuestas hasta después de la fecha límite.", "fecha_limite": fecha_limite_db.isoformat(), }), 403
        # --- Fin Validaciones ---

        # --- Obtener TODAS las apuestas (Añadir estado_apuesta) ---
        sql_get_all_bets = """
            SELECT
                u.id_usuario,
                u.nombre AS nombre_usuario,
                a.id_apuesta,
                a.posiciones,
                a.vrapida,
                a.estado_apuesta -- <<< AÑADIDO
            FROM apuesta a
            JOIN usuario u ON a.id_usuario = u.id_usuario
            WHERE a.id_porra = %s AND a.id_carrera = %s
            ORDER BY u.nombre ASC;
            """
        cur.execute(sql_get_all_bets, (id_porra, id_carrera))
        todas_apuestas = cur.fetchall()
        cur.close()

        # --- Formatear la respuesta (incluir estado_apuesta) ---
        lista_apuestas_formateada = []
        for apuesta in todas_apuestas:
            try:
                pos_data = apuesta['posiciones']
                posiciones_list = [] # Parsear JSONB (código existente)
                if isinstance(pos_data, str): posiciones_list = json.loads(pos_data)
                elif isinstance(pos_data, list): posiciones_list = pos_data
                elif isinstance(pos_data, dict) and all(isinstance(k, int) for k in pos_data.keys()): posiciones_list = [pos_data[k] for k in sorted(pos_data.keys())]
                else: raise TypeError("Tipo inesperado para 'posiciones'")

                apuesta_formateada = {
                    "id_apuesta": apuesta["id_apuesta"],
                    "id_usuario": apuesta["id_usuario"],
                    "nombre_usuario": apuesta["nombre_usuario"],
                    "posiciones": posiciones_list,
                    "vrapida": apuesta["vrapida"],
                    "estado_apuesta": apuesta["estado_apuesta"] # <<< AÑADIDO
                }
                lista_apuestas_formateada.append(apuesta_formateada)
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                 print(f"Error formateando apuesta TODAS (ID Apuesta: {apuesta.get('id_apuesta')}): {e}")
                 continue

        return jsonify(lista_apuestas_formateada), 200

    except psycopg2.DatabaseError as db_error: # Sin cambios manejo error
        print(f"Error DB en obtener_todas_apuestas_carrera: {db_error}")
        return jsonify({"error": "Error de base de datos al obtener apuestas"}), 500
    except Exception as error: # Sin cambios manejo error
        print(f"Error general en obtener_todas_apuestas_carrera: {error}")
        import traceback; traceback.print_exc()
        return jsonify({"error": "Error interno al obtener todas las apuestas"}), 500
    finally: # Sin cambios finally
        if conn is not None and not conn.closed:
            conn.close()

# --- Endpoint GET /api/carreras/<id_carrera>/resultado (MODIFICADO v2 para usar piloto_carrera_detalle) ---
@app.route('/api/carreras/<int:id_carrera>/resultado', methods=['GET'])
def obtener_resultado_carrera(id_carrera):
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Obtener resultado detallado JSONB y AÑO de la carrera
        sql_race = "SELECT ano, resultado_detallado FROM carrera WHERE id_carrera = %s;"
        cur.execute(sql_race, (id_carrera,))
        resultado_db = cur.fetchone()

        if not resultado_db:
            cur.close(); conn.close()
            return jsonify({"error": "Carrera no encontrada"}), 404

        ano_carrera = resultado_db.get("ano")
        resultado_detallado_json = resultado_db.get("resultado_detallado") # Puede ser None o un Dict Python

        # 2. Verificar si los resultados existen (JSONB no es NULL y tiene contenido)
        if not resultado_detallado_json or not isinstance(resultado_detallado_json, dict) or not ano_carrera:
            # La carrera existe pero aún no tiene resultados válidos
            cur.close(); conn.close()
            return jsonify({ "id_carrera": id_carrera, "status": "pendiente" }), 200 # 200 OK

        # --- Inicio Lógica Modificada para Obtener Detalles de Pilotos ---
        pilotos_map = {} # Mapa para almacenar detalles: codigo -> {nombre, escuderia, color}

        # 3. PRIMERO: Intentar obtener detalles desde piloto_carrera_detalle para esta carrera específica
        sql_pilotos_carrera = """
            SELECT codigo_piloto, nombre_completo_carrera, escuderia_carrera, color_fondo_hex_carrera
            FROM piloto_carrera_detalle
            WHERE id_carrera = %s;
        """
        cur.execute(sql_pilotos_carrera, (id_carrera,))
        pilotos_info_carrera_db = cur.fetchall()

        if pilotos_info_carrera_db:
            print(f"DEBUG [GetResult]: Encontrados {len(pilotos_info_carrera_db)} pilotos en piloto_carrera_detalle para carrera {id_carrera}.")
            for p in pilotos_info_carrera_db:
                pilotos_map[p['codigo_piloto']] = {
                    'nombre_completo': p.get('nombre_completo_carrera', p['codigo_piloto']),
                    'escuderia': p.get('escuderia_carrera', ''),
                    'color_escuderia_hex': p.get('color_fondo_hex_carrera', '#CCCCCC')
                }
        else:
             # Si no hay NADA en piloto_carrera_detalle para esta carrera (no debería pasar si PUT /resultado funciona)
             # usamos piloto_temporada como fallback completo.
            print(f"WARN [GetResult]: No se encontraron pilotos en piloto_carrera_detalle para carrera {id_carrera}. Usando piloto_temporada para {ano_carrera} como fallback.")
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

        # 4. SEGUNDO: Para pilotos en el resultado que *pudieran* no estar en el mapa (fallback individual)
        # Esto es una salvaguarda extra por si piloto_carrera_detalle no se pobló correctamente
        # o si el resultado_detallado_json contiene un piloto inesperado.
        codigos_en_resultado = set()
        if 'posiciones_detalle' in resultado_detallado_json and isinstance(resultado_detallado_json['posiciones_detalle'], list):
            for piloto_res in resultado_detallado_json['posiciones_detalle']:
                if isinstance(piloto_res, dict) and 'codigo' in piloto_res:
                    codigos_en_resultado.add(piloto_res['codigo'])
        if 'vrapida_piloto' in resultado_detallado_json:
             codigos_en_resultado.add(resultado_detallado_json['vrapida_piloto'])

        pilotos_faltantes = codigos_en_resultado - set(pilotos_map.keys())

        if pilotos_faltantes:
            print(f"WARN [GetResult]: Los siguientes códigos de piloto del resultado no se encontraron inicialmente: {pilotos_faltantes}. Buscando en piloto_temporada...")
            placeholders = ','.join(['%s'] * len(pilotos_faltantes))
            sql_fallback_pilotos = f"""
                SELECT codigo_piloto, nombre_completo, escuderia, color_fondo_hex
                FROM piloto_temporada WHERE ano = %s AND codigo_piloto IN ({placeholders});
            """
            cur.execute(sql_fallback_pilotos, (ano_carrera, *list(pilotos_faltantes)))
            pilotos_fallback_db = cur.fetchall()
            for p in pilotos_fallback_db:
                pilotos_map[p['codigo_piloto']] = {
                    'nombre_completo': p.get('nombre_completo', p['codigo_piloto']),
                    'escuderia': p.get('escuderia', ''),
                    'color_escuderia_hex': p.get('color_fondo_hex', '#CCCCCC')
                }
        # --- Fin Lógica Modificada Obtener Detalles Pilotos ---

        # 5. Procesar el JSON almacenado y ENRIQUECER con detalles del mapa
        try:
            posiciones_detalle_db = resultado_detallado_json.get('posiciones_detalle')
            vrapida_piloto_db = resultado_detallado_json.get('vrapida_piloto')
            vrapida_tiempo_db = resultado_detallado_json.get('vrapida_tiempo') # Se mantiene igual

            if not isinstance(posiciones_detalle_db, list) or not posiciones_detalle_db or \
               not isinstance(vrapida_piloto_db, str) or not vrapida_piloto_db or \
               vrapida_tiempo_db is None: # Tiempo VR puede ser string vacío
                raise ValueError("JSON de resultado almacenado tiene formato inválido o faltan claves.")

            # 6. Construir la lista de resultados detallada ENRIQUECIDA
            resultado_final_posiciones = []
            for piloto_res_db in posiciones_detalle_db:
                if not isinstance(piloto_res_db, dict): continue
                codigo = piloto_res_db.get('codigo')
                if not codigo: continue

                # Obtener detalles del mapa (que prioriza piloto_carrera_detalle)
                piloto_detalle_final = pilotos_map.get(codigo, {
                    'nombre_completo': codigo, 'escuderia': '?', 'color_escuderia_hex': '#CCCCCC' # Fallback final
                })

                resultado_final_posiciones.append({
                    "posicion": piloto_res_db.get('posicion'),
                    "codigo": codigo,
                    "tiempo_str": piloto_res_db.get('tiempo_str'),
                    "nombre_completo": piloto_detalle_final['nombre_completo'],
                    "escuderia": piloto_detalle_final['escuderia'],
                    "color_escuderia_hex": piloto_detalle_final['color_escuderia_hex']
                })

            # 7. Construir detalles del piloto de la vuelta rápida ENRIQUECIDO
            vrapida_piloto_detalle_final = pilotos_map.get(vrapida_piloto_db, {
                'nombre_completo': vrapida_piloto_db, 'escuderia': '?', 'color_escuderia_hex': '#CCCCCC' # Fallback final
            })
            vrapida_info_final = {
                "codigo": vrapida_piloto_db,
                "tiempo_vr": vrapida_tiempo_db,
                "nombre_completo": vrapida_piloto_detalle_final['nombre_completo'],
                "escuderia": vrapida_piloto_detalle_final['escuderia'],
                "color_escuderia_hex": vrapida_piloto_detalle_final['color_escuderia_hex']
            }

            # 8. Crear respuesta final combinada
            resultado_json_respuesta = {
                "id_carrera": id_carrera,
                "status": "finalizada",
                "posiciones_detalle": resultado_final_posiciones,
                "vrapida_detalle": vrapida_info_final
            }
            cur.close(); conn.close()
            return jsonify(resultado_json_respuesta), 200 # 200 OK

        except (ValueError, TypeError, KeyError) as e:
             print(f"Error procesando resultado JSONB/Pilotos para carrera {id_carrera}: {e}")
             cur.close(); conn.close()
             # Devolver estado pendiente si los datos están corruptos o incompletos
             return jsonify({ "id_carrera": id_carrera, "status": "pendiente", "error_detalle": f"Datos de resultado almacenados corruptos o incompletos: {e}" }), 200 # 200 OK

    except psycopg2.DatabaseError as db_error:
        print(f"Error DB en obtener_resultado_carrera: {db_error}")
        if conn: conn.close()
        return jsonify({"error": "Error de base de datos al obtener el resultado"}), 500
    except Exception as error:
        print(f"Error general en obtener_resultado_carrera: {error}")
        import traceback
        traceback.print_exc()
        if conn: conn.close()
        return jsonify({"error": "Error interno al obtener el resultado de la carrera"}), 500
# --- FIN Endpoint GET Resultado MODIFICADO ---

# --- Endpoint PUT /api/carreras/<id_carrera>/resultado (MODIFICADO v9 - Condición 5+ miembros para trofeos GP) ---
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
           not isinstance(vrapida_tiempo_input, str): # vrapida_tiempo puede ser string vacío
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

    conn = None # Declarar conn aquí para usarlo en finally
    total_puntuaciones_calculadas = 0
    # Mapas trofeos
    map_carrera_trofeo = { 'Australia': 'GANA_AUSTRALIA', 'China': 'GANA_CHINA', 'Japan': 'GANA_JAPON', 'Bahrein': 'GANA_BAREIN', 'Saudi Arabia': 'GANA_ARABIA_SAUDI', 'Miami': 'GANA_MIAMI', 'Emilia-Romagna': 'GANA_EMILIA_ROMANA', 'Monaco': 'GANA_MONACO', 'Spain': 'GANA_ESPANA', 'Canada': 'GANA_CANADA', 'Austria': 'GANA_AUSTRIA', 'Great Bretain': 'GANA_GRAN_BRETANA', 'Belgium': 'GANA_BELGICA', 'Hungary': 'GANA_HUNGRIA', 'Netherlands': 'GANA_PAISES_BAJOS', 'Italy': 'GANA_ITALIA', 'Azerbaijan': 'GANA_AZERBAYAN', 'Singapore': 'GANA_SINGAPUR', 'United States': 'GANA_ESTADOS_UNIDOS', 'Mexico': 'GANA_MEXICO', 'Brazil': 'GANA_BRASIL', 'Las Vegas': 'GANA_LAS_VEGAS', 'Qatar': 'GANA_CATAR', 'Abu Dhabi': 'GANA_ABU_DABI' }
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
        desc_carrera = carrera_info_row['desc_carrera'] # Nombre de la carrera actual

        # Validación contra Pilotos Activos
        active_drivers_for_race = set(); expected_driver_count_for_race = 0
        sql_active_race_drivers = "SELECT codigo_piloto FROM piloto_carrera_detalle WHERE id_carrera = %s AND activo_para_apuesta = TRUE;"
        cur.execute(sql_active_race_drivers, (id_carrera,))
        pilotos_activos_db = cur.fetchall()
        if pilotos_activos_db:
            active_drivers_for_race = {p['codigo_piloto'] for p in pilotos_activos_db}; expected_driver_count_for_race = len(active_drivers_for_race)
        else: # Fallback a temporada
            sql_season_drivers = "SELECT codigo_piloto FROM piloto_temporada WHERE ano = %s;"
            cur.execute(sql_season_drivers, (ano_carrera,))
            pilotos_temporada_db = cur.fetchall()
            if not pilotos_temporada_db: cur.close(); conn.close(); return jsonify({"error": f"Config error: No hay pilotos definidos para temporada {ano_carrera}."}), 409
            active_drivers_for_race = {p['codigo_piloto'] for p in pilotos_temporada_db}; expected_driver_count_for_race = len(active_drivers_for_race)

        # Validar resultado recibido contra pilotos activos
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
        print(f"DEBUG: Poblando piloto_carrera_detalle para carrera {id_carrera}...")
        # Obtener detalles de piloto_temporada para poblar
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
        pilotos_procesados = set() # Para evitar duplicar si el de VR ya estaba en posiciones

        # Iterar sobre los pilotos DEL RESULTADO para crear/actualizar detalles
        for piloto_res in posiciones_detalle_input:
            codigo = piloto_res.get('codigo')
            if codigo and codigo in piloto_details_map_season:
                details = piloto_details_map_season[codigo]
                # Insertamos marcando activo_para_apuesta = FALSE porque la carrera ya ha ocurrido
                valores_pilotos_detalle.append((
                    id_carrera, codigo,
                    details.get('nombre_completo', codigo), details.get('escuderia', ''),
                    details.get('color_fondo_hex', '#CCCCCC'), details.get('color_texto_hex', '#000000'),
                    False # <-- Marcar como inactivo para futuras apuestas
                ))
                pilotos_procesados.add(codigo)

        # Asegurarnos de incluir al piloto de la VR si no estaba ya
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
            print(f"DEBUG: Upserted {cur.rowcount} registros en piloto_carrera_detalle.")
        else:
            print(f"WARN: No se generaron valores para piloto_carrera_detalle.")

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
            print(f"DEBUG: Próxima carrera encontrada: ID={next_race_id}, Nombre='{next_race_name}'")
        else:
            print(f"DEBUG: No se encontró próxima carrera para año {ano_carrera} después de ID {id_carrera}.")

        # --- 4. Bucle Principal Cálculo Puntuaciones, Trofeos, Notificaciones ---
        resultado_para_calculo = {"posiciones": posiciones_resultado_codigos, "vrapida": vrapida_piloto_input}
        # Obtener porras del año, incluyendo TIPO PORRA y MEMBER_COUNT
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
            return jsonify({ "mensaje": f"Resultado y detalles carrera {id_carrera} actualizados. No se encontraron porras activas.", "puntuaciones_calculadas": 0 }), 200

        posiciones_resultado_map = {piloto: index for index, piloto in enumerate(posiciones_resultado_codigos)}
        users_notified_about_result = set()
        users_notified_about_next_race = set()

        for porra_row in porras_del_ano:
            id_porra_actual = porra_row['id_porra']
            tipo_porra_actual = porra_row['tipo_porra']
            member_count_porra = porra_row['member_count'] # <-- Contiene el número de miembros activos
            print(f"\nDEBUG: Procesando porra {id_porra_actual} (Tipo: {tipo_porra_actual}, Miembros: {member_count_porra}) para carrera {id_carrera}...")

            # --- Obtener SOLO apuestas ACEPTADAS ---
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
                     else: raise TypeError("Tipo inesperado apuesta['posiciones']")

                     if len(apuesta_pos_list) == expected_driver_count_for_race:
                         apuesta_dict = { 'id_usuario': apuesta_raw['id_usuario'], 'posiciones': apuesta_pos_list, 'vrapida': apuesta_raw['vrapida'] }
                         lista_apuestas_para_calculo.append(apuesta_dict)
                         map_apuestas_usuario[apuesta_raw['id_usuario']] = apuesta_dict
                     else: print(f"WARN [PUT Result]: Apuesta aceptada user {apuesta_raw['id_usuario']} en porra {id_porra_actual} omitida (longitud {len(apuesta_pos_list)} != {expected_driver_count_for_race}).")
                 except (json.JSONDecodeError, TypeError, KeyError) as e_parse: print(f"Error procesando apuesta aceptada user {apuesta_raw.get('id_usuario')} en porra {id_porra_actual}: {e_parse}")

            puntuaciones_calculadas = []
            if lista_apuestas_para_calculo:
                puntuaciones_calculadas = calcular_puntuaciones_api(resultado_para_calculo, lista_apuestas_para_calculo)
            print(f"DEBUG: Puntuaciones calculadas porra {id_porra_actual}: {len(puntuaciones_calculadas)} regs.")

            sql_delete_puntuaciones = "DELETE FROM puntuacion WHERE id_porra = %s AND id_carrera = %s;"
            cur.execute(sql_delete_puntuaciones, (id_porra_actual, id_carrera))

            participants_to_notify_result = set()
            participants_to_notify_next_race = set()

            if puntuaciones_calculadas:
                 puntuaciones_calculadas.sort(key=lambda x: x.get('puntos', 0), reverse=True)
                 valores_insert_puntuaciones = []
                 current_rank = 0; last_score = -1; rank_counter = 0
                 map_rank_usuario = {}

                 for p in puntuaciones_calculadas:
                     rank_counter += 1
                     user_id = p['id_usuario']
                     if p['puntos'] != last_score: current_rank = rank_counter; last_score = p['puntos']
                     valores_insert_puntuaciones.append((id_porra_actual, id_carrera, user_id, p['puntos'], ano_carrera))
                     map_rank_usuario[user_id] = current_rank
                     if user_id not in users_notified_about_result: participants_to_notify_result.add(user_id)
                     if next_race_id is not None and user_id not in users_notified_about_next_race: participants_to_notify_next_race.add(user_id)

                 sql_insert_puntuacion = "INSERT INTO puntuacion (id_porra, id_carrera, id_usuario, puntos, ano) VALUES (%s, %s, %s, %s, %s);"
                 cur.executemany(sql_insert_puntuacion, valores_insert_puntuaciones)
                 num_insertadas = cur.rowcount; total_puntuaciones_calculadas += num_insertadas
                 print(f"DEBUG: Puntuaciones nuevas insertadas: {num_insertadas} filas")

                 detalles_trofeo = {"ano": ano_carrera, "id_porra": id_porra_actual, "id_carrera": id_carrera}
                 trofeo_carrera_especifico = map_carrera_trofeo.get(desc_carrera)

                 for user_id, rank in map_rank_usuario.items():
                     if rank == 1: # Usuario ha ganado la carrera (o empatado en primer puesto)
                         # Trofeo por ganar CUALQUIER carrera (si hay >= 5 miembros)
                         if member_count_porra >= 5: # <--- CONDICIÓN YA EXISTENTE Y CORRECTA
                             _award_trophy(user_id, 'GANA_CARRERA_CUALQUIERA', conn, cur, detalles=detalles_trofeo)
                         
                         # Trofeo específico de la carrera (ej: GANA_AUSTRALIA)
                         # ----> INICIO DE LA CORRECCIÓN <----
                         if trofeo_carrera_especifico and member_count_porra >= 5: # <--- AÑADIR CONDICIÓN DE MIEMBROS
                         # ----> FIN DE LA CORRECCIÓN <----
                             _award_trophy(user_id, trofeo_carrera_especifico, conn, cur, detalles=detalles_trofeo)
                         
                         # Trofeo por ganar en porra pública (NO requiere 5+ miembros según descripción)
                         if tipo_porra_actual == 'PUBLICA':
                             _award_trophy(user_id, 'GANA_CARRERA_PUBLICA', conn, cur, detalles=detalles_trofeo)

                     # Trofeos por acertar piloto (si hay >= 5 miembros)
                     if member_count_porra >= 5: # <--- CONDICIÓN YA EXISTENTE Y CORRECTA PARA ESTE BLOQUE
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
                 print(f"DEBUG: No se calcularon/insertaron puntuaciones para porra {id_porra_actual}. Obteniendo participantes para notificación.")
                 cur.execute("SELECT id_usuario FROM participacion WHERE id_porra = %s AND estado IN ('CREADOR', 'ACEPTADA');", (id_porra_actual,))
                 all_participants = cur.fetchall()
                 for participant in all_participants:
                     user_id = participant['id_usuario']
                     if user_id not in users_notified_about_result: participants_to_notify_result.add(user_id)
                     if next_race_id is not None and user_id not in users_notified_about_next_race: participants_to_notify_next_race.add(user_id)

            # --- Notificación Resultado Listo ---
            if participants_to_notify_result:
                 user_ids_to_notify_list_res = list(participants_to_notify_result)
                 placeholders_res = ','.join(['%s'] * len(user_ids_to_notify_list_res))
                 sql_get_tokens_res = f"""
                    SELECT id_usuario, fcm_token, language_code
                    FROM usuario
                    WHERE id_usuario IN ({placeholders_res})
                      AND fcm_token IS NOT NULL AND fcm_token != '';
                """
                 cur.execute(sql_get_tokens_res, tuple(user_ids_to_notify_list_res))
                 tokens_to_send_res = cur.fetchall()
                 if tokens_to_send_res:
                    global thread_pool_executor
                    if thread_pool_executor is None: print("!!!!!!!! ERROR CRÍTICO [Notif Resultado]: ThreadPoolExecutor no inicializado !!!!!!!!!!")
                    else:
                        submitted_count_res = 0
                        for token_row in tokens_to_send_res:
                            user_id = token_row['id_usuario']; token = token_row['fcm_token']
                            user_lang = (token_row.get('language_code') or 'es').strip().lower()
                            try:
                                thread_pool_executor.submit(send_fcm_result_notification_task, user_id, token, desc_carrera, id_porra_actual, user_lang)
                                users_notified_about_result.add(user_id); submitted_count_res += 1
                            except Exception as submit_err: print(f"!!!!!!!! ERROR [Notif Resultado]: Fallo al hacer submit para user {user_id}. Error: {submit_err} !!!!!!!!!!")
                        print(f"DEBUG: Enviadas {submitted_count_res} tareas de notificación de resultado al executor para porra {id_porra_actual}.")
                 else: print(f"DEBUG: No se encontraron tokens FCM válidos para notificar resultado en porra {id_porra_actual}.")

            # --- Notificación Próxima Carrera Disponible ---
            if next_race_id is not None and participants_to_notify_next_race:
                print(f"DEBUG: Preparando notificación 'Next Race Available' para carrera '{next_race_name}' (ID: {next_race_id}).")
                user_ids_to_notify_list_next = list(participants_to_notify_next_race)
                placeholders_next = ','.join(['%s'] * len(user_ids_to_notify_list_next))
                sql_get_tokens_next = f"""
                    SELECT id_usuario, fcm_token, language_code
                    FROM usuario
                    WHERE id_usuario IN ({placeholders_next})
                      AND fcm_token IS NOT NULL AND fcm_token != '';
                """
                cur.execute(sql_get_tokens_next, tuple(user_ids_to_notify_list_next))
                tokens_to_send_next = cur.fetchall()
                if tokens_to_send_next:
                    if thread_pool_executor is None: print("!!!!!!!! ERROR CRÍTICO [Notif Next Race]: ThreadPoolExecutor no inicializado !!!!!!!!!!")
                    else:
                        submitted_count_next = 0
                        for token_row in tokens_to_send_next:
                            user_id = token_row['id_usuario']; token = token_row['fcm_token']
                            user_lang = (token_row.get('language_code') or 'es').strip().lower()
                            try:
                                thread_pool_executor.submit( send_fcm_next_race_notification_task, user_id, token, desc_carrera, next_race_name, id_porra_actual, next_race_id, user_lang )
                                users_notified_about_next_race.add(user_id)
                                submitted_count_next += 1
                            except Exception as submit_err: print(f"!!!!!!!! ERROR [Notif Next Race]: Fallo al hacer submit para user {user_id}. Error: {submit_err} !!!!!!!!!!")
                        print(f"DEBUG: Enviadas {submitted_count_next} tareas de notificación 'Next Race Available' al executor para porra {id_porra_actual}.")
                else: print(f"DEBUG: No se encontraron tokens FCM válidos para notificar 'Next Race Available' en porra {id_porra_actual}.")
        # --- Fin bucle porras ---

        # --- 5. Comprobar fin temporada ---
        cur.execute("SELECT COUNT(*) FROM carrera WHERE ano = %s;", (ano_carrera,))
        total_races_year = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM carrera WHERE ano = %s AND resultado_detallado IS NOT NULL;", (ano_carrera,))
        finished_races_year = cur.fetchone()[0]
        is_season_finished = (total_races_year > 0 and finished_races_year >= total_races_year)

        if is_season_finished:
             print(f"INFO: Temporada {ano_carrera} finalizada. Comprobando trofeos de temporada...")
             for porra_row in porras_del_ano:
                 id_porra_actual = porra_row['id_porra']
                 tipo_porra_actual = porra_row['tipo_porra']
                 member_count_porra = porra_row['member_count'] # <-- Re-acceder al recuento de miembros
                 detalles_temporada = {"ano": ano_carrera, "id_porra": id_porra_actual}

                 # Trofeo Campeón Temporada
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
                                # Trofeo campeón estándar (si >= 5 miembros)
                                if member_count_porra >= 5: # <--- CONDICIÓN YA EXISTENTE Y CORRECTA
                                    _award_trophy(winner_user_id, 'CAMPEON_TEMPORADA', conn, cur, detalles=detalles_temporada)
                                # Trofeo campeón público (NO requiere 5+ miembros según descripción)
                                if tipo_porra_actual == 'PUBLICA':
                                    _award_trophy(winner_user_id, 'CAMPEON_TEMPORADA_PUBLICA', conn, cur, detalles=detalles_temporada)
                                processed_winners.add(winner_user_id)
                         else: break 

                 # Trofeo Aplicado (participar en todas las carreras - NO requiere 5+ miembros)
                 cur.execute("SELECT id_usuario FROM participacion WHERE id_porra = %s AND estado IN ('CREADOR', 'ACEPTADA');", (id_porra_actual,))
                 participantes = cur.fetchall()
                 for participante in participantes:
                     user_id_part = participante['id_usuario']
                     sql_count_bets = "SELECT COUNT(DISTINCT a.id_carrera) FROM apuesta a JOIN carrera c ON a.id_carrera = c.id_carrera WHERE a.id_usuario = %s AND a.id_porra = %s AND c.ano = %s AND a.estado_apuesta = 'ACEPTADA';"
                     cur.execute(sql_count_bets, (user_id_part, id_porra_actual, ano_carrera))
                     bets_count_user = cur.fetchone()[0]
                     if bets_count_user >= total_races_year:
                         _award_trophy(user_id_part, 'APLICADO', conn, cur, detalles=detalles_temporada)
        # --- Fin comprobación fin temporada ---

        conn.commit()
        cur.close()
        return jsonify({"mensaje": f"Resultado detallado y detalles piloto carrera {id_carrera} guardados.", "puntuaciones_calculadas_totales": total_puntuaciones_calculadas, "fin_temporada_comprobado": is_season_finished}), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR DETALLADO en actualizar_resultado_carrera:")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al actualizar resultado/calcular puntos"}), 500
    finally:
        if conn is not None and not conn.closed:
             conn.close()
# --- FIN Endpoint PUT Resultado MODIFICADO ---


# --- NUEVO Endpoint para OBTENER las puntuaciones de una carrera específica ---
# --- Endpoint GET /api/porras/<id_porra>/carreras/<id_carrera>/puntuaciones (MODIFICADO con Paginación y Datos Usuario) ---
@app.route('/api/porras/<int:id_porra>/carreras/<int:id_carrera>/puntuaciones', methods=['GET'])
@jwt_required()
def obtener_puntuaciones_porra_carrera(id_porra, id_carrera):
    id_usuario_actual_str = get_jwt_identity()
    try:
        id_usuario_actual = int(id_usuario_actual_str)
    except (ValueError, TypeError):
        return jsonify({"error": "Token de usuario inválido"}), 400

    # --- Paginación ---
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 25))
        if page < 1: page = 1
        if page_size < 1: page_size = 25
        if page_size > 100: page_size = 100
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

        # --- 2. Obtener Puntuaciones PAGINADAS ---
        sql_get_scores_page = f"""
            WITH RankedRaceScores AS (
                SELECT
                    u.id_usuario,
                    u.nombre,
                    COALESCE(p.puntos, 0) as puntos, -- Puntos de esta carrera específica
                    RANK() OVER (ORDER BY COALESCE(p.puntos, 0) DESC, u.nombre ASC) as posicion
                FROM usuario u
                JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s AND p.id_carrera = %s
                WHERE pa.id_porra = %s -- Asegura que solo contamos participantes activos
            )
            SELECT id_usuario, nombre, puntos, posicion
            FROM RankedRaceScores
            ORDER BY posicion ASC
            LIMIT %s OFFSET %s;
        """
        cur.execute(sql_get_scores_page, (id_porra, id_porra, id_carrera, id_porra, page_size, offset))
        puntuaciones_pagina = cur.fetchall()

        # --- 3. Obtener Total de Items (participantes activos en la porra) ---
        sql_count_total = """
             SELECT COUNT(DISTINCT u.id_usuario)
             FROM usuario u
             JOIN participacion pa ON u.id_usuario = pa.id_usuario
             WHERE pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA');
        """
        cur.execute(sql_count_total, (id_porra,))
        total_items = cur.fetchone()[0]

        # --- 4. Obtener Datos del Usuario Actual (Rank y Puntos para ESTA carrera) ---
        my_rank = None
        my_score = None
        sql_get_user_rank_race = """
             WITH RankedRaceScores AS (
                 SELECT
                    u.id_usuario,
                    COALESCE(p.puntos, 0) as puntos,
                    RANK() OVER (ORDER BY COALESCE(p.puntos, 0) DESC, u.nombre ASC) as posicion
                 FROM usuario u
                 JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                 LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s AND p.id_carrera = %s
                 WHERE pa.id_porra = %s -- Asegura que solo contamos participantes activos
            )
            SELECT posicion, puntos
            FROM RankedRaceScores
            WHERE id_usuario = %s;
        """
        cur.execute(sql_get_user_rank_race, (id_porra, id_porra, id_carrera, id_porra, id_usuario_actual))
        user_rank_data = cur.fetchone()
        if user_rank_data:
            my_rank = user_rank_data['posicion']
            my_score = user_rank_data['puntos']

        cur.close()

        # --- 5. Formatear Respuesta ---
        lista_puntuaciones_pagina = []
        for row in puntuaciones_pagina:
            lista_puntuaciones_pagina.append({
                "posicion": row["posicion"],
                "nombre": row["nombre"],
                "puntos": row["puntos"],
                "id_usuario": row["id_usuario"] # Añadir ID para identificar al usuario en Flutter
            })

        return jsonify({
            "my_rank": my_rank,
            "my_score": my_score,
            "total_items": total_items,
            "page": page,
            "page_size": page_size,
            "items": lista_puntuaciones_pagina # Solo la página actual
        }), 200 # 200 OK

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error en obtener_puntuaciones_porra_carrera (paginado): {error}")
        import traceback
        traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al obtener las puntuaciones de la carrera"}), 500
    finally:
        if conn is not None:
            conn.close()


# --- NUEVO Endpoint para OBTENER la clasificación general de un año ---
# --- Endpoint GET /api/porras/<id_porra>/clasificacion (MODIFICADO con Paginación y Datos Usuario) ---
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
        # Usamos RANK() para obtener la posición real
        sql_get_standings_page = f"""
            WITH RankedScores AS (
                SELECT
                    u.id_usuario,
                    u.nombre,
                    COALESCE(SUM(p.puntos), 0) as puntos_totales,
                    RANK() OVER (ORDER BY COALESCE(SUM(p.puntos), 0) DESC, u.nombre ASC) as posicion
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
        my_rank = None
        my_score = None
        sql_get_user_rank = """
            WITH RankedScores AS (
                 SELECT
                    u.id_usuario,
                    COALESCE(SUM(p.puntos), 0) as puntos_totales,
                    RANK() OVER (ORDER BY COALESCE(SUM(p.puntos), 0) DESC, u.nombre ASC) as posicion
                 FROM usuario u
                 LEFT JOIN participacion pa ON u.id_usuario = pa.id_usuario AND pa.id_porra = %s AND pa.estado IN ('CREADOR', 'ACEPTADA')
                 LEFT JOIN puntuacion p ON u.id_usuario = p.id_usuario AND p.id_porra = %s
                 WHERE pa.id_porra = %s -- Asegura que solo contamos participantes activos
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
                "id_usuario": row["id_usuario"] # Añadir ID para identificar al usuario en Flutter
            })

        return jsonify({
            "my_rank": my_rank,
            "my_score": my_score,
            "total_items": total_items,
            "page": page,
            "page_size": page_size,
            "items": lista_clasificacion_pagina # Solo la página actual
        }), 200 # 200 OK

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

        # --- MODIFICADO: Obtener estado_apuesta ---
        sql_get_races_and_status = """
            SELECT
                c.id_carrera, c.ano, c.desc_carrera, c.fecha_limite_apuesta,
                (CASE WHEN a.id_apuesta IS NOT NULL THEN TRUE ELSE FALSE END) as has_bet,
                a.estado_apuesta, -- <<< AÑADIDO
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

        # Formatear fechas y devolver (incluir estado_apuesta)
        lista_resultado = []
        for row_raw in races_with_status_raw:
            row = dict(row_raw)
            if 'fecha_limite_apuesta' in row and isinstance(row['fecha_limite_apuesta'], datetime):
                 row['fecha_limite_apuesta'] = row['fecha_limite_apuesta'].isoformat()
            row['has_results'] = bool(row.get('has_results', False))
            # estado_apuesta puede ser None si has_bet es False, lo cual está bien
            row['estado_apuesta'] = row.get('estado_apuesta') # <<< INCLUIDO
            lista_resultado.append(row)

        return jsonify(lista_resultado), 200

    except psycopg2.DatabaseError as db_error: # Sin cambios manejo error
        print(f"Error DB en get_my_races_with_bet_status: {db_error}")
        if conn: conn.close()
        return jsonify({"error": "Error DB obteniendo estado carreras"}), 500
    except Exception as error: # Sin cambios manejo error
        print(f"ERROR DETALLADO en get_my_races_with_bet_status:"); import traceback; traceback.print_exc()
        if conn: conn.close()
        return jsonify({"error": "Error interno al obtener estado de apuestas de carreras"}), 500
    finally: # Sin cambios finally
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



# --- NUEVO Endpoint POST /api/porras/publica/<id_porra>/join ---
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

        # 1. Verificar si la porra existe y es pública
        cur.execute("SELECT es_publica, id_creador FROM porra WHERE id_porra = %s;", (id_porra,))
        porra_info = cur.fetchone()

        if not porra_info:
            return jsonify({"error": "Porra no encontrada"}), 404
        if not porra_info['es_publica']:
            return jsonify({"error": "Esta porra no es pública"}), 403 # Forbidden

        # 2. (Opcional) Impedir que el creador se una a sí mismo (ya debería estar por la creación)
        if porra_info['id_creador'] == id_usuario_actual:
            # Podrías simplemente devolver éxito o un mensaje indicando que ya es creador
            return jsonify({"mensaje": "Ya eres el creador de esta porra"}), 200 # O 409 Conflict si prefieres

        # 3. Verificar si el usuario ya es miembro
        cur.execute("SELECT 1 FROM participacion WHERE id_porra = %s AND id_usuario = %s;", (id_porra, id_usuario_actual))
        ya_es_miembro = cur.fetchone()

        if ya_es_miembro:
            return jsonify({"error": "Ya eres miembro de esta porra"}), 409 # Conflict

        # 4. Si es pública y no es miembro, añadirlo
        sql_insert = """
            INSERT INTO participacion (id_porra, id_usuario, estado)
            VALUES (%s, %s, 'ACEPTADA')
            ON CONFLICT (id_porra, id_usuario) DO NOTHING; -- Seguridad extra por si acaso
        """
        cur.execute(sql_insert, (id_porra, id_usuario_actual))

        conn.commit()
        cur.close()

        return jsonify({"mensaje": "Te has unido a la porra pública correctamente"}), 200 # OK (o 201 si prefieres)

    except psycopg2.Error as db_error: # Captura errores específicos de psycopg2
        print(f"Error de base de datos en unirse_porra_publica: {db_error}")
        if conn: conn.rollback()
        # Evita exponer detalles del error de BD
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

        # --- MODIFICADO: Obtener estado_apuesta ---
        sql_get_bets = """
            SELECT id_apuesta, id_porra, id_carrera, id_usuario, posiciones, vrapida, estado_apuesta
            FROM apuesta
            WHERE id_porra = %s AND id_usuario = %s
            ORDER BY id_carrera ASC; -- Opcional: ordenar por carrera
        """
        cur.execute(sql_get_bets, (id_porra, id_usuario_actual))
        my_bets = cur.fetchall()
        cur.close()

        # Formatear la respuesta
        lista_apuestas_formateada = []
        for apuesta in my_bets:
            try:
                # Parsear JSONB (sin cambios)
                pos_data = apuesta['posiciones']
                posiciones_list = []
                if isinstance(pos_data, str): posiciones_list = json.loads(pos_data)
                elif isinstance(pos_data, list): posiciones_list = pos_data
                elif isinstance(pos_data, dict) and all(isinstance(k, int) for k in pos_data.keys()): posiciones_list = [pos_data[k] for k in sorted(pos_data.keys())]
                else: raise TypeError("Tipo inesperado para 'posiciones'")

                apuesta_formateada = {
                    "id_apuesta": apuesta["id_apuesta"], "id_porra": apuesta["id_porra"],
                    "id_carrera": apuesta["id_carrera"], "id_usuario": apuesta["id_usuario"],
                    "posiciones": posiciones_list, "vrapida": apuesta["vrapida"],
                    "estado_apuesta": apuesta["estado_apuesta"] # <<< AÑADIDO
                }
                lista_apuestas_formateada.append(apuesta_formateada)
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                 print(f"Error formateando my-bets (ID Apuesta: {apuesta.get('id_apuesta')}): {e}")
                 continue

        return jsonify(lista_apuestas_formateada), 200

    except psycopg2.DatabaseError as db_error: # Sin cambios manejo error
        print(f"Error DB en get_all_my_bets_in_porra: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos al obtener mis apuestas"}), 500
    except Exception as error: # Sin cambios manejo error
        print(f"Error general en get_all_my_bets_in_porra: {error}")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al obtener mis apuestas"}), 500
    finally: # Sin cambios finally
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
    cur = None # Asegurar que cur se pueda cerrar en finally
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Obtener detalles apuesta, porra, usuario APOSTADOR y carrera
        sql_get_info = """
            SELECT
                a.id_usuario, a.id_carrera, a.estado_apuesta,
                p.id_porra, p.nombre_porra, p.id_creador, p.tipo_porra,
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

        # 2. Validaciones (Porra admin, Creador, Estado pendiente)
        if info['tipo_porra'] != 'PRIVADA_ADMINISTRADA': return jsonify({"error": "Apuesta no pertenece a porra administrada"}), 403
        try:
            id_creador_db = info['id_creador']
            id_usuario_actual_int = int(id_usuario_actual)
            if id_creador_db != id_usuario_actual_int: return jsonify({"error": "Solo el creador puede gestionar"}), 403
        except (ValueError, TypeError): return jsonify({"error": "Error interno autorización"}), 500
        if info['estado_apuesta'] != 'PENDIENTE': return jsonify({"error": "Apuesta ya no está pendiente"}), 409

        # 3. Actualizar estado de la apuesta
        nuevo_estado = 'ACEPTADA' if aceptar else 'RECHAZADA'
        fecha_decision = datetime.now(timezone.utc)
        sql_update_bet = "UPDATE apuesta SET estado_apuesta = %s, fecha_estado_apuesta = %s WHERE id_apuesta = %s;"
        cur.execute(sql_update_bet, (nuevo_estado, fecha_decision, id_apuesta))

        # --- 4. Enviar Notificación (si hay token) ---
        fcm_token_apostador = info.get('fcm_token')
        id_usuario_apostador = info.get('id_usuario')
        nombre_carrera = info.get('desc_carrera', 'esta carrera')
        nombre_porra = info.get('nombre_porra', 'esta porra')

        if fcm_token_apostador and id_usuario_apostador:
            print(f"DEBUG [Responder Apuesta]: Intentando enviar notif '{nuevo_estado}' a user {id_usuario_apostador}...")
            user_lang = (info.get('language_code') or 'es').strip().lower()
            global thread_pool_executor
            if thread_pool_executor:
                 thread_pool_executor.submit(
                     send_fcm_bet_status_notification_task,
                     id_usuario_apostador,
                     fcm_token_apostador,
                     nombre_carrera,
                     nombre_porra,
                     nuevo_estado, # 'ACEPTADA' o 'RECHAZADA'
                     user_lang   
                 )
                 print(f"DEBUG [Responder Apuesta]: Tarea FCM ({nuevo_estado}) enviada al executor.")
            else:
                 print("WARN [Responder Apuesta]: ThreadPoolExecutor no disponible, no se pudo enviar tarea FCM.")
        else:
             print(f"DEBUG [Responder Apuesta]: No se envía notificación (token o ID apostador faltante). Token: {'Sí' if fcm_token_apostador else 'No'}, ID: {id_usuario_apostador}")
        # --- Fin Enviar Notificación ---

        conn.commit()
        cur.close()

        mensaje = f"Apuesta {'aceptada' if aceptar else 'rechazada'} correctamente."
        return jsonify({"mensaje": mensaje}), 200

    except psycopg2.Error as db_error: # Sin cambios manejo error
        print(f"Error DB en responder_apuesta_pendiente: {db_error}")
        if conn: conn.rollback()
        return jsonify({"error": "Error de base de datos"}), 500
    except Exception as error: # Sin cambios manejo error
        print(f"Error inesperado en responder_apuesta_pendiente: {error}")
        import traceback; traceback.print_exc()
        if conn: conn.rollback()
        return jsonify({"error": "Error interno al responder a la apuesta"}), 500
    finally: # Sin cambios
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