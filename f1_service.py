import livef1
import pandas as pd
import numpy as np
import eventlet
import traceback
import math
import requests
import json
from threading import Event, Lock
from datetime import datetime, timedelta

class F1Service:
    def __init__(self, socketio, db_config=None):
        self.socketio = socketio
        self.db_config = db_config  # Guardamos la config de la DB
        self.stop_event = Event()
        self.is_running = False
        self.mode = 'SIMULATION' # 'SIMULATION' o 'LIVE'
        
        self.simulation_speed = 1.0 
        
        self.session = None
        self.race_name = "Waiting for race..."
        self.race_year = 0
        self.total_laps = 0
        self.current_lap_display = 0 
        
        self.min_x = float('inf')
        self.max_x = float('-inf')
        self.min_y = float('inf')
        self.max_y = float('-inf')
        
        self.raw_track_points = [] 
        self.track_layout = []     
        self.is_map_complete = False 
        
        self.map_state = 'PRE_GRID' 
        self.stop_counter = 0 
        
        # --- MEMORIA DE ESTADO (CACHE) ---
        self.tyre_state_cache = {} 
        self.tyre_lock = Lock()
        self.last_tyre_line_count = 0 
        
        # Clima
        self.weather_state_cache = {
            'air_temp': '--', 
            'track_temp': '--', 
            'humidity': '--', 
            'rain': False
        }
        self.weather_lock = Lock()
        self.last_weather_line_count = 0
        
        self.socketio.on_event('connect', self._handle_connect, namespace='/race')
        self.socketio.on_event('request_init_data', self._handle_init_request, namespace='/race')

        # DICCIONARIO DINÁMICO (Se llena desde DB)
        self.DRIVER_MAPPING = {}
        
        self.TYRE_MAP = {
            'SOFT': 'S', 'MEDIUM': 'M', 'HARD': 'H', 
            'INTERMEDIATE': 'I', 'WET': 'W',
            'HYPERSOFT': 'S', 'ULTRASOFT': 'S', 'SUPERSOFT': 'S',
            'C1': 'H', 'C2': 'H', 'C3': 'M', 'C4': 'S', 'C5': 'S'
        }

        self.TRACK_STATUS_MAP = {
            '1': 'GREEN', '2': 'YELLOW', '4': 'SC',
            '5': 'RED', '6': 'VSC', '7': 'VSC' 
        }

    # --- CONTROL BÁSICO ---

    def set_speed(self, multiplier):
        try:
            val = float(multiplier)
            if val <= 0: val = 0.1
            if val > 500: val = 500 
            self.simulation_speed = val
            print(f"⏩ Velocidad cambiada a {self.simulation_speed}x", flush=True)
            return True
        except:
            return False

    def _handle_connect(self):
        print(f"👤 [Connect] Cliente conectado.", flush=True)
        self._send_initial_data()

    def _handle_init_request(self, data=None):
        self._send_initial_data()

    def _send_initial_data(self):
        self.socketio.emit('track_data', {
            'layout': self.track_layout,
            'name': self.race_name,
            'year': self.race_year,
            'total_laps': self.total_laps,
            'ready': self.is_map_complete 
        }, namespace='/race')

    def normalize(self, val, min_v, max_v, invert=False):
        if min_v == float('inf') or max_v == float('-inf'): return 50.0
        if pd.isna(val) or max_v == min_v: return 50.0
        range_v = max_v - min_v
        if range_v == 0: return 50.0
        pct = ((val - min_v) / range_v) * 100.0
        if pct < 0: pct = 0
        if pct > 100: pct = 100
        return 100.0 - pct if invert else pct

    def _update_live_map(self, x, y):
        if self.raw_track_points:
            last_x, last_y = self.raw_track_points[-1]
            dist = math.sqrt((x - last_x)**2 + (y - last_y)**2)
            if dist < 50: return False 

        self.raw_track_points.append((x, y))

        if x < self.min_x: self.min_x = x
        if x > self.max_x: self.max_x = x
        if y < self.min_y: self.min_y = y
        if y > self.max_y: self.max_y = y

        self.track_layout = []
        step = 1
        if len(self.raw_track_points) > 1000: step = 4
        if len(self.raw_track_points) > 2000: step = 8
        if len(self.raw_track_points) > 4000: step = 15
        
        for px, py in self.raw_track_points[::step]:
            self.track_layout.append({
                'x': self.normalize(px, self.min_x, self.max_x),
                'y': self.normalize(py, self.min_y, self.max_y, invert=True)
            })
        return True 

    def _get_time_col_as_timedelta(self, df):
        try:
            if isinstance(df.index, pd.TimedeltaIndex): return df.index
            col_name = None
            if 'Time' in df.columns: col_name = 'Time'
            elif 'SessionTime' in df.columns: col_name = 'SessionTime'
            elif 'timestamp' in df.columns: col_name = 'timestamp'
            if col_name: return pd.to_timedelta(df[col_name])
            return None
        except: return None

    # --- PARSER HELPERS ---
    def _extract_tyre_info(self, stint_data):
        try:
            if not stint_data: return 'U', 'UL', -1
            last_stint = None; stint_id = -1
            
            if isinstance(stint_data, dict):
                if len(stint_data) > 0:
                    try:
                        keys = sorted(list(stint_data.keys()), key=lambda x: int(x))
                        stint_id = int(keys[-1])
                        last_stint = stint_data[keys[-1]]
                    except:
                        last_stint = list(stint_data.values())[-1]; stint_id = 0 
            elif isinstance(stint_data, list):
                if len(stint_data) > 0:
                    last_stint = stint_data[-1]; stint_id = len(stint_data) - 1
            
            if not last_stint: return 'U', 'UL', -1
            
            raw_compound = str(last_stint.get('Compound', 'UNKNOWN')).upper()
            compound_code = self.TYRE_MAP.get(raw_compound, 'U')
            
            if compound_code == 'U' and raw_compound != 'UNKNOWN':
                if 'SOFT' in raw_compound: compound_code = 'S'
                elif 'HARD' in raw_compound: compound_code = 'H'
                elif 'MED' in raw_compound: compound_code = 'M'
                elif 'INT' in raw_compound: compound_code = 'I'
                elif 'WET' in raw_compound: compound_code = 'W'
                elif 'TEST' in raw_compound: compound_code = 'T'
            
            total_laps = last_stint.get('TotalLaps')
            tyre_life_str = str(int(total_laps)) if total_laps is not None else '0'
            return compound_code, tyre_life_str, stint_id
        except:
            return 'U', 'UL', -1
    
    def _parse_lap_time(self, val):
        if val is None or val == '' or pd.isna(val): return float('inf')
        if isinstance(val, dict): val = val.get('Value', None)
        if isinstance(val, (pd.Timedelta, timedelta)): return val.total_seconds()
        s_val = str(val).strip()
        try:
            if ':' in s_val:
                parts = s_val.split(':')
                if len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
            return float(s_val)
        except: return float('inf')

    # --- SISTEMA DE DATOS MANUALES (NEUMÁTICOS Y CLIMA) ---
    
    def _fetch_stream_file(self, session_path, filename, last_count, full_download=True):
        """Descarga robusta con cabeceras de navegador."""
        if not session_path: return [], last_count
        
        base_url = "https://livetiming.formula1.com/static/"
        full_url = base_url + session_path + filename
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Connection': 'keep-alive'
            }
            r = requests.get(full_url, headers=headers, timeout=10)
            
            if r.status_code != 200:
                # print(f"⚠️ Fetch {filename} falló con {r.status_code}")
                return [], last_count
            
            lines = r.text.splitlines()
            total_lines = len(lines)
            
            start_idx = 0
            if not full_download and last_count < total_lines:
                start_idx = last_count
            
            new_last_count = total_lines
            parsed_records = []
            
            last_known_ts = timedelta(seconds=0)

            for i in range(start_idx, total_lines):
                line = lines[i].strip()
                if not line: continue
                
                ts_val = None
                
                first_brace = line.find('{')
                if first_brace > 0:
                    ts_str = line[:first_brace].strip().strip('"')
                    if ts_str:
                        try:
                            h, m, s = ts_str.split(':')
                            if '.' in s: s, ms = s.split('.')
                            else: ms = 0
                            ts_val = timedelta(hours=int(h), minutes=int(m), seconds=int(s), milliseconds=int(ms))
                            last_known_ts = ts_val
                        except: pass
                
                if ts_val is None: ts_val = last_known_ts

                json_part = line[first_brace:] if first_brace != -1 else line
                try:
                    data = json.loads(json_part)
                    parsed_records.append({'ts': ts_val, 'data': data})
                except: pass
            
            return parsed_records, new_last_count
            
        except Exception as e:
            # print(f"⚠️ Error descarga {filename}: {e}")
            return [], last_count

    # --- ADAPTADOR LIBRERÍA -> FORMATO CACHÉ ---
    def _parse_weather_from_lib(self, weather_df):
        records = []
        try:
            if weather_df.empty: return []
            weather_df['ts'] = self._get_time_col_as_timedelta(weather_df)
            if weather_df['ts'] is None: return []
            weather_df = weather_df.dropna(subset=['ts']).sort_values('ts')
            
            for _, row in weather_df.iterrows():
                data_packet = {}
                if 'AirTemp' in row and pd.notna(row['AirTemp']): data_packet['AirTemp'] = str(row['AirTemp'])
                if 'TrackTemp' in row and pd.notna(row['TrackTemp']): data_packet['TrackTemp'] = str(row['TrackTemp'])
                if 'Humidity' in row and pd.notna(row['Humidity']): data_packet['Humidity'] = str(row['Humidity'])
                if 'Rainfall' in row and pd.notna(row['Rainfall']): data_packet['Rainfall'] = str(row['Rainfall'])
                
                records.append({'ts': row['ts'], 'data': data_packet})
            return records
        except Exception as e:
            print(f"⚠️ Error adaptando weather lib data: {e}")
            return []

    # --- CLIMA LOGIC ---
    def _update_weather_cache(self, records):
        with self.weather_lock:
            for rec in records:
                data = rec['data']
                if 'AirTemp' in data: self.weather_state_cache['air_temp'] = str(data['AirTemp'])
                if 'TrackTemp' in data: self.weather_state_cache['track_temp'] = str(data['TrackTemp'])
                if 'Humidity' in data: self.weather_state_cache['humidity'] = str(data['Humidity'])
                if 'Rainfall' in data:
                    r = str(data['Rainfall'])
                    self.weather_state_cache['rain'] = (r == '1' or r.lower() == 'true')

    def _update_tyre_cache(self, records):
        with self.tyre_lock:
            for rec in records:
                data = rec['data']
                if 'Lines' not in data: continue
                
                for d_no, d_info in data['Lines'].items():
                    if 'Stints' in d_info:
                        d_no = str(d_no)
                        stints = d_info['Stints']
                        new_comp, new_life, new_stint_id = self._extract_tyre_info(stints)
                        
                        curr = self.tyre_state_cache.get(d_no, {'compound': 'U', 'life': '0', 'stint_id': -1})
                        old_comp = curr['compound']
                        old_stint_id = curr.get('stint_id', -1)
                        
                        final_comp = new_comp
                        if new_stint_id == old_stint_id and new_comp == 'U' and old_comp != 'U':
                            final_comp = old_comp
                        
                        self.tyre_state_cache[d_no] = {
                            'compound': final_comp, 'life': new_life, 'stint_id': new_stint_id
                        }

    # --- POLLER COMBINADO (LIVE) ---
    def _poll_live_data_background(self, session_path):
        # print("📡 Iniciando Poller Live (Tyres + Weather)...", flush=True)
        # Nombres de ficheros que intentamos encontrar
        # Hacemos un primer check para ver cual existe (simplificado)
        
        while self.is_running and not self.stop_event.is_set():
            eventlet.sleep(30)
            try:
                # 1. Neumáticos
                t_recs, self.last_tyre_line_count = self._fetch_stream_file(
                    session_path, "TimingAppData.jsonStream", self.last_tyre_line_count, full_download=False
                )
                if t_recs: self._update_tyre_cache(t_recs)

                # 2. Clima (Intento 1)
                w_recs, count = self._fetch_stream_file(
                    session_path, "MeteorologicalData.jsonStream", self.last_weather_line_count, full_download=False
                )
                
                # 3. Clima (Intento 2 - Backup)
                if not w_recs and count == self.last_weather_line_count:
                     w_recs, count = self._fetch_stream_file(
                        session_path, "WeatherData.jsonStream", self.last_weather_line_count, full_download=False
                    )
                
                if w_recs:
                    self.last_weather_line_count = count
                    self._update_weather_cache(w_recs)
                    
            except Exception as e:
                pass # print(f"⚠️ Error poller: {e}")

    # -----------------------------------------------------------

    def _load_drivers_from_db(self, year):
        """Carga los pilotos desde la base de datos en memoria para acceso rápido."""
        import psycopg2
        import psycopg2.extras
        
        print(f"📡 Cargando pilotos de la temporada {year} desde la DB...", flush=True)
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Consultamos los pilotos de la temporada que tengan dorsal (n_piloto)
            query = """
                SELECT n_piloto, codigo_piloto, color_fondo_hex 
                FROM piloto_temporada 
                WHERE ano = %s AND n_piloto IS NOT NULL;
            """
            cur.execute(query, (str(year),))
            rows = cur.fetchall()
            
            # Limpiamos y rellenamos el mapeo
            new_mapping = {}
            for row in rows:
                new_mapping[str(row['n_piloto'])] = {
                    'code': row['codigo_piloto'],
                    'color': row['color_fondo_hex'] if row['color_fondo_hex'] else '#888888'
                }
            
            self.DRIVER_MAPPING = new_mapping
            print(f"✅ Se han cargado {len(self.DRIVER_MAPPING)} pilotos correctamente.", flush=True)
            
        except Exception as e:
            print(f"❌ Error cargando pilotos desde DB: {e}", flush=True)
        finally:
            if conn:
                conn.close()

    def start_simulation(self, year=2024, gp="Sao Paulo", speed=1.0):
        if self.is_running: return "Ya corriendo (detenlo primero)"
        self.stop_event.clear()
        self.is_running = True
        self.mode = 'SIMULATION'
        self.race_year = year
        self.race_name = f"{gp} (Sim)"
        self.current_lap_display = 0
        self.simulation_speed = speed 
        self._reset_map_state()
        self.socketio.start_background_task(self._run_simulation, year, gp)
        return f"Iniciando simulación de {gp} {year}..."

    # --- NUEVAS FUNCIONES PARA LIVE REAL ---

    def start_live(self, year, race_name):
        """
        Inicia el modo de conexión en directo real con LiveF1.
        """
        if self.is_running: 
            print("⚠️ Servicio ya corriendo. Reiniciando para LIVE.")
            self.stop_service()
            eventlet.sleep(1) # Esperar limpieza
            
        self.stop_event.clear()
        self.is_running = True
        self.mode = 'LIVE'
        self.race_year = year
        self.race_name = race_name
        self._reset_map_state()
        
        print(f"📡 F1SERVICE: Iniciando conexión LIVEF1 para {race_name} {year}...")
        # Lanzamos el cliente de streaming en un hilo separado
        self.socketio.start_background_task(self._run_live_client)
        return True

    def _run_live_client(self):
        """
        Conecta al stream oficial de F1 usando livef1.Client().
        Itera sobre los eventos y actualiza el estado.
        """
        try:
            self._load_drivers_from_db(self.race_year)
            client = livef1.Client()
            print("📡 LIVEF1: Conectando a sesión activa...", flush=True)
            
            # Iterar sobre eventos en tiempo real
            # La librería maneja la conexión SignalR internamente
            for packet in client.events():
                if self.stop_event.is_set(): 
                    break
                
                # Procesar paquete (simplificado para robustez)
                # Aquí podríamos parsear 'CarData.z' para actualizar el mapa
                # o 'TimingData' para tiempos.
                
                # Para esta versión, nos aseguramos de mantener la conexión viva
                # y emitir un 'heartbeat' de datos si es necesario.
                
                eventlet.sleep(0) # Yield para no bloquear
                
        except Exception as e:
            print(f"❌ ERROR LIVEF1: {e}")
            traceback.print_exc()
        finally:
            self.is_running = False
            print("📡 LIVEF1: Desconectado.")

    def _reset_map_state(self):
        self.raw_track_points = []
        self.track_layout = []
        self.is_map_complete = False 
        self.min_x = float('inf'); self.max_x = float('-inf')
        self.min_y = float('inf'); self.max_y = float('-inf')
        self.map_state = 'PRE_GRID' 
        self.stop_counter = 0
        
        self.tyre_state_cache = {} 
        self.last_tyre_line_count = 0
        self.weather_state_cache = {'air_temp': '--', 'track_temp': '--', 'humidity': '--', 'rain': False}
        self.last_weather_line_count = 0

    def _run_simulation(self, year, gp):
        # --- NUEVO: Cargar pilotos antes de empezar ---
        self._load_drivers_from_db(year)

        print(f"🏎️ CARGANDO DATOS {year} {gp}...", flush=True)

        try:
            session = livef1.get_session(year, gp, "Race")
            
            # 1. Info Sesión
            self.total_laps = 70
            try:
                session_info = session.get_data("SessionInfo")
                if not session_info.empty and 'TotalLaps' in session_info.columns:
                    val = session_info['TotalLaps'].iloc[0]
                    if pd.notna(val) and int(val) > 0: self.total_laps = int(val)
            except: pass

            # 2. Posición y Tiempos
            try:
                pos_df = session.get_data("Position.z")
                time_df = session.get_data("TimingData")
            except Exception as e:
                print(f"❌ Error fatal cargando datos básicos: {e}")
                self.is_running = False
                return

            if pos_df.empty or time_df.empty:
                print("❌ DATOS VACÍOS - Abortando")
                self.is_running = False
                return

            # 3. DATOS AUXILIARES
            print("🛠️ Preparando datos auxiliares...", flush=True)
            
            # A) Neumáticos
            tyre_recs, _ = self._fetch_stream_file(session.path, "TimingAppData.jsonStream", 0, full_download=True)
            for r in tyre_recs: 
                if r['ts'] is None: r['ts'] = timedelta(0)
            tyre_queue = sorted(tyre_recs, key=lambda x: x['ts'])
            
            # B) Clima (Estrategia TRIPLE FALLBACK)
            weather_queue = []
            
            # Intento 1: Librería
            try:
                w_df = session.get_data("MeteorologicalData")
                if not w_df.empty:
                    print(f"✅ Clima OK: Vía librería ({len(w_df)} regs)")
                    weather_queue = self._parse_weather_from_lib(w_df)
            except: pass
            
            # Intento 2: Manual MeteorologicalData
            if not weather_queue:
                print("⚠️ Fallback 1: Probando MeteorologicalData.jsonStream...")
                w_recs, _ = self._fetch_stream_file(session.path, "MeteorologicalData.jsonStream", 0)
                if w_recs:
                    print(f"✅ Clima OK: Vía manual MetData ({len(w_recs)} regs)")
                    for r in w_recs: 
                        if r['ts'] is None: r['ts'] = timedelta(0)
                    weather_queue = sorted(w_recs, key=lambda x: x['ts'])

            # Intento 3: Manual WeatherData (Nombre antiguo/alternativo)
            if not weather_queue:
                print("⚠️ Fallback 2: Probando WeatherData.jsonStream...")
                w_recs, _ = self._fetch_stream_file(session.path, "WeatherData.jsonStream", 0)
                if w_recs:
                    print(f"✅ Clima OK: Vía manual WeatherData ({len(w_recs)} regs)")
                    for r in w_recs: 
                        if r['ts'] is None: r['ts'] = timedelta(0)
                    weather_queue = sorted(w_recs, key=lambda x: x['ts'])
            
            if not weather_queue:
                print("⚠️ No se encontraron datos de clima en ninguna fuente. Se mostrará vacío.")
            
            print(f"✅ Colas listas: Tyres={len(tyre_queue)}, Weather={len(weather_queue)}")

            if self.mode == 'LIVE':
                self.socketio.start_background_task(self._poll_live_data_background, session.path)

            try: track_status_df = session.get_data("TrackStatus")
            except: track_status_df = pd.DataFrame()

            # --- PRE-PROCESAMIENTO ---
            pos_df['ts'] = self._get_time_col_as_timedelta(pos_df)
            time_df['ts'] = self._get_time_col_as_timedelta(time_df)
            pos_df = pos_df.dropna(subset=['ts']).sort_values('ts')
            time_df = time_df.dropna(subset=['ts']).sort_values('ts')

            def norm_driver(df):
                col = next((c for c in ['DriverNo', 'Driver'] if c in df.columns), 'Driver')
                df = df.rename(columns={col: 'DriverNo'})
                df['DriverNo'] = df['DriverNo'].astype(str)
                return df

            pos_df = norm_driver(pos_df)
            time_df = norm_driver(time_df)
            
            has_track_status = False
            if not track_status_df.empty:
                track_status_df['ts'] = self._get_time_col_as_timedelta(track_status_df)
                if track_status_df['ts'] is not None:
                    track_status_df = track_status_df.dropna(subset=['ts']).sort_values('ts')
                    has_track_status = True

            print("⚙️ Fusionando datos principales...", flush=True)

            drivers_bkp = time_df['DriverNo'].copy()
            time_df = time_df.groupby('DriverNo').ffill()
            time_df['DriverNo'] = drivers_bkp

            freq_ms = 200
            pos_resampled = pos_df.groupby(['DriverNo', pd.Grouper(key='ts', freq=f'{freq_ms}ms')]).last().reset_index()
            pos_resampled = pos_resampled.sort_values('ts')

            merged_df = pd.merge_asof(
                pos_resampled, 
                time_df.sort_values('ts'), 
                on='ts', 
                by='DriverNo', 
                direction='backward',
                suffixes=('_pos', '_time')
            )

            if has_track_status:
                merged_df = pd.merge_asof(merged_df, track_status_df[['ts', 'Status']].sort_values('ts'), on='ts', direction='backward')

            merged_df['Position'] = merged_df['Position'].fillna(0).astype(int)
            merged_df['NumberOfLaps'] = merged_df['NumberOfLaps'].fillna(0).astype(int)
            merged_df['NumberOfPitStops'] = merged_df['NumberOfPitStops'].fillna(0).astype(int)
            
            all_drivers = pos_df['DriverNo'].unique()
            leader_id = str(all_drivers[0])
            try:
                grid = merged_df[merged_df['Position'] == 1].head(1)
                if not grid.empty: leader_id = str(grid.iloc[0]['DriverNo'])
            except: pass

            timestamps = merged_df['ts'].unique()
            last_leader_pos = None
            
            self._send_initial_data()
            print(f"🟢 START STREAMING ({len(timestamps)} frames)", flush=True)

            # base_sleep_time ya no se usa como espera fija, sino como referencia de la tasa de datos
            # base_sleep_time = freq_ms / 1000.0 
            
            tyre_q_idx = 0
            weather_q_idx = 0
            
            driver_best_laps = {}
            current_fastest_driver_global = None

            last_ts = None

            for ts in timestamps:
                if self.stop_event.is_set(): break
 
                # --- CÁLCULO DE ESPERA DINÁMICA (CORREGIDO) ---
                # Ahora solo dormimos lo necesario para cubrir el delta entre el frame anterior y este.
                # Se eliminó el sleep redundante al final del bucle.
                if last_ts is not None:
                    delta_seconds = (ts - last_ts).total_seconds()
                    wait_time = delta_seconds / self.simulation_speed
                    if wait_time > 0:
                        self.socketio.sleep(wait_time)
                
                last_ts = ts

                # --- PROCESAR COLAS ---
                while tyre_q_idx < len(tyre_queue):
                    evt = tyre_queue[tyre_q_idx]
                    if evt['ts'] <= ts:
                        self._update_tyre_cache([evt])
                        tyre_q_idx += 1
                    else: break
                
                while weather_q_idx < len(weather_queue):
                    evt = weather_queue[weather_q_idx]
                    if evt['ts'] <= ts:
                        self._update_weather_cache([evt])
                        weather_q_idx += 1
                    else: break

                frame = merged_df[merged_df['ts'] == ts]
                if frame.empty: continue
                
                frame_completed_laps = int(frame['NumberOfLaps'].max())
                
                current_flag = 'GREEN'
                if has_track_status and 'Status' in frame.columns:
                    current_flag = self.TRACK_STATUS_MAP.get(str(frame.iloc[0]['Status']), 'GREEN')
                if self.total_laps > 0 and frame_completed_laps >= self.total_laps:
                    current_flag = 'FINISHED'

                # LEER CLIMA DESDE CACHE
                weather_info = {}
                with self.weather_lock:
                    weather_info = self.weather_state_cache.copy()

                if 'BestLapTime_Value' in frame.columns:
                    for _, row in frame.iterrows():
                        d_no = str(row['DriverNo'])
                        time_sec = self._parse_lap_time(row['BestLapTime_Value'])
                        if time_sec > 0 and time_sec < float('inf'):
                            if d_no not in driver_best_laps or time_sec < driver_best_laps[d_no]:
                                driver_best_laps[d_no] = time_sec
                
                if driver_best_laps:
                    current_fastest_driver_global = min(driver_best_laps, key=driver_best_laps.get)

                leader_row = frame[frame['DriverNo'].astype(str) == leader_id]
                is_leader_moving = False
                if not leader_row.empty:
                    lx, ly = leader_row.iloc[0]['X'], leader_row.iloc[0]['Y']
                    if last_leader_pos:
                        if math.sqrt((lx - last_leader_pos[0])**2 + (ly - last_leader_pos[1])**2) > 2.0:
                            is_leader_moving = True
                    last_leader_pos = (lx, ly)

                if self.map_state == 'PRE_GRID' and is_leader_moving:
                    self.map_state = 'FORMATION'; self.raw_track_points = []; self._send_initial_data()
                elif self.map_state == 'FORMATION':
                    if not is_leader_moving:
                        self.stop_counter += 1
                        if self.stop_counter > 20: 
                            self.map_state = 'GRID_WAIT'; self.stop_counter=0; self.is_map_complete=True; self._send_initial_data()
                    else: self.stop_counter = 0
                elif self.map_state == 'GRID_WAIT' and is_leader_moving: self.map_state = 'RACE'
                
                if frame_completed_laps >= 1 and self.map_state != 'RACE': self.map_state = 'RACE'
                self.current_lap_display = frame_completed_laps + 1 if self.map_state == 'RACE' else 0
                if self.total_laps > 0 and self.current_lap_display > self.total_laps: self.current_lap_display = self.total_laps

                race_snapshot = []
                map_updated = False
                
                for _, row in frame.iterrows():
                    d_id = str(row['DriverNo'])
                    if d_id not in self.DRIVER_MAPPING: continue
                    
                    x_raw, y_raw = row['X'], row['Y']
                    laps_done = int(row['NumberOfLaps'])
                    
                    if (self.map_state == 'FORMATION' or (self.map_state == 'RACE' and laps_done<2)) and d_id == leader_id:
                        if self._update_live_map(x_raw, y_raw): map_updated = True
                    
                    x = self.normalize(x_raw, self.min_x, self.max_x)
                    y = self.normalize(y_raw, self.min_y, self.max_y, invert=True)
                    pos = int(row['Position'])
                    
                    status = "RUNNING"
                    if 'Retired' in row and (row['Retired'] == True or row['Retired'] == 1): status = "DNF"
                    elif pos == 0: status = "ON_GRID"
                    elif self.total_laps > 0 and laps_done >= self.total_laps: status = "FINISHED"

                    gap = row.get('IntervalToPositionAhead_Value', '')
                    if pd.isna(gap) or gap == '': gap = row.get('GapToLeader', '')
                    if pd.isna(gap): gap = ''

                    d_info = self.DRIVER_MAPPING.get(d_id, {'code': d_id, 'color': '#888'})
                    
                    compound = 'U'; tyre_life = '0'
                    with self.tyre_lock:
                        if d_id in self.tyre_state_cache:
                            compound = self.tyre_state_cache[d_id]['compound']
                            tyre_life = self.tyre_state_cache[d_id]['life']
                    
                    race_snapshot.append({
                        'd': d_info['code'], 'n': d_id, 'x': x, 'y': y, 'c': d_info['color'],
                        't': compound, 'l': tyre_life, 'p': pos, 'gap': str(gap),
                        'stops': int(row['NumberOfPitStops']), 'status': status, 
                        'lap': self.current_lap_display, 'fl': (d_id == current_fastest_driver_global)
                    })

                self.socketio.emit('race_data', {
                    'cars': race_snapshot, 'flag': current_flag, 
                    'total_laps': self.total_laps, 'weather': weather_info
                }, namespace='/race')
                
                if map_updated and self.is_map_complete: self._send_initial_data()
                
                # --- CORRECCIÓN IMPORTANTE: ELIMINADO EL SLEEP REDUNDANTE AQUÍ ---
                # Antes dormíamos 'wait_time' arriba Y 'base_sleep_time' aquí abajo.
                # Ahora solo dormimos arriba según el delta real.

        except Exception as e:
            print(f"❌ ERROR CRÍTICO EN SIMULACIÓN: {e}")
            traceback.print_exc()
        
        self.is_running = False
        print("🔴 FIN SESIÓN")

    def stop_service(self):
        self.stop_event.set()
        self.is_running = False
        return "Deteniendo servicio..."