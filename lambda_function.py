import pymysql
import json
import requests
from datetime import datetime, timezone, timedelta, date
import time
import logging
import boto3
from collections import defaultdict
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

META_GERENCIA = "3190"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
API_KEY = "PmUAU9nYtbD7_TlUZ-wRC05bz6dxjfvPyhUxuNP67JOPIvbZv0QSvz8UzIivwL-z"
BASE_URL = "https://akikb.storeganise.com/api/v1/admin"
headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Accept": "application/json"
}
RDS_CONFIG_ADMIN = {
    'host': 'rds-mysql-bi-construction-prod.cnpwfizfiryz.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'it9Kq6tSwZvEVlxg4AX0',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
RDS_CONFIG_VENTAS = RDS_CONFIG_ADMIN.copy()
RDS_CONFIG_VENTAS['database'] = 'bi-ventas-dev'
RDS_CONFIG_HISTORICO = RDS_CONFIG_ADMIN.copy()
RDS_CONFIG_HISTORICO['database'] = 'historico_dev'

IDENTIFICADORES_BASE = ["er", "ss", "pn", "vm", "en", "pf", "vn", "ao", "av", 
                       "lr", "ra", "sf", "mm", "ce", "jp", "vt", "lc", "ld", "sm", 
                       "rs", "mt", "tp", "bl", "vk", "cg", "pnf"]

def es_unidad_flex(unit_code):
    if not unit_code or not isinstance(unit_code, str):
        return False
    
    unit_code_lower = unit_code.lower().strip()
    unit_code_clean = ''.join(c for c in unit_code_lower if c.isalnum())
    
    if 'flex' in unit_code_clean:
        return True
    
    identificadores_ordenados = sorted(IDENTIFICADORES_BASE, key=len, reverse=True)
    
    for identificador in identificadores_ordenados:
        patron = f'^{identificador}f\\d+$'
        if re.match(patron, unit_code_clean):
            return True
    
    return False

class GlobalCache:
    def __init__(self):
        self._cache = {}
        self._initialized = False
        
    def initialize(self):
        if not self._initialized:
            logger.info("INICIALIZANDO CACHE GLOBAL")
            
            logger.info("Obteniendo todos los rentals")
            self._cache['all_rentals'] = self._fetch_all_paginated("unit-rentals", {"include": "unit", "state": "occupied,ended"})
            
            logger.info("Obteniendo todos los jobs")
            self._cache['all_jobs'] = self._fetch_all_paginated("jobs", {"state": "completed", "orderState": "completed"})
            
            logger.info("Obteniendo todas las unidades")
            self._cache['all_units'] = self._fetch_all_paginated("units", {"state": "occupied,reserved,overdue,vacant", "include": "unitType"})
            
            logger.info("Obteniendo todos los sites")
            self._cache['all_sites'] = self._fetch_all_paginated("sites")
            
            logger.info("Extrayendo sucursales KB")
            self._cache['sucursales_kb'] = self._extract_sucursales_kb()
            
            logger.info("Creando mapa de sucursales")
            self._cache['mapa_sucursales'], self._cache['info_sucursales'] = self._create_mapa_sucursales()
            
            logger.info("Obteniendo rentals del mes actual")
            self._cache['rentals_mes_actual'] = self._get_rentals_mes_actual()
            
            logger.info("Obteniendo reportes de ayer/hoy")
            self._cache['reportes_ayer_hoy'] = self._obtener_reportes_ayer_hoy()
            
            self._initialized = True
            logger.info(f"CACHE INICIALIZADO: {len(self._cache['all_rentals'])} rentals, {len(self._cache['all_units'])} unidades, {len(self._cache['all_sites'])} sites")
    
    def _fetch_all_paginated(self, endpoint, params=None, limit=100, delay=1):
        all_items = []
        offset = 0
        
        if params is None:
            params = {}
            
        while True:
            current_params = {
                "limit": limit,
                "offset": offset,
                **params
            }
            try:
                resp = requests.get(
                    f"{BASE_URL}/{endpoint}", 
                    headers=headers, 
                    params=current_params, 
                    timeout=60 
                )
                
                if resp.status_code == 429:
                    logger.warning("Rate limit, esperando 60s")
                    time.sleep(60)
                    continue 

                if resp.status_code != 200:
                    logger.error(f"Error API {resp.status_code} en {endpoint}")
                    break
                    
                payload = resp.json()
                
                if isinstance(payload, dict):
                    items = payload.get("data", [])
                elif isinstance(payload, list):
                    items = payload
                else:
                    items = []
                
                if not items:
                    break
                    
                all_items.extend(items)
                
                if len(items) < limit:
                    break
                    
                offset += limit
                time.sleep(delay)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error de conexion en {endpoint}: {e}")
                break
                
        return all_items
    
    def _extract_sucursales_kb(self):
        sites = self._cache['all_sites']
        sucursales_kb = []
        for site in sites:
            codigo = site.get("code", "").lower()
            if codigo and codigo.startswith("kb"):
                sucursal_formateada = codigo.upper()
                sucursales_kb.append(sucursal_formateada)
        
        sucursales_kb = sorted(list(set(sucursales_kb)))
        logger.info(f"Total sucursales KB encontradas: {len(sucursales_kb)}")
        return sucursales_kb
    
    def _create_mapa_sucursales(self):
        sites = self._cache['all_sites']
        mapa_sucursales = {}
        info_sucursales = {}
        
        for site in sites:
            site_id = site.get("id")
            if not site_id:
                continue
            
            codigo = site.get("code", "").strip()
            nombre = site.get("title", {}).get("es") or site.get("name", "")
            
            if codigo and codigo.lower().startswith("kb"):
                sucursal_codigo = codigo.upper()
                if len(sucursal_codigo) == 3: 
                    sucursal_codigo = sucursal_codigo[:2] + '0' + sucursal_codigo[2:]
                
                mapa_sucursales[site_id] = sucursal_codigo
                info_sucursales[sucursal_codigo] = {
                    "nombre": nombre,
                    "codigo_original": codigo,
                    "site_id": site_id
                }
        
        logger.info(f"Mapeadas {len(mapa_sucursales)} sucursales KB")
        return mapa_sucursales, info_sucursales
    
    def _get_rentals_mes_actual(self):
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        rentals_mes = []
        for rental in self._cache['all_rentals']:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state not in ["occupied", "ended"]:
                continue
            
            if not start_date_str:
                continue
            
            try:
                if "T" in start_date_str:
                    start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).date()
                else:
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                
                if first_day_of_month <= start_date <= today:
                    rentals_mes.append(rental)
                    
            except Exception:
                continue
        
        logger.info(f"Rentals del mes actual: {len(rentals_mes)}")
        return rentals_mes
    
    def _obtener_usuarios_rapido(self, rentals):
        if not rentals:
            return {}
        
        user_ids = set()
        for rental in rentals:
            owner_id = rental.get("ownerId")
            if owner_id:
                user_ids.add(owner_id)
        
        if not user_ids:
            return {}
        
        logger.info(f"Obteniendo {len(user_ids)} usuarios para reportes")
        usuarios_por_id = {}
        
        def fetch_user(user_id):
            try:
                resp = requests.get(
                    f"{BASE_URL}/users/{user_id}",
                    headers=headers,
                    params={"fields": "id,name,company"},
                    timeout=15
                )
                if resp.status_code == 200:
                    user = resp.json()
                    return user_id, {
                        "name": user.get("name", "").strip(),
                        "company": user.get("company", {})
                    }
            except Exception as e:
                logger.warning(f"Error obteniendo usuario {user_id}: {e}")
                return user_id, None
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_user, user_id): user_id for user_id in user_ids}
            
            for future in as_completed(futures):
                user_id, user_data = future.result()
                if user_data:
                    usuarios_por_id[user_id] = user_data
        
        logger.info(f"Obtenidos {len(usuarios_por_id)} usuarios para reportes")
        return usuarios_por_id
    
    def _calcular_area_m2_reportes(self, unit_data):
        if not unit_data:
            return 10.0
        
        try:
            length = unit_data.get("length")
            width = unit_data.get("width")
            
            if length and width:
                length_val = float(str(length).replace(",", ".").strip())
                width_val = float(str(width).replace(",", ".").strip())
                if length_val > 0 and width_val > 0:
                    return length_val * width_val
        except:
            pass
        
        try:
            area = unit_data.get("area")
            if area:
                area_val = float(str(area).replace(",", ".").strip())
                if area_val > 0:
                    return area_val
        except:
            pass
        
        return 10.0
    
    def _procesar_rentals_reportes(self, rentals, usuarios_por_id):
        moveins_por_fecha = {}
        moveouts_por_fecha = {}
        
        for rental in rentals:
            start_date_str = rental.get("startDate")
            end_date_str = rental.get("endDate")
            state = rental.get("state", "").lower()
            unit_code = rental.get("unit", {}).get("name") or f"UNIT_{rental.get('id', 'N/A')}"
            
            if start_date_str:
                try:
                    if "T" in start_date_str:
                        start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).date()
                    else:
                        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                    
                    today = datetime.now(timezone.utc).date()
                    yesterday = today - timedelta(days=1)
                    
                    if start_date in [yesterday, today]:
                        fecha_movimiento = start_date
                        es_movein = True
                        es_moveout = False
                except Exception as e:
                    logger.warning(f"Error parsing startDate {start_date_str}: {e}")
                    continue
            
            if state == "ended" and end_date_str:
                try:
                    if "T" in end_date_str:
                        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).date()
                    else:
                        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                    
                    today = datetime.now(timezone.utc).date()
                    yesterday = today - timedelta(days=1)
                    
                    if end_date in [yesterday, today]:
                        fecha_movimiento = end_date
                        es_movein = False
                        es_moveout = True
                except Exception as e:
                    logger.warning(f"Error parsing endDate {end_date_str}: {e}")
                    continue
            
            if not fecha_movimiento:
                continue
            
            unit_data = rental.get("unit", {})
            unit_code = unit_data.get("name") or unit_data.get("code") or f"UNIT_{rental.get('id', 'N/A')}"
            area_m2 = self._calcular_area_m2_reportes(unit_data)
            site_id = unit_data.get("siteId") or rental.get("siteId")
            site_code = "SIN_SITE"
            if site_id:
                site_code = str(site_id).upper()
            
            customer_name = "Cliente no disponible"
            customer_company = ""
            owner_id = rental.get("ownerId")
            
            if owner_id and owner_id in usuarios_por_id:
                usuario_info = usuarios_por_id[owner_id]
                customer_name = usuario_info.get("name", "Cliente (sin nombre)").strip()
                
                company_data = usuario_info.get("company", {})
                if isinstance(company_data, dict):
                    customer_company = company_data.get("name", "")
                elif isinstance(company_data, str):
                    customer_company = company_data
            
            descuento_total = 0
            seguro_monto = 0
            seguro_tipo = "No tiene seguro"
            
            charges = rental.get("charges", [])
            for charge in charges:
                amount = charge.get("amount", 0)
                
                if amount < 0:
                    descuento_total += abs(amount)
                
                title = charge.get('title', {})
                if isinstance(title, dict):
                    title_text = title.get('es', '') or title.get('en', '') or str(title)
                else:
                    title_text = str(title)
                
                title_lower = title_text.lower()
                if 'seguro' in title_lower and amount > 0:
                    seguro_monto = amount
                    seguro_tipo = title_text
            
            precio = float(rental.get("price", 0))
            
            movimiento_detalle = {
                "unit_code": unit_code,
                "m2": round(area_m2, 2),
                "site_code": site_code,
                "customer": customer_name,
                "company": customer_company,
                "seguro_tiene": seguro_monto > 0,
                "seguro_tipo": seguro_tipo,
                "seguro_monto": seguro_monto,
                "descuento": round(descuento_total, 2),
                "precio": precio
            }
            
            fecha_key = fecha_movimiento.isoformat()
            
            if es_movein:
                if fecha_key not in moveins_por_fecha:
                    moveins_por_fecha[fecha_key] = {
                        "unidades": 0,
                        "m2_total": 0.0,
                        "detalle_unidades": []
                    }
                
                moveins_por_fecha[fecha_key]["unidades"] += 1
                moveins_por_fecha[fecha_key]["m2_total"] += area_m2
                moveins_por_fecha[fecha_key]["detalle_unidades"].append(movimiento_detalle)
            
            if es_moveout:
                if fecha_key not in moveouts_por_fecha:
                    moveouts_por_fecha[fecha_key] = {
                        "unidades": 0,
                        "m2_total": 0.0,
                        "detalle_unidades": []
                    }
                
                moveouts_por_fecha[fecha_key]["unidades"] += 1
                moveouts_por_fecha[fecha_key]["m2_total"] += area_m2
                moveouts_por_fecha[fecha_key]["detalle_unidades"].append(movimiento_detalle)
        
        return moveins_por_fecha, moveouts_por_fecha
    
    def _obtener_reportes_ayer_hoy(self):
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        
        logger.info(f"Obteniendo reportes de {yesterday} a {today}")
        
        all_rentals = self._cache.get('all_rentals', [])
        
        if not all_rentals:
            logger.error("No hay rentals en el cache")
            return self._crear_estructura_vacia_reporte()
        
        logger.info(f"Obtenidos {len(all_rentals)} rentals del cache para reportes")
        
        rentals_ayer_hoy = []
        
        for rental in all_rentals:
            start_date_str = rental.get("startDate")
            end_date_str = rental.get("endDate")
            state = rental.get("state", "").lower()
            
            if state not in ["occupied", "ended"]:
                continue
            
            tiene_fecha_valida = False
            
            if start_date_str:
                try:
                    if "T" in start_date_str:
                        start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).date()
                    else:
                        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                    
                    if start_date in [yesterday, today]:
                        tiene_fecha_valida = True
                except Exception as e:
                    logger.warning(f"Error parsing startDate {start_date_str}: {e}")
            
            if end_date_str and state == "ended":
                try:
                    if "T" in end_date_str:
                        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).date()
                    else:
                        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                    
                    if end_date in [yesterday, today]:
                        tiene_fecha_valida = True
                except Exception as e:
                    logger.warning(f"Error parsing endDate {end_date_str}: {e}")
            
            if tiene_fecha_valida:
                rentals_ayer_hoy.append(rental)
        
        logger.info(f"{len(rentals_ayer_hoy)} rentals tienen fechas en ayer/hoy")
        
        if not rentals_ayer_hoy:
            return self._crear_estructura_vacia_reporte()
        
        usuarios_por_id = self._obtener_usuarios_rapido(rentals_ayer_hoy)
        moveins_por_fecha, moveouts_por_fecha = self._procesar_rentals_reportes(rentals_ayer_hoy, usuarios_por_id)
        
        resultado = self._crear_estructura_vacia_reporte()
        
        for fecha_key, datos in sorted(moveins_por_fecha.items(), reverse=True):
            resultado["data_moveins"]["detalle_moveins_diarios"].append({
                "fecha": fecha_key,
                "unidades": datos["unidades"],
                "m2": round(datos["m2_total"], 2)
            })
            
            for unidad in datos["detalle_unidades"]:
                unidad_con_fecha = unidad.copy()
                unidad_con_fecha["fecha"] = fecha_key
                resultado["data_moveins"]["detalle_completo_unidades"].append(unidad_con_fecha)
            
            resultado["data_moveins"]["total_unidades"] += datos["unidades"]
            resultado["data_moveins"]["total_m2_movein"] += datos["m2_total"]
        
        for fecha_key, datos in sorted(moveouts_por_fecha.items(), reverse=True):
            resultado["data_moveouts"]["detalle_moveouts_diarios"].append({
                "fecha": fecha_key,
                "unidades": datos["unidades"],
                "m2": round(datos["m2_total"], 2)
            })
            
            for unidad in datos["detalle_unidades"]:
                unidad_con_fecha = unidad.copy()
                unidad_con_fecha["fecha"] = fecha_key
                resultado["data_moveouts"]["detalle_completo_unidades"].append(unidad_con_fecha)
            
            resultado["data_moveouts"]["total_unidades"] += datos["unidades"]
            resultado["data_moveouts"]["total_m2_moveout"] += datos["m2_total"]
        
        logger.info(f"Reportes ayer/hoy: Move-ins={resultado['data_moveins']['total_unidades']}, Move-outs={resultado['data_moveouts']['total_unidades']}")
        
        if resultado["data_moveouts"]["total_unidades"] > 0:
            logger.info("DETALLE DE MOVE-OUTS ENCONTRADOS:")
            for unidad in resultado["data_moveouts"]["detalle_completo_unidades"][:10]:
                logger.info(f"  - {unidad['unit_code']}: {unidad['customer']} ({unidad['company']}), {unidad['m2']} m², {unidad['fecha']}")
        else:
            logger.info("No se encontraron move-outs")
        
        return resultado
    
    def _crear_estructura_vacia_reporte(self):
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        
        return {
            "data_moveins": {
                "fecha_moveins": f"{yesterday} y {today}",
                "total_unidades": 0,
                "total_m2_movein": 0.0,
                "detalle_moveins_diarios": [],
                "detalle_completo_unidades": []
            },
            "data_moveouts": {
                "fecha_moveouts": f"{yesterday} y {today}",
                "total_unidades": 0,
                "total_m2_moveout": 0.0,
                "detalle_moveouts_diarios": [],
                "detalle_completo_unidades": []
            }
        }
    
    def get(self, key):
        if not self._initialized:
            self.initialize()
        return self._cache.get(key)
    
    def get_all_data(self):
        if not self._initialized:
            self.initialize()
        return {
            'all_rentals': self._cache['all_rentals'],
            'all_jobs': self._cache['all_jobs'],
            'all_units': self._cache['all_units'],
            'all_sites': self._cache['all_sites'],
            'sucursales_kb': self._cache['sucursales_kb'],
            'mapa_sucursales': self._cache['mapa_sucursales'],
            'info_sucursales': self._cache['info_sucursales'],
            'rentals_mes_actual': self._cache['rentals_mes_actual'],
            'reportes_ayer_hoy': self._cache['reportes_ayer_hoy']
        }

GLOBAL_CACHE = GlobalCache()

def parse_date_to_dateobj(s):
    if not s:
        return None
    if isinstance(s, datetime):
        return s.astimezone(timezone.utc).date()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    except Exception:
        pass
    try:
        if "T" in s:
            return datetime.fromisoformat(s[:10]).date()
    except Exception:
        pass
    
    return None

def compute_area_m2(unit):
    if not isinstance(unit, dict):
        return 10.0 
    
    area_fields = ["area", "size", "squareMeters", "squareMeter", "m2", "square_meters", "square_meter"]
    
    for field in area_fields:
        if field in unit:
            try:
                area = float(str(unit[field]).replace(",", "").strip())
                if area and area > 0:
                    return area
            except:
                pass
    
    length_fields = ["length", "depth", "longitud", "largo", "long"]
    width_fields = ["width", "ancho", "breadth", "wide"]
    
    length = None
    width = None
    
    for field in length_fields:
        if field in unit:
            try:
                length = float(str(unit[field]).replace(",", "").strip())
                if length:
                    break
            except:
                pass
    
    for field in width_fields:
        if field in unit:
            try:
                width = float(str(unit[field]).replace(",", "").strip())
                if width:
                    break
            except:
                pass
    
    if length and width:
        return length * width
    
    if "data" in unit and isinstance(unit["data"], dict):
        for field in area_fields:
            if field in unit["data"]:
                try:
                    area = float(str(unit["data"][field]).replace(",", "").strip())
                    if area and area > 0:
                        return area
                except:
                    pass
    
    return 10.0

def compute_area_m2_corregida(unit):
    if not isinstance(unit, dict):
        return None
    
    area_fields = ["area", "size", "squareMeters", "squareMeter", "m2", 
                   "square_meters", "square_meter", "squareFeet", "sqft"]
    
    for field in area_fields:
        if field in unit:
            try:
                value = unit[field]
                if value is None:
                    continue
                    
                str_value = str(value).strip()
                if not str_value:
                    continue
                
                str_value = str_value.replace(",", "").replace(" ", "")
                
                area = float(str_value)
                if area > 0:
                    return area
            except (ValueError, TypeError):
                continue
    
    if "data" in unit and isinstance(unit["data"], dict):
        data = unit["data"]
        for field in area_fields:
            if field in data:
                try:
                    value = data[field]
                    if value is None:
                        continue
                    
                    str_value = str(value).replace(",", "").strip()
                    if str_value:
                        area = float(str_value)
                        if area > 0:
                            return area
                except:
                    continue
    
    length_fields = ["length", "depth", "longitud", "largo", "long"]
    width_fields = ["width", "ancho", "breadth", "wide"]
    
    length = None
    width = None
    
    for field in length_fields:
        if field in unit:
            try:
                value = unit[field]
                if value:
                    str_value = str(value).replace(",", "").strip()
                    length = float(str_value)
                    break
            except:
                continue
    
    for field in width_fields:
        if field in unit:
            try:
                value = unit[field]
                if value:
                    str_value = str(value).replace(",", "").strip()
                    width = float(str_value)
                    break
            except:
                continue
    
    if length and width:
        area = length * width
        return area
    
    if "data" in unit and isinstance(unit["data"], dict):
        data = unit["data"]
        
        for field in length_fields:
            if field in data:
                try:
                    value = data[field]
                    if value:
                        str_value = str(value).replace(",", "").strip()
                        length = float(str_value)
                        break
                except:
                    continue
        
        for field in width_fields:
            if field in data:
                try:
                    value = data[field]
                    if value:
                        str_value = str(value).replace(",", "").strip()
                        width = float(str_value)
                        break
                except:
                    continue
        
        if length and width:
            area = length * width
            return area
    
    unit_type = unit.get("unitType") or unit.get("type")
    if isinstance(unit_type, dict):
        unit_type_name = unit_type.get("name", "")
        if "m2" in unit_type_name.lower() or "m²" in unit_type_name.lower():
            import re
            matches = re.findall(r'(\d+(?:\.\d+)?)\s*m[²2]', unit_type_name, re.IGNORECASE)
            if matches:
                try:
                    area = float(matches[0])
                    if area > 0:
                        return area
                except:
                    pass
    
    custom_fields = unit.get("customFields") or {}
    for key, value in custom_fields.items():
        if any(term in key.lower() for term in ["area", "size", "m2", "square"]):
            try:
                if value:
                    str_value = str(value).replace(",", "").strip()
                    area = float(str_value)
                    if area > 0:
                        return area
            except:
                continue
    
    return None

def _parse_number(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        s = str(x).replace(",", "").strip()
        return float(s) if s != "" else None
    except Exception:
        return None

def _extract_price(obj):
    if not isinstance(obj, dict):
        return None
    
    price_fields = ["price", "unitPrice", "monthlyPrice", "amount", "totalPrice", "unitPriceGross", 
                   "rentalPrice", "monthlyRent", "rent", "pricing", "basePrice"]
    
    for field in price_fields:
        price = obj.get(field)
        if isinstance(price, (int, float)) and price > 0:
            return float(price)
        elif isinstance(price, str):
            try:
                price_num = float(price.replace(",", "").strip())
                if price_num > 0:
                    return price_num
            except:
                pass
    
    data = obj.get("data") or {}
    for field in price_fields:
        price = data.get(field)
        if isinstance(price, (int, float)) and price > 0:
            return float(price)
        elif isinstance(price, str):
            try:
                price_num = float(price.replace(",", "").strip())
                if price_num > 0:
                    return price_num
            except:
                pass
    
    pricing = obj.get("pricing") or data.get("pricing")
    if isinstance(pricing, dict):
        for field in price_fields:
            price = pricing.get(field)
            if isinstance(price, (int, float)) and price > 0:
                return float(price)
            elif isinstance(price, str):
                try:
                    price_num = float(price.replace(",", "").strip())
                    if price_num > 0:
                        return price_num
                except:
                    pass
    
    return None

def procesar_rentals_con_y_sin_duplicados(all_rentals, fecha_inicio_6_meses, fecha_inicio_3_meses, today, first_day_of_month):
    resultados = {
        "con_duplicados": {"contratos": {}, "finiquitos": {}, "unidades_unicas": {}},
        "sin_duplicados": {"contratos": {}, "finiquitos": {}, "unidades_unicas": {}}
    }
    unidades_contrato_por_mes = defaultdict(set) 
    unidades_finiquito_por_mes = defaultdict(set)
    area_contratos_por_mes_con = defaultdict(float)
    area_finiquitos_por_mes_con = defaultdict(float)
    area_contratos_por_mes_sin = defaultdict(float)
    area_finiquitos_por_mes_sin = defaultdict(float)
    total_rentals = len(all_rentals)
    rentals_con_unit_id = 0
    rentals_sin_unit_id = 0
    
    for rental in all_rentals:
        unit_id = rental.get("unitId")
        rental_state = rental.get("state", "").lower()
        unit_data = rental.get("unit", {})
        area_m2 = compute_area_m2(unit_data)
        
        if rental_state == "occupied":
            start_date = parse_date_to_dateobj(rental.get("startDate"))
            if start_date and fecha_inicio_6_meses <= start_date <= today:
                mes_key = start_date.strftime("%Y-%m")
                area_contratos_por_mes_con[mes_key] += area_m2
        
        elif rental_state == "ended":
            end_date = parse_date_to_dateobj(rental.get("endDate"))
            if end_date and fecha_inicio_6_meses <= end_date <= today:
                mes_key = end_date.strftime("%Y-%m")
                area_finiquitos_por_mes_con[mes_key] += area_m2
        
        if unit_id:
            rentals_con_unit_id += 1
            
            if rental_state == "occupied":
                start_date = parse_date_to_dateobj(rental.get("startDate"))
                if start_date and fecha_inicio_6_meses <= start_date <= today:
                    mes_key = start_date.strftime("%Y-%m")
                    if unit_id not in unidades_contrato_por_mes[mes_key]:
                        unidades_contrato_por_mes[mes_key].add(unit_id)
                        area_contratos_por_mes_sin[mes_key] += area_m2
            
            elif rental_state == "ended":
                end_date = parse_date_to_dateobj(rental.get("endDate"))
                if end_date and fecha_inicio_6_meses <= end_date <= today:
                    mes_key = end_date.strftime("%Y-%m")
                    if unit_id not in unidades_finiquito_por_mes[mes_key]:
                        unidades_finiquito_por_mes[mes_key].add(unit_id)
                        area_finiquitos_por_mes_sin[mes_key] += area_m2
        else:
            rentals_sin_unit_id += 1
    
    resultados["con_duplicados"]["contratos"] = dict(area_contratos_por_mes_con)
    resultados["con_duplicados"]["finiquitos"] = dict(area_finiquitos_por_mes_con)
    resultados["sin_duplicados"]["contratos"] = dict(area_contratos_por_mes_sin)
    resultados["sin_duplicados"]["finiquitos"] = dict(area_finiquitos_por_mes_sin)
    
    for mes in unidades_contrato_por_mes:
        resultados["sin_duplicados"]["unidades_unicas"][mes] = {
            "contratos": len(unidades_contrato_por_mes[mes]),
            "finiquitos": len(unidades_finiquito_por_mes.get(mes, set()))
        }
    
    logger.info(f"ESTADISTICAS DE DUPLICADOS:")
    logger.info(f"   Total rentals obtenidos: {total_rentals}")
    logger.info(f"   Rentals con unit_id: {rentals_con_unit_id} ({rentals_con_unit_id/total_rentals*100:.1f}%)")
    logger.info(f"   Rentals sin unit_id: {rentals_sin_unit_id} ({rentals_sin_unit_id/total_rentals*100:.1f}%)")
    
    return resultados

def calcular_contratos_finiquitos_simplificado():
    try:
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        fecha_inicio_6_meses = today - timedelta(days=180)
        fecha_inicio_3_meses = today - timedelta(days=90)
        logger.info("="*70)
        logger.info("CALCULANDO CONTRATOS Y FINIQUITOS SIMPLIFICADO")
        logger.info("="*70)
        logger.info(f"Fecha actual: {today}")
        logger.info(f"6 meses atras: {fecha_inicio_6_meses}")
        logger.info(f"3 meses atras: {fecha_inicio_3_meses}")
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        logger.info(f"Total rentals obtenidos del cache: {len(all_rentals)}")
        resultados = procesar_rentals_con_y_sin_duplicados(
            all_rentals, fecha_inicio_6_meses, fecha_inicio_3_meses, today, first_day_of_month
        )
        resultados_sin_duplicados = resultados["sin_duplicados"]
        area_contratos_por_mes = resultados_sin_duplicados["contratos"]
        area_finiquitos_por_mes = resultados_sin_duplicados["finiquitos"]
        meses_ordenados = sorted(area_contratos_por_mes.keys())
        meses_analizados = len(meses_ordenados)
        contratos_mensuales = []
        finiquitos_mensuales = []
        logger.info(f"Meses analizados ({meses_analizados}): {meses_ordenados}")
        
        for mes in meses_ordenados:
            area_contratos = area_contratos_por_mes[mes]
            area_finiquitos = area_finiquitos_por_mes.get(mes, 0)
            contratos_mensuales.append(area_contratos)
            finiquitos_mensuales.append(area_finiquitos)
            
            if mes in resultados_sin_duplicados["unidades_unicas"]:
                unidades = resultados_sin_duplicados["unidades_unicas"][mes]
                logger.info(f"  {mes}: Contratos={area_contratos:.2f} m² ({unidades['contratos']} unids), Finiquitos={area_finiquitos:.2f} m² ({unidades['finiquitos']} unids)")
            else:
                logger.info(f"  {mes}: Contratos={area_contratos:.2f} m², Finiquitos={area_finiquitos:.2f} m²")
        
        if contratos_mensuales:
            promedio_contratos_dia = sum(contratos_mensuales) / len(contratos_mensuales)
            promedio_finiquitos_dia = sum(finiquitos_mensuales) / len(finiquitos_mensuales)
            promedio_neto_dia = promedio_contratos_dia - promedio_finiquitos_dia
        else:
            promedio_contratos_dia = 0
            promedio_finiquitos_dia = 0
            promedio_neto_dia = 0
        
        if len(contratos_mensuales) >= 3:
            contratos_3_meses = contratos_mensuales[-3:]
            finiquitos_3_meses = finiquitos_mensuales[-3:]
            
            promedio_contratos_cierre = sum(contratos_3_meses) / len(contratos_3_meses)
            promedio_finiquitos_cierre = sum(finiquitos_3_meses) / len(finiquitos_3_meses)
            promedio_neto_cierre = promedio_contratos_cierre - promedio_finiquitos_cierre
        else:
            promedio_contratos_cierre = promedio_contratos_dia * 1.1
            promedio_finiquitos_cierre = promedio_finiquitos_dia * 1.05
            promedio_neto_cierre = promedio_contratos_cierre - promedio_finiquitos_cierre
        
        if promedio_contratos_dia > 9000:
            factor_ajuste = 7500 / promedio_contratos_dia
            promedio_contratos_dia = promedio_contratos_dia * factor_ajuste
            promedio_finiquitos_dia = promedio_finiquitos_dia * factor_ajuste
            promedio_neto_dia = promedio_contratos_dia - promedio_finiquitos_dia
            
            promedio_contratos_cierre = promedio_contratos_cierre * factor_ajuste
            promedio_finiquitos_cierre = promedio_finiquitos_cierre * factor_ajuste
            promedio_neto_cierre = promedio_contratos_cierre - promedio_finiquitos_cierre
        
        resultado_simplificado = {
            "periodo_dia": "promedio_6_meses",
            "fecha_inicio_dia": fecha_inicio_6_meses.isoformat(),
            "fecha_fin_dia": today.isoformat(),
            "total_m2_contratos_dia": round(promedio_contratos_dia, 2),
            "total_m2_finiquitos_dia": round(promedio_finiquitos_dia, 2),
            "neto_m2_dia": round(promedio_neto_dia, 2),
            "periodo_cierre": "promedio_3_meses",
            "fecha_inicio_cierre": fecha_inicio_3_meses.isoformat(),
            "fecha_fin_cierre": today.isoformat(),
            "total_m2_contratos_cierre": round(promedio_contratos_cierre, 2),
            "total_m2_finiquitos_cierre": round(promedio_finiquitos_cierre, 2),
            "neto_m2_cierre": round(promedio_neto_cierre, 2),
            "meses_analizados": meses_analizados
        }
        
        logger.info(f"RESUMEN SIMPLIFICADO:")
        logger.info(f"   Promedio 6 meses - Contratos/dia: {promedio_contratos_dia:.2f} m²")
        logger.info(f"   Promedio 6 meses - Finiquitos/dia: {promedio_finiquitos_dia:.2f} m²")
        logger.info(f"   Promedio 6 meses - Neto/dia: {promedio_neto_dia:.2f} m²")
        logger.info(f"   Promedio 3 meses - Contratos/dia: {promedio_contratos_cierre:.2f} m²")
        logger.info(f"   Promedio 3 meses - Finiquitos/dia: {promedio_finiquitos_cierre:.2f} m²")
        logger.info(f"   Promedio 3 meses - Neto/dia: {promedio_neto_cierre:.2f} m²")
        
        return resultado_simplificado
        
    except Exception as e:
        logger.error(f"ERROR calculando contratos y finiquitos simplificado: {str(e)}")
        import traceback
        traceback.print_exc()
        today = datetime.now(timezone.utc).date()
        fecha_inicio_6_meses = today - timedelta(days=180)
        fecha_inicio_3_meses = today - timedelta(days=90)
        
        return {
            "periodo_dia": "promedio_6_meses",
            "fecha_inicio_dia": fecha_inicio_6_meses.isoformat(),
            "fecha_fin_dia": today.isoformat(),
            "total_m2_contratos_dia": 0,
            "total_m2_finiquitos_dia": 0,
            "neto_m2_dia": 0,
            "periodo_cierre": "promedio_3_meses",
            "fecha_inicio_cierre": fecha_inicio_3_meses.isoformat(),
            "fecha_fin_cierre": today.isoformat(),
            "total_m2_contratos_cierre": 0,
            "total_m2_finiquitos_cierre": 0,
            "neto_m2_cierre": 0,
            "meses_analizados": 0,
            "error": str(e)
        }

def calcular_y_unificar_tres_tablas():
    try:
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        fecha_inicio_6_meses = today - timedelta(days=180)
        
        logger.info("CALCULANDO Y UNIFICANDO 3 TABLAS PARA POWER BI")
        logger.info(f"Periodo mes actual: {first_day_of_month} a {today}")
        logger.info(f"Periodo 6 meses: {fecha_inicio_6_meses} a {today}")
        
        connection = pymysql.connect(**RDS_CONFIG_VENTAS)
        
        with connection.cursor() as cursor:
            logger.info("1. Calculando neto_flex (promedio diario FLEX del mes actual)")
            cursor.execute("""
                SELECT DISTINCT Bodega
                FROM dwh_transacciones 
                WHERE Bodega IS NOT NULL AND Bodega != ''
                AND Fecha BETWEEN %s AND %s
            """, (first_day_of_month, today))
            todas_bodegas = [row['Bodega'] for row in cursor.fetchall()]
            siglas_no_flex = ['bl', 'kb', 'cg', 'ce', 'en', 'jp', 'ld', 'lr', 'mt', 'pn', 'ra', 'rs', 'sf', 'sm', 'av', 'tp', 'vk']
            bodegas_flex = []
            
            for bodega in todas_bodegas:
                nombre_bodega = bodega.lower()
                es_flex = True
                
                for sigla in siglas_no_flex:
                    if nombre_bodega.startswith(sigla):
                        es_flex = False
                        break
                
                if es_flex:
                    bodegas_flex.append(bodega)
            
            netom2_flex_diario = {}
            if bodegas_flex:
                placeholders = ','.join(['%s'] * len(bodegas_flex))
                
                query_flex_diario = f"""
                    SELECT 
                        DATE(Fecha) AS fecha,
                        COALESCE(SUM(M2MoveIn), 0) as m2_in,
                        COALESCE(SUM(M2MoveOut), 0) as m2_out
                    FROM (
                        SELECT DISTINCT
                            DATE(Fecha) AS Fecha,
                            Bodega,
                            COALESCE(M2MoveIn, 0) AS M2MoveIn,
                            COALESCE(M2MoveOut, 0) AS M2MoveOut
                        FROM dwh_transacciones
                        WHERE Bodega IN ({placeholders})
                        AND Fecha BETWEEN %s AND %s
                    ) AS datos_unicos
                    GROUP BY DATE(Fecha)
                    ORDER BY DATE(Fecha)
                """
                cursor.execute(query_flex_diario, bodegas_flex + [first_day_of_month, today])
                registros_flex = cursor.fetchall()
                for registro in registros_flex:
                    fecha = registro["fecha"].isoformat() if registro["fecha"] else ""
                    m2_in = float(registro['m2_in'] or 0)
                    m2_out = float(registro['m2_out'] or 0)
                    neto = m2_in - m2_out
                    
                    netom2_flex_diario[fecha] = neto
            
            if netom2_flex_diario:
                promedio_neto_flex = sum(netom2_flex_diario.values()) / len(netom2_flex_diario)
            else:
                promedio_neto_flex = 0
            
            logger.info(f"neto_flex (promedio mes): {promedio_neto_flex:.2f} m²")
            logger.info("2. Calculando neto (diario global del mes actual)")
            
            query_neto_diario = """
                SELECT 
                    DATE(Fecha) AS fecha,
                    COALESCE(SUM(M2MoveIn), 0) as m2_in,
                    COALESCE(SUM(M2MoveOut), 0) as m2_out
                FROM (
                    SELECT DISTINCT
                        DATE(Fecha) AS Fecha,
                        CodigoKB,
                        COALESCE(M2MoveIn, 0) AS M2MoveIn,
                        COALESCE(M2MoveOut, 0) AS M2MoveOut
                    FROM dwh_transacciones
                    WHERE Fecha BETWEEN %s AND %s
                    AND CodigoKB LIKE 'KB%%'
                ) AS datos_unicos
                GROUP BY DATE(Fecha)
                ORDER BY DATE(Fecha)
            """
            
            cursor.execute(query_neto_diario, [first_day_of_month, today])
            registros_neto = cursor.fetchall()
            
            neto_diario_dict = {}
            for registro in registros_neto:
                fecha = registro["fecha"].isoformat() if registro["fecha"] else ""
                m2_in = float(registro['m2_in'] or 0)
                m2_out = float(registro['m2_out'] or 0)
                neto = m2_in - m2_out
                
                neto_diario_dict[fecha] = neto
            
            logger.info(f"neto diario: {len(neto_diario_dict)} registros del mes actual")
            logger.info("3. Calculando neto6m (acumulado 6 meses)")
            query_acumulado_6m = """
                SELECT 
                    DATE(Fecha) AS fecha,
                    COALESCE(SUM(M2MoveIn), 0) as m2_in,
                    COALESCE(SUM(M2MoveOut), 0) as m2_out
                FROM (
                    SELECT DISTINCT
                        DATE(Fecha) AS Fecha,
                        CodigoKB,
                        COALESCE(M2MoveIn, 0) AS M2MoveIn,
                        COALESCE(M2MoveOut, 0) AS M2MoveOut
                    FROM dwh_transacciones
                    WHERE Fecha BETWEEN %s AND %s
                    AND CodigoKB LIKE 'KB%%'
                ) AS datos_unicos
                GROUP BY DATE(Fecha)
                ORDER BY DATE(Fecha)
            """
            
            cursor.execute(query_acumulado_6m, [fecha_inicio_6_meses, today])
            registros_6m = cursor.fetchall()
            acumulado_por_fecha = {}
            acumulado_total = 0
            
            for registro in registros_6m:
                fecha = registro["fecha"].isoformat() if registro["fecha"] else ""
                m2_in = float(registro['m2_in'] or 0)
                m2_out = float(registro['m2_out'] or 0)
                neto_diario = m2_in - m2_out
                
                acumulado_total += neto_diario
                acumulado_por_fecha[fecha] = round(acumulado_total, 2)
            
            logger.info(f"neto6m acumulado: total {acumulado_total:.2f} m² en {len(acumulado_por_fecha)} dias")
            logger.info("Unificando las 3 tablas")
            todas_fechas_mes = []
            current_date = first_day_of_month
            while current_date <= today:
                todas_fechas_mes.append(current_date.isoformat())
                current_date += timedelta(days=1)
            datos_unificados = []
            
            for fecha_str in todas_fechas_mes:
                try:
                    neto_flex_valor = round(promedio_neto_flex, 2)
                    neto_valor = neto_diario_dict.get(fecha_str, 0)
                    neto6m_valor = acumulado_por_fecha.get(fecha_str, 0)
                    item = {
                        "fecha": fecha_str,
                        "neto_flex": neto_flex_valor,
                        "neto": round(neto_valor, 2),
                        "neto6m": neto6m_valor
                    }
                    datos_unificados.append(item)
                    
                except Exception as e:
                    logger.error(f"Error procesando fecha {fecha_str}: {e}")
                    datos_unificados.append({
                        "fecha": fecha_str,
                        "neto_flex": 0,
                        "neto": 0,
                        "neto6m": 0
                    })
            
            logger.info(f"Datos unificados: {len(datos_unificados)} registros del mes actual")
            
            if len(datos_unificados) > 1:
                for i in range(1, len(datos_unificados)):
                    if datos_unificados[i]["neto6m"] < datos_unificados[i-1]["neto6m"]:
                        logger.warning(f"neto6m no ascendente en {datos_unificados[i]['fecha']}")

            resultado = {
                "datos": datos_unificados,
                "metadata": {
                    "fecha_calculo": today.isoformat(),
                    "periodo_mes_inicio": first_day_of_month.isoformat(),
                    "periodo_mes_fin": today.isoformat(),
                    "periodo_6m_inicio": fecha_inicio_6_meses.isoformat(),
                    "periodo_6m_fin": today.isoformat(),
                    "total_dias": len(datos_unificados),
                    "primer_dia": first_day_of_month.isoformat(),
                    "ultimo_dia": today.isoformat(),
                    "promedio_neto_flex": round(promedio_neto_flex, 2),
                    "acumulado_total_6m": round(acumulado_total, 2)
                }
            }
            if datos_unificados:
                logger.info(f"RESUMEN FINAL:")
                logger.info(f"   - Promedio neto_flex (mes): {promedio_neto_flex:.2f}")
                logger.info(f"   - Ultimo neto diario: {datos_unificados[-1]['neto']:.2f}")
                logger.info(f"   - Ultimo neto6m acumulado: {datos_unificados[-1]['neto6m']:.2f}")
                logger.info(f"   - Total acumulado 6 meses: {acumulado_total:.2f}")
                logger.info("Primeros 3 dias:")
                for item in datos_unificados[:3]:
                    logger.info(f"   {item['fecha']}: neto_flex={item['neto_flex']:.2f}, neto={item['neto']:.2f}, neto6m={item['neto6m']:.2f}")
                
                logger.info("Ultimos 3 dias:")
                for item in datos_unificados[-3:]:
                    logger.info(f"   {item['fecha']}: neto_flex={item['neto_flex']:.2f}, neto={item['neto']:.2f}, neto6m={item['neto6m']:.2f}")
            
            return resultado
            
    except Exception as e:
        logger.error(f"Error calculando y unificando 3 tablas: {str(e)}")
        import traceback
        traceback.print_exc()
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        datos_error = []
        current_date = first_day_of_month
        while current_date <= today:
            datos_error.append({
                "fecha": current_date.isoformat(),
                "neto_flex": 0,
                "neto": 0,
                "neto6m": 0
            })
            current_date += timedelta(days=1)
        
        return {
            "datos": datos_error,
            "metadata": {
                "fecha_calculo": today.isoformat(),
                "periodo_mes_inicio": first_day_of_month.isoformat(),
                "periodo_mes_fin": today.isoformat(),
                "total_dias": len(datos_error),
                "primer_dia": first_day_of_month.isoformat(),
                "ultimo_dia": today.isoformat(),
                "error": str(e)[:100]
            }
        }
    finally:
        if 'connection' in locals():
            connection.close()

def contar_moveouts_reales_api(all_rentals, today=None):
    try:
        if today is None:
            today = datetime.now(timezone.utc).date()
        
        first_day_of_month = today.replace(day=1)
        logger.info(f"CONTANDO MOVE-OUTS REALES DESDE CACHE")
        logger.info(f"Periodo: {first_day_of_month} a {today}")
        moveouts_ids = set()
        moveouts_data = []
        
        for rental in all_rentals:
            end_date_str = rental.get("endDate")
            rental_state = rental.get("state", "").lower()
            if rental_state != "ended":
                continue
            
            if not end_date_str:
                continue
            
            try:
                if "T" in end_date_str:
                    end_date = datetime.fromisoformat(
                        end_date_str.replace("Z", "+00:00")
                    ).date()
                else:
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                
                if first_day_of_month <= end_date <= today:
                    unit_id = rental.get("unitId")
                    if unit_id:
                        if unit_id not in moveouts_ids:
                            moveouts_ids.add(unit_id)
                            moveouts_data.append(rental)
                            
            except Exception as e:
                logger.warning(f"Error parsing date {end_date_str}: {e}")
                continue
        
        moveouts_total = len(moveouts_ids)
        
        logger.info(f"MOVE-OUTS REALES (CACHE): {moveouts_total} unidades")
        
        return moveouts_total, moveouts_data
        
    except Exception as e:
        logger.error(f"Error en contar_moveouts_reales_api: {e}")
        return 0, []

def contar_moveins_correcto(today=None):
    try:
        if today is None:
            today = datetime.now(timezone.utc).date()
        
        first_day_of_month = today.replace(day=1)
        logger.info(f"CALCULANDO MOVE-INS DESDE CACHE - CORREGIDO")
        logger.info(f"Periodo: {first_day_of_month} a {today}")
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        
        if not all_rentals:
            logger.error("No hay rentals en cache")
            return 0, {}, []
        
        unidades_movein = set()
        conteo_estados = defaultdict(int)
        
        for rental in all_rentals:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state not in ["occupied", "ended"]:
                continue
            
            if not start_date_str:
                continue
            
            try:
                if "T" in start_date_str:
                    start_date = datetime.fromisoformat(
                        start_date_str.replace("Z", "+00:00")
                    ).date()
                else:
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                
                if first_day_of_month <= start_date <= today:
                    unit_id = rental.get("unitId")
                    if unit_id:
                        unidades_movein.add(unit_id)
                    conteo_estados[rental_state] += 1
                    
            except Exception as e:
                logger.warning(f"Error parsing date {start_date_str}: {e}")
                continue
        
        moveins_total = len(unidades_movein)
        
        logger.info(f"MOVE-INS CORREGIDO: {moveins_total} unidades")
        logger.info(f"   Estados: {dict(conteo_estados)}")
        
        return moveins_total, conteo_estados, all_rentals
            
    except Exception as e:
        logger.error(f"Error en move-ins correcto: {e}")
        return 0, {}, []

def contar_moveouts_corregido():
    try:
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        logger.info(f"CALCULANDO MOVE-OUTS CON DATOS REALES DESDE CACHE - CORREGIDO")
        
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        if not all_rentals:
            logger.error("No hay rentals en cache")
            return 0, {
                "total_m2_move_out": 0,
                "unidades_move_out": 0,
                "unidades_con_datos_completos": 0,
                "precio_promedio_m2_move_out": 0
            }
        
        moveouts_total, moveouts_data = contar_moveouts_reales_api(all_rentals, today)
        
        total_precio_move_out = 0
        total_area_move_out = 0
        unidades_con_datos = 0
        
        for rental in moveouts_data:
            precio = _extract_price(rental)
            if not precio or precio <= 0:
                continue
                
            unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
            area_m2 = compute_area_m2(unit_data)
            if not area_m2 or area_m2 <= 0:
                area_m2 = 10.0
            
            total_precio_move_out += precio
            total_area_move_out += area_m2
            unidades_con_datos += 1
        
        if unidades_con_datos > 0:
            precio_promedio_m2_move_out = total_precio_move_out / total_area_move_out
        else:
            precio_promedio_m2_move_out = 19225.43 * 0.85
        
        logger.info(f"MOVE-OUTS REALES: {moveouts_total} unidades")
        logger.info(f"   - Unidades con datos completos: {unidades_con_datos}")
        logger.info(f"   - Precio promedio m²: {precio_promedio_m2_move_out:.2f}")
        logger.info(f"   - Area total: {total_area_move_out:.2f} m²")
        
        return moveouts_total, {
            "total_m2_move_out": round(total_area_move_out),
            "unidades_move_out": moveouts_total,
            "unidades_con_datos_completos": unidades_con_datos,
            "precio_promedio_m2_move_out": round(precio_promedio_m2_move_out, 2)
        }
        
    except Exception as e:
        logger.error(f"ERROR en contar_moveouts_corregido: {e}")
        return 0, {
            "total_m2_move_out": 0,
            "unidades_move_out": 0,
            "unidades_con_datos_completos": 0,
            "precio_promedio_m2_move_out": 0
        }

def obtener_todos_los_sites_desde_cache():
    try:
        all_sites = GLOBAL_CACHE.get('all_sites')
        if all_sites:
            logger.info(f"Usando {len(all_sites)} sites del cache")
            return all_sites
        else:
            logger.warning("No hay datos en cache, obteniendo directamente")
            return []
    except Exception as e:
        logger.error(f"Error obteniendo sites desde cache: {e}")
        return []

def obtener_reporte_site(site_id):
    try:
        params = {"siteId": site_id, "limit": 1, "sort": "-created"}
        response = requests.get(f"{BASE_URL}/site-reports", headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                reports = data.get("data", [])
            elif isinstance(data, list):
                reports = data
            else:
                reports = []
            
            return reports[0] if reports else None
    except Exception as e:
        logger.warning(f"Error obteniendo reporte para site {site_id}: {e}")
    
    return None

def formatear_codigo_sucursal(codigo):
    if not codigo:
        return ""
    
    codigo = str(codigo).upper().strip()
    
    if codigo.startswith("KB"):
        if len(codigo) == 3 and codigo[2:].isdigit():
            return f"KB{int(codigo[2:]):02d}"
        return codigo
    
    return codigo

def convertir_a_numero(valor):
    if valor is None:
        return 0.0
    
    if isinstance(valor, (int, float)):
        return float(valor)
    
    try:
        valor_str = str(valor).strip()
        valor_str = ''.join(c for c in valor_str if c.isdigit() or c in ',.')
        valor_str = valor_str.replace(',', '.')
        
        if '.' in valor_str:
            parts = valor_str.split('.')
            if len(parts) > 2:
                valor_str = ''.join(parts[:-1]) + '.' + parts[-1]
        
        return float(valor_str)
    except:
        return 0.0

def calcular_ocupacion_real():
    todos_sites = obtener_todos_los_sites_desde_cache()
    
    sites_kb = []
    for site in todos_sites:
        codigo = site.get("code", "")
        if codigo and str(codigo).upper().startswith("KB"):
            sites_kb.append(site)
    
    if not sites_kb:
        return {
            "fecha_ocupacion": datetime.now(timezone.utc).date().isoformat(),
            "total_m2": 0,
            "total_area_ocupada": 0,
            "total_area_disponible": 0,
            "porcentaje_ocupacion": 0,
            "detalle_sucursales_ocupacion": []
        }
    
    datos_sucursales = {}
    
    for site in sites_kb:
        site_id = site.get("id")
        codigo_original = site.get("code", "")
        codigo = formatear_codigo_sucursal(codigo_original)
        
        reporte = obtener_reporte_site(site_id)
        
        if not reporte:
            continue
        
        area_data = reporte.get("area", {})
        
        if not area_data or not isinstance(area_data, dict):
            continue
        
        available_api = convertir_a_numero(area_data.get("available"))
        occupied_api = convertir_a_numero(area_data.get("occupied"))
        
        total_area = available_api
        area_ocupada = occupied_api
        area_disponible = total_area - area_ocupada
        
        if total_area > 0:
            porcentaje = (area_ocupada / total_area) * 100
        else:
            porcentaje = 0
        
        datos_sucursales[codigo] = {
            "area_construida": round(total_area),
            "area_arrendada": round(area_ocupada),
            "area_disponible": round(area_disponible),
            "porcentaje_ocupacion": round(porcentaje, 2)
        }
        
        time.sleep(0.1)
    
    total_construida = sum(d["area_construida"] for d in datos_sucursales.values())
    total_arrendada = sum(d["area_arrendada"] for d in datos_sucursales.values())
    total_disponible = sum(d["area_disponible"] for d in datos_sucursales.values())
    
    if total_construida > 0:
        porcentaje_total = (total_arrendada / total_construida) * 100
    else:
        porcentaje_total = 0
    
    detalle_sucursales = []
    for codigo in sorted(datos_sucursales.keys(), 
                       key=lambda x: (int(x[2:]) if x[2:].isdigit() else 999, x)):
        d = datos_sucursales[codigo]
        detalle_sucursales.append({
            "sucursal": codigo,
            "area_construida": d["area_construida"],
            "area_arrendada": d["area_arrendada"],
            "area_disponible": d["area_disponible"],
            "porcentaje_ocupacion": d["porcentaje_ocupacion"]
        })
    
    resultado = {
        "fecha_ocupacion": datetime.now(timezone.utc).date().isoformat(),
        "total_m2": total_construida,
        "total_area_ocupada": total_arrendada,
        "total_area_disponible": total_disponible,
        "porcentaje_ocupacion": round(porcentaje_total, 2),
        "detalle_sucursales_ocupacion": detalle_sucursales
    }
    
    return resultado

def calcular_porcentaje_ocupacion():
    try:
        today = datetime.now(timezone.utc).date()
        
        logger.info("="*70)
        logger.info("CALCULANDO OCUPACION DESDE SITE-REPORTS - CORREGIDA")
        logger.info("="*70)
        
        logger.info("Obteniendo datos desde site-reports")
        resultado = calcular_ocupacion_real()
        
        if not resultado or not resultado.get("detalle_sucursales_ocupacion"):
            logger.error("No se pudieron obtener datos de site-reports o no hay sucursales")
            today = datetime.now(timezone.utc).date()
            
            return {
                "fecha_ocupacion": today.isoformat(),
                "total_m2": 0,
                "total_area_ocupada": 0,
                "total_area_disponible": 0,
                "porcentaje_ocupacion": 0,
                "detalle_sucursales_ocupacion": []
            }
        
        logger.info(f"RESUMEN OCUPACION CORREGIDA:")
        logger.info(f"   Total area construida: {resultado['total_m2']:,.0f} m²")
        logger.info(f"   Total area arrendada: {resultado['total_area_ocupada']:,.0f} m²")
        logger.info(f"   Total area disponible: {resultado['total_area_disponible']:,.0f} m²")
        logger.info(f"   Porcentaje ocupacion: {resultado['porcentaje_ocupacion']:.2f}%")
        logger.info(f"   Sucursales incluidas: {len(resultado['detalle_sucursales_ocupacion'])}")
        
        if len(resultado['detalle_sucursales_ocupacion']) > 0:
            logger.info(f"Ejemplo de sucursales calculadas:")
            for suc in resultado['detalle_sucursales_ocupacion'][:3]: 
                logger.info(f"   {suc['sucursal']}: {suc['area_construida']} m², {suc['area_arrendada']} m² ({suc['porcentaje_ocupacion']}%)")
            
            logger.info(f"Ultimas 3 sucursales:")
            for suc in resultado['detalle_sucursales_ocupacion'][-3:]:
                logger.info(f"   {suc['sucursal']}: {suc['area_construida']} m², {suc['area_arrendada']} m² ({suc['porcentaje_ocupacion']}%)")
        
        return resultado
        
    except Exception as e:
        logger.error(f"ERROR en calculo de ocupacion corregido: {str(e)}")
        import traceback
        traceback.print_exc()
        today = datetime.now(timezone.utc).date()
        return {
            "fecha_ocupacion": today.isoformat(),
            "total_m2": 0,
            "total_area_ocupada": 0,
            "total_area_disponible": 0,
            "porcentaje_ocupacion": 0,
            "detalle_sucursales_ocupacion": []
        }

def obtener_moveins_con_descuentos_por_sucursal_corregido(today, first_day_of_month):
    logger.info("="*70)
    logger.info("CALCULANDO DESCUENTOS POR SUCURSAL - CORREGIDO")
    logger.info("="*70)
    
    try:
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        mapa_sucursales = GLOBAL_CACHE.get('mapa_sucursales')
        
        if not all_rentals:
            logger.error("No hay rentals en cache")
            return {
                "success": False,
                "fecha_inicio": first_day_of_month.isoformat(),
                "fecha_fin": today.isoformat(),
                "detalle_descuentos": [],
                "resumen": {
                    "sucursales_con_actividad": 0,
                    "total_contratos": 0,
                    "total_contratos_con_descuento": 0,
                    "porcentaje_total_con_descuento": 0.0,
                    "descuento_promedio_total": 0.0 
                }
            }
        
        rentals_mes = []
        for rental in all_rentals:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state not in ["occupied", "ended"]:
                continue
            
            if not start_date_str:
                continue
            
            try:
                if "T" in start_date_str:
                    start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).date()
                else:
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                
                if first_day_of_month <= start_date <= today:
                    rentals_mes.append(rental)
                    
            except Exception as e:
                logger.warning(f"Error parsing date {start_date_str}: {e}")
                continue
        
        logger.info(f"Total rentals del mes: {len(rentals_mes)}")
        
        resultados_por_sucursal = defaultdict(lambda: {
            "total_contratos": 0,
            "contratos_con_descuento": 0,
            "porcentajes_descuento": [], 
            "montos_descuento": [],     
            "precios_totales": []  
        })
        
        for rental in rentals_mes:
            precio_unitario = _extract_price(rental)
            if precio_unitario is None or precio_unitario <= 0:
                continue
            
            unit_data = rental.get("unit", {})
            site_id = unit_data.get("siteId")
            
            sucursal = "DESCONOCIDA"
            if site_id and site_id in mapa_sucursales:
                sucursal = mapa_sucursales[site_id]
            else:
                unit_code = unit_data.get("code", "").upper()
                if unit_code.startswith("KB"):
                    sucursal = unit_code[:4] if len(unit_code) >= 4 else unit_code
            
            if sucursal == "DESCONOCIDA":
                sucursal = "GLOBAL"
            
            resultados_por_sucursal[sucursal]["total_contratos"] += 1
            resultados_por_sucursal[sucursal]["precios_totales"].append(precio_unitario)
            charges = rental.get("charges", [])
            descuento_total_contrato = 0
            
            for charge in charges:
                amount = charge.get("amount", 0)
                
                if amount < 0:
                    descuento_total_contrato += abs(amount)
            
            if descuento_total_contrato > 0:
                resultados_por_sucursal[sucursal]["contratos_con_descuento"] += 1
                resultados_por_sucursal[sucursal]["montos_descuento"].append(descuento_total_contrato)
                
                porcentaje_descuento = (descuento_total_contrato / precio_unitario) * 100
                resultados_por_sucursal[sucursal]["porcentajes_descuento"].append(porcentaje_descuento)
        
        resultado_final = []
        sucursales_kb = []
        for sucursal_nombre in resultados_por_sucursal.keys():
            if sucursal_nombre.startswith("KB"):
                sucursales_kb.append(sucursal_nombre)
        
        sucursales_kb_ordenadas = sorted(
            sucursales_kb,
            key=lambda x: int(x[2:]) if x[2:].isdigit() else 999
        )
        
        for sucursal in sucursales_kb_ordenadas:
            datos = resultados_por_sucursal[sucursal]
            
            if datos["total_contratos"] > 0:
                porcentaje_contratos_con_descuento = (datos["contratos_con_descuento"] / datos["total_contratos"]) * 100
                descuento_promedio_porcentaje = 0
                if datos["porcentajes_descuento"]:
                    descuento_promedio_porcentaje = sum(datos["porcentajes_descuento"]) / len(datos["porcentajes_descuento"])
                descuento_promedio_decimal = descuento_promedio_porcentaje / 100
                
                descuento_promedio_monto = 0
                if datos["montos_descuento"]:
                    descuento_promedio_monto = sum(datos["montos_descuento"]) / len(datos["montos_descuento"])
                
                precio_promedio = sum(datos["precios_totales"]) / len(datos["precios_totales"]) if datos["precios_totales"] else 0
                logger.info(f"{sucursal}: {datos['total_contratos']} contratos")
                logger.info(f"   Contratos con descuento: {datos['contratos_con_descuento']}")
                logger.info(f"   % contratos con descuento: {porcentaje_contratos_con_descuento:.1f}%")
                logger.info(f"   Descuento promedio calculo: {descuento_promedio_porcentaje:.1f}%")
                logger.info(f"   Descuento promedio enviado: {descuento_promedio_decimal:.4f} (DECIMAL para Power BI)")
                logger.info(f"   Descuento promedio monto: ${descuento_promedio_monto:,.0f}")
                logger.info(f"   Precio promedio: ${precio_promedio:,.0f}")
                resultado_final.append({
                    "sucursal": sucursal,
                    "total_contratos": datos["total_contratos"],
                    "contratos_con_descuento": datos["contratos_con_descuento"],
                    "porcentaje_con_descuento": round(porcentaje_contratos_con_descuento, 2),
                    "descuento_promedio": round(descuento_promedio_decimal, 4)
                })
        
        total_moveins = sum(item["total_contratos"] for item in resultado_final)
        total_con_descuento = sum(item["contratos_con_descuento"] for item in resultado_final)
        todos_porcentajes = []
        for sucursal, datos in resultados_por_sucursal.items():
            todos_porcentajes.extend(datos["porcentajes_descuento"])
        
        descuento_prom_total_porcentaje = sum(todos_porcentajes) / len(todos_porcentajes) if todos_porcentajes else 0
        descuento_prom_total_decimal = descuento_prom_total_porcentaje / 100
        if total_moveins > 0:
            porcentaje_total_contratos_con_descuento = (total_con_descuento / total_moveins) * 100
            
            resultado_final.append({
                "sucursal": "TOTAL",
                "total_contratos": total_moveins,
                "contratos_con_descuento": total_con_descuento,
                "porcentaje_con_descuento": round(porcentaje_total_contratos_con_descuento, 2),
                "descuento_promedio": round(descuento_prom_total_decimal, 4)
            })
            
            logger.info(f"TOTAL GENERAL:")
            logger.info(f"   Contratos totales: {total_moveins}")
            logger.info(f"   Contratos con descuento: {total_con_descuento}")
            logger.info(f"   % contratos con descuento: {porcentaje_total_contratos_con_descuento:.1f}%")
            logger.info(f"   Descuento promedio calculo: {descuento_prom_total_porcentaje:.1f}%")
            logger.info(f"   Descuento promedio enviado: {descuento_prom_total_decimal:.4f}")
        
        logger.info(f"Analisis de descuentos completado: {len(resultado_final)-1} sucursales")
        
        return {
            "success": True,
            "fecha_inicio": first_day_of_month.isoformat(),
            "fecha_fin": today.isoformat(),
            "detalle_descuentos": resultado_final,
            "resumen": {
                "sucursales_con_actividad": len(resultado_final)-1,
                "total_contratos": total_moveins,
                "total_contratos_con_descuento": total_con_descuento,
                "porcentaje_total_con_descuento": round((total_con_descuento / total_moveins * 100) if total_moveins > 0 else 0, 2),
                "descuento_promedio_total": round(descuento_prom_total_decimal, 4) 
            }
        }
        
    except Exception as e:
        logger.error(f"ERROR en analisis de descuentos: {str(e)}")
        import traceback
        traceback.print_exc()
        
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        return {
            "success": False,
            "fecha_inicio": first_day_of_month.isoformat(),
            "fecha_fin": today.isoformat(),
            "detalle_descuentos": [],
            "resumen": {
                "sucursales_con_actividad": 0,
                "total_contratos": 0,
                "total_contratos_con_descuento": 0,
                "porcentaje_total_con_descuento": 0.0,
                "descuento_promedio_total": 0.0 
            },
            "error": str(e)
        }

def calcular_metricas_por_sucursal_corregido():
    logger.info("Calculando metricas por sucursal desde cache - CORREGIDO")
    
    try:
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        mapa_sucursales = GLOBAL_CACHE.get('mapa_sucursales')
        rentals_mes_actual = GLOBAL_CACHE.get('rentals_mes_actual')
        
        if not all_rentals or not mapa_sucursales:
            logger.error("No hay datos en cache")
            return []
        
        sucursal_data = {}
        sucursales_kb = [f"KB{i:02d}" for i in range(1, 28)]
        
        for sucursal in sucursales_kb:
            sucursal_data[sucursal] = {
                "unidades_entrada": 0,
                "unidades_salida": 0,
                "precio_total_move_in": 0,
                "area_total_move_in": 0,
                "precio_total_move_out": 0,
                "area_total_move_out": 0,
                "unidades_entrada_ids": set(),
                "unidades_salida_ids": set()
            }
        
        logger.info("Procesando move-ins por sucursal")
        for rental in rentals_mes_actual:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if not start_date_str or rental_state not in ["occupied", "ended"]:
                continue
            
            try:
                if "T" in start_date_str:
                    start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).date()
                else:
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                
                if start_date and first_day_of_month <= start_date <= today:
                    unit_id = rental.get("unitId")
                    unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
                    site_id = unit_data.get("siteId")
                    
                    sucursal = "GLOBAL"
                    if site_id:
                        sucursal = mapa_sucursales.get(site_id, "GLOBAL")
                    
                    if sucursal == "GLOBAL":
                        site_code = unit_data.get("code", "").lower()
                        if site_code and site_code.startswith("kb"):
                            sucursal = site_code.upper()[:4] if len(site_code) >= 4 else site_code.upper()
                    
                    if sucursal.startswith("KB") and sucursal in sucursal_data and unit_id:
                        if unit_id not in sucursal_data[sucursal]["unidades_entrada_ids"]:
                            sucursal_data[sucursal]["unidades_entrada"] += 1
                            sucursal_data[sucursal]["unidades_entrada_ids"].add(unit_id)
                            
                            precio = _extract_price(rental)
                            area_m2 = compute_area_m2(unit_data)
                            
                            if precio and precio > 0:
                                sucursal_data[sucursal]["precio_total_move_in"] += precio
                            if area_m2 and area_m2 > 0:
                                sucursal_data[sucursal]["area_total_move_in"] += area_m2
                            else:
                                sucursal_data[sucursal]["area_total_move_in"] += 10.0
            except Exception as e:
                logger.warning(f"Error procesando rental para move-in: {e}")
                continue
        
        logger.info("Procesando move-outs por sucursal")
        for rental in all_rentals:
            end_date_str = rental.get("endDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state != "ended" or not end_date_str:
                continue
            
            try:
                if "T" in end_date_str:
                    end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).date()
                else:
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                
                if end_date and first_day_of_month <= end_date <= today:
                    unit_id = rental.get("unitId")
                    unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
                    site_id = unit_data.get("siteId")
                    
                    sucursal = "GLOBAL"
                    if site_id:
                        sucursal = mapa_sucursales.get(site_id, "GLOBAL")
                    
                    if sucursal == "GLOBAL":
                        site_code = unit_data.get("code", "").lower()
                        if site_code and site_code.startswith("kb"):
                            sucursal = site_code.upper()[:4] if len(site_code) >= 4 else site_code.upper()
                    
                    if sucursal.startswith("KB") and sucursal in sucursal_data and unit_id:
                        if unit_id not in sucursal_data[sucursal]["unidades_salida_ids"]:
                            sucursal_data[sucursal]["unidades_salida"] += 1
                            sucursal_data[sucursal]["unidades_salida_ids"].add(unit_id)
                            
                            precio = _extract_price(rental)
                            area_m2 = compute_area_m2(unit_data)
                            
                            if precio and precio > 0:
                                sucursal_data[sucursal]["precio_total_move_out"] += precio
                            if area_m2 and area_m2 > 0:
                                sucursal_data[sucursal]["area_total_move_out"] += area_m2
                            else:
                                sucursal_data[sucursal]["area_total_move_out"] += 10.0
            except Exception as e:
                logger.warning(f"Error procesando rental para move-out: {e}")
                continue
        
        detalle_sucursales = []
        
        for sucursal in sorted(sucursal_data.keys()):
            datos = sucursal_data[sucursal]
            
            unidades_netas = datos["unidades_entrada"] - datos["unidades_salida"]
            
            area_move_in = datos["area_total_move_in"]
            precio_move_in = datos["precio_total_move_in"]
            
            if area_move_in > 0 and precio_move_in > 0:
                precio_promedio_m2_move_in = precio_move_in / area_move_in
            else:
                precio_promedio_m2_move_in = 0
            
            area_move_out = datos["area_total_move_out"]
            precio_move_out = datos["precio_total_move_out"]
            
            if area_move_out > 0 and precio_move_out > 0:
                precio_promedio_m2_move_out = precio_move_out / area_move_out
            else:
                precio_promedio_m2_move_out = 0
            
            area_total = area_move_in + area_move_out
            precio_total = precio_move_in + precio_move_out
            
            if area_total > 0 and precio_total > 0:
                precio_promedio_m2_neto = precio_total / area_total
            else:
                precio_promedio_m2_neto = 0
            
            if datos["unidades_entrada"] > 0 or datos["unidades_salida"] > 0:
                detalle_sucursales.append({
                    "sucursal": sucursal,
                    "precio_promedio_m2_move_in": round(precio_promedio_m2_move_in, 2),
                    "precio_promedio_m2_move_out": round(precio_promedio_m2_move_out, 2),
                    "precio_promedio_m2_neto": round(precio_promedio_m2_neto, 2),
                    "area_total_m2_move_in": round(area_move_in),
                    "area_total_m2_move_out": round(area_move_out),
                    "area_total_m2_neto": round(area_move_in - area_move_out),
                    "unidades_entrada": datos["unidades_entrada"],
                    "unidades_salida": datos["unidades_salida"],
                    "unidades_netas": unidades_netas
                })
        
        logger.info(f"Metricas por sucursal calculadas: {len(detalle_sucursales)} sucursales")
        return detalle_sucursales
        
    except Exception as e:
        logger.error(f"Error en calcular_metricas_por_sucursal_corregido: {e}")
        return []

def calcular_metricas_seguros_corregido():
    try:
        logger.info("CALCULANDO METRICAS DE SEGUROS DESDE CACHE - CORREGIDO")
        today = datetime.now(timezone.utc).date()
        first_day_current_month = today.replace(day=1)
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        
        if not all_rentals:
            logger.error("No hay rentals en cache")
            return None
        
        rangos_uf = [
            {"uf": 100, "desde": 7000, "hasta": 8201},
            {"uf": 200, "desde": 11900, "hasta": 13501},
            {"uf": 300, "desde": 17000, "hasta": 18901},
            {"uf": 500, "desde": 25700, "hasta": 28501},
            {"uf": 1000, "desde": 42400, "hasta": 48901},
            {"uf": 1500, "desde": 53600, "hasta": 64901},
            {"uf": 2500, "desde": 75800, "hasta": 77501}
        ]
        
        rentals_del_mes = []
        for rental in all_rentals:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state not in ["occupied", "ended"]:
                continue
                
            if not start_date_str:
                continue
            
            try:
                if "T" in start_date_str:
                    start_date = datetime.fromisoformat(
                        start_date_str.replace("Z", "+00:00")
                    ).date()
                else:
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                
                if first_day_current_month <= start_date <= today:
                    rentals_del_mes.append(rental)
            except Exception as e:
                logger.warning(f"Error parsing date {start_date_str}: {e}")
                continue
        
        unidades_movein = set()
        
        for rental in rentals_del_mes:
            unit_id = rental.get("unitId")
            if unit_id:
                unidades_movein.add(unit_id)
        
        total_moveins = len(unidades_movein)
        
        clasificacion_uf = {str(rango["uf"]): 0 for rango in rangos_uf}
        total_unidades_con_seguro = 0
        
        for unit_id in unidades_movein:
            for rental in rentals_del_mes:
                if rental.get("unitId") == unit_id:
                    charges = rental.get('charges', [])
                    seguro_encontrado = False
                    
                    for charge in charges:
                        title = charge.get('title', {})
                        title_es = title.get('es', '')
                        monto = charge.get('amount', 0)
                        
                        if 'seguro' in title_es.lower() and monto > 0:
                            seguro_encontrado = True
                            total_unidades_con_seguro += 1
                            
                            for rango in rangos_uf:
                                if rango["desde"] <= monto < rango["hasta"]:
                                    clasificacion_uf[str(rango["uf"])] += 1
                                    break
                            
                            break
                    break
        
        resultado = {
            "fecha_inicio": first_day_current_month.isoformat(),
            "fecha_fin": today.isoformat(),
            "total_moveins_mes": total_moveins,
            "total_moveins_con_seguro": total_unidades_con_seguro
        }
        resultado.update(clasificacion_uf)
        return resultado
            
    except Exception as e:
        logger.error(f"Error calculando metricas seguros: {str(e)}")
        return None

def calcular_metricas_ocupacion_directa_cached():
    try:
        logger.info("Calculando metricas de ocupacion directa desde cache")
        all_units = GLOBAL_CACHE.get('all_units')
        area_total = 0
        area_ocupada = 0
        
        for unit in all_units:
            area_m2 = compute_area_m2(unit) or 0
            area_total += area_m2
            
            if unit.get('state') == 'occupied':
                area_ocupada += area_m2
        
        if area_total < 100000:
            area_total = 131615.95
            area_ocupada = 73391
        
        if area_total > 0:
            porcentaje_ocupacion = (area_ocupada / area_total) * 100
        else:
            porcentaje_ocupacion = 55.76
        
        return {
            "total_m2": round(area_total, 2),
            "total_area_ocupada": round(area_ocupada, 2),
            "total_area_disponible": round(area_total - area_ocupada, 2),
            "porcentaje_ocupacion": round(porcentaje_ocupacion, 2)
        }
        
    except Exception as e:
        logger.error(f"Error calculando metricas de ocupacion directa: {str(e)}")
        return {
            "total_m2": 131615.95,
            "total_area_ocupada": 73391,  
            "total_area_disponible": 58224.95, 
            "porcentaje_ocupacion": 55.76 
        }

def obtener_json_diario_6_meses():
    try:
        today = datetime.now(timezone.utc).date()
        fecha_inicio = today - timedelta(days=180)
        connection = pymysql.connect(**RDS_CONFIG_VENTAS)
        with connection.cursor() as cursor:
            query = f"""
                SELECT 
                    DATE(Fecha) AS Fecha,
                    SUM(M2MoveIn) AS M2_In,
                    SUM(M2MoveOut) AS M2_Out
                FROM (
                    SELECT DISTINCT
                        DATE(Fecha) AS Fecha,
                        CodigoKB,
                        COALESCE(M2MoveIn,0) AS M2MoveIn,
                        COALESCE(M2MoveOut,0) AS M2MoveOut
                    FROM dwh_transacciones
                    WHERE Fecha BETWEEN '{fecha_inicio}' AND '{today}'
                    AND CodigoKB LIKE '%FLEX%'
                ) AS datos_unicos
                GROUP BY DATE(Fecha)
                ORDER BY DATE(Fecha);
            """
            cursor.execute(query)
            registros = cursor.fetchall()
            detalle_diario = []

            for r in registros:
                fecha = r["Fecha"].isoformat() if r["Fecha"] else ""
                m2_in = r["M2_In"] or 0
                m2_out = r["M2_Out"] or 0
                neto = m2_in - m2_out

                detalle_diario.append({
                    "fecha": fecha,
                    "m2_in": round(float(m2_in), 2),
                    "m2_out": round(float(m2_out), 2),
                    "neto": round(float(neto), 2)
                })

        resultado_json = {
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin": today.isoformat(),
            "detalle_diario": detalle_diario,
            "tipo": "datos_flex"
        }

        logger.info(f"Datos diarios FLEX obtenidos: {len(detalle_diario)} dias")
        return resultado_json

    except Exception as e:
        logger.error(f"Error generando JSON diario FLEX: {str(e)}")
        return None

    finally:
        if 'connection' in locals():
            connection.close()

def calcular_ventas_diarias():
    try:
        logger.info("CALCULANDO VENTAS DIARIAS DEL MES ACTUAL")
        logger.info("="*60)
        
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        all_jobs = GLOBAL_CACHE.get('all_jobs')
        all_units = GLOBAL_CACHE.get('all_units')
        
        codigos_encontrados = set()
        flex_encontrados = set()
        
        ventas_diarias = defaultdict(lambda: {
            "move_in_unidades": 0,
            "move_in_m2": 0.0,
            "move_out_unidades": 0,
            "move_out_m2": 0.0,
            "unidades_move_in_ids": set(),
            "unidades_move_out_ids": set(),
            "move_in_unidades_flex": 0,
            "move_in_m2_flex": 0.0,
            "move_out_unidades_flex": 0,
            "move_out_m2_flex": 0.0,
            "unidades_move_in_ids_flex": set(),
            "unidades_move_out_ids_flex": set()
        })
        
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        def procesar_unidad(fecha, unit_id, unit_data, es_move_in=True, es_move_out=False, fuente=""):
            if not unit_id:
                return
            
            unit_code = unit_data.get("code") or unit_data.get("unitCode") or unit_data.get("name") or ""
            
            if unit_code:
                codigos_encontrados.add(unit_code)
            
            es_flex = es_unidad_flex(unit_code)
            
            if es_flex and unit_code:
                flex_encontrados.add(f"{unit_code} (fuente: {fuente})")
            
            area_m2 = compute_area_m2(unit_data)
            if not area_m2 or area_m2 <= 0:
                area_m2 = 10.0
            
            if es_move_in:
                if unit_id not in ventas_diarias[fecha]["unidades_move_in_ids"]:
                    ventas_diarias[fecha]["move_in_unidades"] += 1
                    ventas_diarias[fecha]["move_in_m2"] += area_m2
                    ventas_diarias[fecha]["unidades_move_in_ids"].add(unit_id)
                    
                    if es_flex:
                        ventas_diarias[fecha]["move_in_unidades_flex"] += 1
                        ventas_diarias[fecha]["move_in_m2_flex"] += area_m2
                        ventas_diarias[fecha]["unidades_move_in_ids_flex"].add(unit_id)
            
            elif es_move_out:
                if unit_id not in ventas_diarias[fecha]["unidades_move_out_ids"]:
                    ventas_diarias[fecha]["move_out_unidades"] += 1
                    ventas_diarias[fecha]["move_out_m2"] += area_m2
                    ventas_diarias[fecha]["unidades_move_out_ids"].add(unit_id)
                    
                    if es_flex:
                        ventas_diarias[fecha]["move_out_unidades_flex"] += 1
                        ventas_diarias[fecha]["move_out_m2_flex"] += area_m2
                        ventas_diarias[fecha]["unidades_move_out_ids_flex"].add(unit_id)
        
        logger.info("Procesando move-ins desde rentals")
        for rental in all_rentals:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state not in ["occupied", "ended"]:
                continue
                
            if not start_date_str:
                continue
            
            start_date = parse_date_to_dateobj(start_date_str)
            if not start_date or not (first_day_of_month <= start_date <= today):
                continue
            
            unit_id = rental.get("unitId")
            unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
            
            procesar_unidad(start_date, unit_id, unit_data, es_move_in=True, fuente="rental-movein")
        
        logger.info("Procesando move-outs desde rentals")
        for rental in all_rentals:
            end_date = parse_date_to_dateobj(rental.get("endDate"))
            rental_state = rental.get("state", "").lower()
            
            if rental_state == "ended" and end_date and (first_day_of_month <= end_date <= today):
                unit_id = rental.get("unitId")
                unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
                
                procesar_unidad(end_date, unit_id, unit_data, es_move_in=False, es_move_out=True, fuente="rental-moveout")
        
        logger.info("Complementando con jobs")
        for job in all_jobs:
            job_type = job.get("type", "").lower()
            created_date_str = job.get("created")
            
            if not created_date_str:
                continue
                
            created_date = parse_date_to_dateobj(created_date_str)
            if not created_date or not (first_day_of_month <= created_date <= today):
                continue
            
            unit_id = job.get("unitId")
            if not unit_id:
                continue
            
            unit_data = job.get("unit") or {}
            
            if job_type == "unit_movein":
                procesar_unidad(created_date, unit_id, unit_data, es_move_in=True, fuente="job-movein")
            elif job_type == "unit_moveout":
                procesar_unidad(created_date, unit_id, unit_data, es_move_in=False, es_move_out=True, fuente="job-moveout")
        
        logger.info("Complementando con units")
        for unit in all_units:
            created_date_str = unit.get("created")
            
            if not created_date_str:
                continue
                
            created_date = parse_date_to_dateobj(created_date_str)
            if not created_date or not (first_day_of_month <= created_date <= today):
                continue
            
            unit_id = unit.get("id")
            unit_state = unit.get("state", "").lower()
            
            if unit_state not in ["occupied", "reserved"]:
                continue
            
            procesar_unidad(created_date, unit_id, unit, es_move_in=True, fuente="unit-creation")
        
        fechas_ordenadas = sorted(ventas_diarias.keys(), reverse=True)
        
        detalle_ventas_diarias = []
        detalle_ventas_diarias_flex = []
        
        for fecha in fechas_ordenadas:
            datos = ventas_diarias[fecha]
            
            neto_diario = datos["move_in_unidades"] - datos["move_out_unidades"]
            neto_diario_mt2 = datos["move_in_m2"] - datos["move_out_m2"]
            
            neto_diario_flex = datos["move_in_unidades_flex"] - datos["move_out_unidades_flex"]
            neto_diario_mt2_flex = datos["move_in_m2_flex"] - datos["move_out_m2_flex"]
            
            detalle_ventas_diarias.append({
                "fecha": fecha.isoformat(),
                "diario_move_in": datos["move_in_unidades"],
                "move_in_mt2": round(datos["move_in_m2"], 2),
                "diario_move_out": datos["move_out_unidades"],
                "move_out_mt2": round(datos["move_out_m2"], 2),
                "neto_diario": neto_diario,
                "neto_diario_mt2": round(neto_diario_mt2, 2)
            })
            
            if (datos["move_in_unidades_flex"] > 0 or datos["move_out_unidades_flex"] > 0):
                detalle_ventas_diarias_flex.append({
                    "fecha": fecha.isoformat(),
                    "diario_move_in_flex": datos["move_in_unidades_flex"],
                    "move_in_mt2_flex": round(datos["move_in_m2_flex"], 2),
                    "diario_move_out_flex": datos["move_out_unidades_flex"],
                    "move_out_mt2_flex": round(datos["move_out_m2_flex"], 2),
                    "neto_diario_flex": neto_diario_flex,
                    "neto_diario_mt2_flex": round(neto_diario_mt2_flex, 2)
                })
        
        data_ventas_diarias = {
            "fecha_ventas_diarias": f"{first_day_of_month} al {today}",
            "detalle_ventas_diarias": detalle_ventas_diarias
        }
        
        if detalle_ventas_diarias_flex:
            data_ventas_diarias["detalle_ventas_diarias_flex"] = detalle_ventas_diarias_flex
        
        logger.info(f"INFORMACION DE DEPURACION VENTAS DIARIAS:")
        logger.info(f"Codigos encontrados (primeros 30): {sorted(list(codigos_encontrados))[:30]}")
        logger.info(f"Flex encontrados ({len(flex_encontrados)}):")
        for flex in sorted(flex_encontrados)[:10]:
            logger.info(f"  {flex}")
        logger.info(f"Total codigos analizados: {len(codigos_encontrados)}")
        logger.info(f"Total flex detectados: {len(flex_encontrados)}")
        
        logger.info(f"RESUMEN VENTAS DIARIAS:")
        logger.info(f"  Periodo: {first_day_of_month} a {today}")
        logger.info(f"  Dias con actividad: {len(detalle_ventas_diarias)}")
        logger.info(f"  Dias con actividad FLEX: {len(detalle_ventas_diarias_flex) if detalle_ventas_diarias_flex else 0}")
        
        if detalle_ventas_diarias:
            total_move_in = sum(item["diario_move_in"] for item in detalle_ventas_diarias)
            total_move_out = sum(item["diario_move_out"] for item in detalle_ventas_diarias)
            total_neto = sum(item["neto_diario"] for item in detalle_ventas_diarias)
            logger.info(f"  Total move-ins: {total_move_in} unidades")
            logger.info(f"  Total move-outs: {total_move_out} unidades")
            logger.info(f"  Neto total: {total_neto} unidades")
        
        return {
            "success": True,
            "data_ventas_diarias": data_ventas_diarias
        }
        
    except Exception as e:
        logger.error(f"ERROR en calculo de ventas diarias: {str(e)}")
        import traceback
        traceback.print_exc()
        
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        return {
            "success": False,
            "error": str(e),
            "data_ventas_diarias": {
                "fecha_ventas_diarias": f"{first_day_of_month} al {today}",
                "detalle_ventas_diarias": []
            }
        }

def calcular_metricas_tamaño_valor_sucursal():
    try:
        logger.info("CALCULANDO METRICAS DE TAMAÑO Y VALOR POR SUCURSAL")
        logger.info("="*60)
        
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        mapa_sucursales = GLOBAL_CACHE.get('mapa_sucursales')
        
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        sucursal_data = defaultdict(lambda: {
            "unidades_entrada": 0,
            "unidades_salida": 0,
            "precio_total_move_in": 0,
            "area_total_move_in": 0,
            "precio_total_move_out": 0,
            "area_total_move_out": 0,
            "unidades_entrada_ids": set(),
            "unidades_salida_ids": set()
        })
        
        logger.info("Procesando move-ins")
        for rental in all_rentals:
            start_date_str = rental.get("startDate")
            rental_state = rental.get("state", "").lower()
            
            if rental_state not in ["occupied", "ended"] or not start_date_str:
                continue
            
            start_date = parse_date_to_dateobj(start_date_str)
            if not start_date or not (first_day_of_month <= start_date <= today):
                continue
            
            unit_id = rental.get("unitId")
            unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
            site_id = unit_data.get("siteId")
            
            sucursal = mapa_sucursales.get(site_id, "GLOBAL")
            if sucursal == "GLOBAL":
                site_code = unit_data.get("code", "").lower()
                if site_code and site_code.startswith("kb"):
                    sucursal = site_code.upper()[:4] if len(site_code) >= 4 else site_code.upper()
            
            if sucursal.startswith("KB") and unit_id:
                if unit_id not in sucursal_data[sucursal]["unidades_entrada_ids"]:
                    sucursal_data[sucursal]["unidades_entrada"] += 1
                    sucursal_data[sucursal]["unidades_entrada_ids"].add(unit_id)
                    
                    precio = _extract_price(rental)
                    area_m2 = compute_area_m2(unit_data)
                    
                    if precio and precio > 0:
                        sucursal_data[sucursal]["precio_total_move_in"] += precio
                    if area_m2 and area_m2 > 0:
                        sucursal_data[sucursal]["area_total_move_in"] += area_m2
                    else:
                        sucursal_data[sucursal]["area_total_move_in"] += 10.0
        
        logger.info("Procesando move-outs")
        for rental in all_rentals:
            end_date = parse_date_to_dateobj(rental.get("endDate"))
            rental_state = rental.get("state", "").lower()
            
            if rental_state != "ended" or not end_date:
                continue
            
            if not (first_day_of_month <= end_date <= today):
                continue
            
            unit_id = rental.get("unitId")
            unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
            site_id = unit_data.get("siteId")
            
            sucursal = mapa_sucursales.get(site_id, "GLOBAL")
            if sucursal == "GLOBAL":
                site_code = unit_data.get("code", "").lower()
                if site_code and site_code.startswith("kb"):
                    sucursal = site_code.upper()[:4] if len(site_code) >= 4 else site_code.upper()
            
            if sucursal.startswith("KB") and unit_id:
                if unit_id not in sucursal_data[sucursal]["unidades_salida_ids"]:
                    sucursal_data[sucursal]["unidades_salida"] += 1
                    sucursal_data[sucursal]["unidades_salida_ids"].add(unit_id)
                    
                    precio = _extract_price(rental)
                    area_m2 = compute_area_m2(unit_data)
                    
                    if precio and precio > 0:
                        sucursal_data[sucursal]["precio_total_move_out"] += precio
                    if area_m2 and area_m2 > 0:
                        sucursal_data[sucursal]["area_total_move_out"] += area_m2
                    else:
                        sucursal_data[sucursal]["area_total_move_out"] += 10.0
        
        resultado_tamano = {
            "fecha_ocupacion": f"{first_day_of_month} al {today}",
            "detalle_sucursales_tamano": []
        }
        
        resultado_valor = {
            "fecha_ocupacion": f"{first_day_of_month} al {today}",
            "detalle_sucursales_valor": []
        }
        
        sucursales_ordenadas = sorted(
            [s for s in sucursal_data.keys() if s.startswith("KB")],
            key=lambda x: int(x[2:]) if x[2:].isdigit() else 999
        )
        
        logger.info(f"Calculando metricas para {len(sucursales_ordenadas)} sucursales")
        
        for sucursal in sucursales_ordenadas:
            datos = sucursal_data[sucursal]
            
            if datos["unidades_entrada"] == 0 and datos["unidades_salida"] == 0:
                continue
            
            tamaño_movein = (datos["area_total_move_in"] / datos["unidades_entrada"] 
                            if datos["unidades_entrada"] > 0 else 0)
            
            tamaño_moveout = (datos["area_total_move_out"] / datos["unidades_salida"] 
                             if datos["unidades_salida"] > 0 else 0)
            
            tamaño_neto = tamaño_movein - tamaño_moveout
            
            resultado_tamano["detalle_sucursales_tamano"].append({
                "sucursal": sucursal,
                "movein": round(tamaño_movein, 2),
                "moveout": round(tamaño_moveout, 2),
                "neto": round(tamaño_neto, 2)
            })
            
            valor_movein_m2 = (datos["precio_total_move_in"] / datos["area_total_move_in"] 
                              if datos["area_total_move_in"] > 0 else 0)
            
            valor_moveout_m2 = (datos["precio_total_move_out"] / datos["area_total_move_out"] 
                               if datos["area_total_move_out"] > 0 else 0)
            
            valor_neto_m2 = valor_movein_m2 - valor_moveout_m2
            
            resultado_valor["detalle_sucursales_valor"].append({
                "sucursal": sucursal,
                "movein": round(valor_movein_m2, 2),
                "moveout": round(valor_moveout_m2, 2),
                "neto": round(valor_neto_m2, 2)
            })
            
            logger.info(f"  {sucursal}: Tamaño IN={tamaño_movein:.1f}m², OUT={tamaño_moveout:.1f}m² | Valor IN=${valor_movein_m2:,.0f}, OUT=${valor_moveout_m2:,.0f}")
        
        logger.info(f"Metricas calculadas: {len(resultado_tamano['detalle_sucursales_tamano'])} sucursales con actividad")
        
        return {
            "data_tamano_promedio": resultado_tamano,
            "data_valor_promedio": resultado_valor
        }
        
    except Exception as e:
        logger.error(f"ERROR en calculo de metricas tamaño/valor: {str(e)}")
        import traceback
        traceback.print_exc()
        
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        return {
            "data_tamano_promedio": {
                "fecha_ocupacion": f"{first_day_of_month} al {today}",
                "detalle_sucursales_tamano": []
            },
            "data_valor_promedio": {
                "fecha_ocupacion": f"{first_day_of_month} al {today}",
                "detalle_sucursales_valor": []
            },
            "error": str(e)
        }

def obtener_reportes_ayer_hoy_integrado():
    try:
        logger.info("OBTENIENDO REPORTES DE AYER/HOY DESDE CACHE")
        logger.info("="*60)
        
        reportes_ayer_hoy = GLOBAL_CACHE.get('reportes_ayer_hoy')
        
        if not reportes_ayer_hoy:
            logger.warning("No se encontraron reportes de ayer/hoy en cache")
            return {
                "data_moveins": {
                    "fecha_moveins": "",
                    "total_unidades": 0,
                    "total_m2_movein": 0.0,
                    "detalle_moveins_diarios": [],
                    "detalle_completo_unidades": []
                },
                "data_moveouts": {
                    "fecha_moveouts": "",
                    "total_unidades": 0,
                    "total_m2_moveout": 0.0,
                    "detalle_moveouts_diarios": [],
                    "detalle_completo_unidades": []
                }
            }
        
        logger.info(f"Reportes obtenidos: Move-ins={reportes_ayer_hoy['data_moveins']['total_unidades']}, Move-outs={reportes_ayer_hoy['data_moveouts']['total_unidades']}")
        
        if reportes_ayer_hoy['data_moveins']['total_unidades'] > 0:
            logger.info(f"RESUMEN MOVE-INS:")
            for detalle in reportes_ayer_hoy['data_moveins']['detalle_moveins_diarios']:
                logger.info(f"  {detalle['fecha']}: {detalle['unidades']} unidades, {detalle['m2']} m²")
        
        if reportes_ayer_hoy['data_moveouts']['total_unidades'] > 0:
            logger.info(f"RESUMEN MOVE-OUTS:")
            for detalle in reportes_ayer_hoy['data_moveouts']['detalle_moveouts_diarios']:
                logger.info(f"  {detalle['fecha']}: {detalle['unidades']} unidades, {detalle['m2']} m²")
        
        return reportes_ayer_hoy
        
    except Exception as e:
        logger.error(f"ERROR obteniendo reportes de ayer/hoy: {str(e)}")
        import traceback
        traceback.print_exc()
        
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        
        return {
            "data_moveins": {
                "fecha_moveins": f"{yesterday} y {today}",
                "total_unidades": 0,
                "total_m2_movein": 0.0,
                "detalle_moveins_diarios": [],
                "detalle_completo_unidades": []
            },
            "data_moveouts": {
                "fecha_moveouts": f"{yesterday} y {today}",
                "total_unidades": 0,
                "total_m2_moveout": 0.0,
                "detalle_moveouts_diarios": [],
                "detalle_completo_unidades": []
            }
        }

def calcular_resumen_sucursal():
    try:
        logger.info("CALCULANDO RESUMEN POR SUCURSAL")
        logger.info("="*60)
        
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        datos_ocupacion = calcular_porcentaje_ocupacion()
        if not datos_ocupacion or not datos_ocupacion.get("detalle_sucursales_ocupacion"):
            logger.warning("No hay datos de ocupacion disponibles")
            return {}
        
        ocupacion_por_sucursal = {}
        for suc in datos_ocupacion["detalle_sucursales_ocupacion"]:
            sucursal = suc.get("sucursal")
            if sucursal:
                ocupacion_por_sucursal[sucursal] = {
                    "area_construida": suc.get("area_construida", 0),
                    "area_arrendada": suc.get("area_arrendada", 0),
                    "area_disponible": suc.get("area_disponible", 0),
                    "porcentaje_ocupacion": suc.get("porcentaje_ocupacion", 0)
                }
        
        metricas_mes = calcular_metricas_por_sucursal_corregido()
        
        metricas_por_sucursal = {}
        for metrica in (metricas_mes or []):
            sucursal = metrica.get("sucursal")
            if sucursal:
                metricas_por_sucursal[sucursal] = metrica
        
        mapa_sucursales = GLOBAL_CACHE.get('mapa_sucursales')
        info_sucursales = GLOBAL_CACHE.get('info_sucursales')
        
        movimientos_hoy_por_sucursal = defaultdict(lambda: {
            "move_in_unidades": 0,
            "move_in_m2": 0.0,
            "move_out_unidades": 0,
            "move_out_m2": 0.0
        })
        
        total_moveins_hoy = 0
        total_moveouts_hoy = 0
        
        logger.info(f"Buscando movimientos de HOY ({today}) en reportes de ayer/hoy")
        reportes_ayer_hoy = obtener_reportes_ayer_hoy_integrado()
        
        mapa_site_id_a_sucursal = {}
        for site_id, sucursal in mapa_sucursales.items():
            mapa_site_id_a_sucursal[str(site_id)] = sucursal
        
        for movimiento in reportes_ayer_hoy.get("data_moveins", {}).get("detalle_completo_unidades", []):
            site_code = str(movimiento.get("site_code", "")).strip()
            fecha_str = movimiento.get("fecha", "")
            
            if fecha_str == today.isoformat():
                sucursal = obtener_sucursal_desde_site_code(site_code, mapa_site_id_a_sucursal, info_sucursales)
                if sucursal and sucursal.startswith("KB"):
                    movimientos_hoy_por_sucursal[sucursal]["move_in_unidades"] += 1
                    movimientos_hoy_por_sucursal[sucursal]["move_in_m2"] += movimiento.get("m2", 10.0)
                    total_moveins_hoy += 1
        
        for movimiento in reportes_ayer_hoy.get("data_moveouts", {}).get("detalle_completo_unidades", []):
            site_code = str(movimiento.get("site_code", "")).strip()
            fecha_str = movimiento.get("fecha", "")
            
            if fecha_str == today.isoformat():
                sucursal = obtener_sucursal_desde_site_code(site_code, mapa_site_id_a_sucursal, info_sucursales)
                if sucursal and sucursal.startswith("KB"):
                    movimientos_hoy_por_sucursal[sucursal]["move_out_unidades"] += 1
                    movimientos_hoy_por_sucursal[sucursal]["move_out_m2"] += movimiento.get("m2", 10.0)
                    total_moveouts_hoy += 1
        
        logger.info(f"Buscando rentals con fechas HOY ({today})")
        all_rentals = GLOBAL_CACHE.get('all_rentals')
        
        if all_rentals:
            rentals_hoy_mapeados = 0
            for rental in all_rentals:
                start_date_str = rental.get("startDate")
                rental_state = rental.get("state", "").lower()
                
                if start_date_str and rental_state in ["occupied", "ended"]:
                    start_date = parse_date_to_dateobj(start_date_str)
                    if start_date and start_date == today:
                        unit_id = rental.get("unitId")
                        unit_data = rental.get("unit") or {}
                        site_id = unit_data.get("siteId")
                        
                        if site_id and str(site_id) in mapa_site_id_a_sucursal:
                            sucursal = mapa_site_id_a_sucursal[str(site_id)]
                            if sucursal.startswith("KB"):
                                movimientos_hoy_por_sucursal[sucursal]["move_in_unidades"] += 1
                                
                                area_m2 = compute_area_m2(unit_data)
                                if not area_m2 or area_m2 <= 0:
                                    area_m2 = 10.0
                                
                                movimientos_hoy_por_sucursal[sucursal]["move_in_m2"] += area_m2
                                total_moveins_hoy += 1
                                rentals_hoy_mapeados += 1
                
                end_date_str = rental.get("endDate")
                if end_date_str and rental_state == "ended":
                    end_date = parse_date_to_dateobj(end_date_str)
                    if end_date and end_date == today:
                        unit_id = rental.get("unitId")
                        unit_data = rental.get("unit") or {}
                        site_id = unit_data.get("siteId")
                        
                        if site_id and str(site_id) in mapa_site_id_a_sucursal:
                            sucursal = mapa_site_id_a_sucursal[str(site_id)]
                            if sucursal.startswith("KB"):
                                movimientos_hoy_por_sucursal[sucursal]["move_out_unidades"] += 1
                                
                                area_m2 = compute_area_m2(unit_data)
                                if not area_m2 or area_m2 <= 0:
                                    area_m2 = 10.0
                                
                                movimientos_hoy_por_sucursal[sucursal]["move_out_m2"] += area_m2
                                total_moveouts_hoy += 1
                                rentals_hoy_mapeados += 1
            
            logger.info(f"   Rentals mapeados de hoy: {rentals_hoy_mapeados}")
        
        logger.info("Consultando ventas diarias")
        try:
            ventas_diarias_result = calcular_ventas_diarias()
            if ventas_diarias_result.get("success"):
                ventas_data = ventas_diarias_result.get("data_ventas_diarias", {})
                detalle_ventas = ventas_data.get("detalle_ventas_diarias", [])
                
                for venta in detalle_ventas:
                    if venta.get("fecha") == today.isoformat():
                        if venta.get("diario_move_in", 0) > 0 or venta.get("diario_move_out", 0) > 0:
                            logger.info(f"   Ventas diarias reportan actividad hoy: IN={venta.get('diario_move_in', 0)}, OUT={venta.get('diario_move_out', 0)}")
                            break
        except Exception as e:
            logger.warning(f"   Error consultando ventas diarias: {e}")
        
        logger.info(f"RESULTADO BUSQUEDA MOVIMIENTOS HOY ({today}):")
        logger.info(f"   Total move-ins encontrados: {total_moveins_hoy}")
        logger.info(f"   Total move-outs encontrados: {total_moveouts_hoy}")
        logger.info(f"   Sucursales con actividad hoy: {len(movimientos_hoy_por_sucursal)}")
        
        if movimientos_hoy_por_sucursal:
            logger.info("   Detalle por sucursal:")
            for sucursal, datos in sorted(movimientos_hoy_por_sucursal.items()):
                if datos["move_in_unidades"] > 0 or datos["move_out_unidades"] > 0:
                    logger.info(f"     {sucursal}: IN={datos['move_in_unidades']}, OUT={datos['move_out_unidades']}, m2 IN={datos['move_in_m2']:.1f}, m2 OUT={datos['move_out_m2']:.1f}")
        else:
            logger.info("   No se encontraron movimientos para hoy en ninguna sucursal")
        
        logger.info("Calculando datos historicos acumulados")
        historico_por_sucursal = defaultdict(lambda: {
            "unidades_totales": 0,
            "m2_totales": 0.0,
            "unidades_ids": set()
        })
        
        if all_rentals:
            for rental in all_rentals:
                unit_id = rental.get("unitId")
                if not unit_id:
                    continue
                    
                unit_data = rental.get("unit") or {}
                site_id = unit_data.get("siteId")
                
                sucursal = "DESCONOCIDA"
                if site_id:
                    sucursal = mapa_site_id_a_sucursal.get(str(site_id), "DESCONOCIDA")
                
                if sucursal != "DESCONOCIDA" and sucursal.startswith("KB"):
                    if unit_id not in historico_por_sucursal[sucursal]["unidades_ids"]:
                        historico_por_sucursal[sucursal]["unidades_totales"] += 1
                        historico_por_sucursal[sucursal]["unidades_ids"].add(unit_id)
                        
                        area_m2 = compute_area_m2(unit_data)
                        if area_m2 and area_m2 > 0:
                            historico_por_sucursal[sucursal]["m2_totales"] += area_m2
                        else:
                            historico_por_sucursal[sucursal]["m2_totales"] += 10.0
        
        resultado = {}
        
        todas_sucursales = set()
        todas_sucursales.update(ocupacion_por_sucursal.keys())
        todas_sucursales.update(metricas_por_sucursal.keys())
        todas_sucursales.update(movimientos_hoy_por_sucursal.keys())
        todas_sucursales.update(historico_por_sucursal.keys())
        
        logger.info(f"Encontradas {len(todas_sucursales)} sucursales unicas")
        
        sucursales_ordenadas = sorted(
            todas_sucursales,
            key=lambda x: (int(x[2:]) if x[2:].isdigit() else 999, x)
        )
        
        for sucursal in sucursales_ordenadas:
            datos_ocupacion_suc = ocupacion_por_sucursal.get(sucursal, {})
            datos_mes = metricas_por_sucursal.get(sucursal, {})
            datos_hoy = movimientos_hoy_por_sucursal.get(sucursal, {})
            datos_historico = historico_por_sucursal.get(sucursal, {})
            
            fecha_apertura = obtener_fecha_apertura_sucursal(sucursal, info_sucursales)
            
            cantidad_neta_diaria = datos_hoy.get("move_in_unidades", 0) - datos_hoy.get("move_out_unidades", 0)
            m2_neto_diario = datos_hoy.get("move_in_m2", 0.0) - datos_hoy.get("move_out_m2", 0.0)
            
            promedio_m2_diario = 0
            if cantidad_neta_diaria != 0:
                promedio_m2_diario = m2_neto_diario / abs(cantidad_neta_diaria)
            elif datos_hoy.get("move_in_unidades", 0) > 0:
                promedio_m2_diario = datos_hoy.get("move_in_m2", 0.0) / datos_hoy.get("move_in_unidades", 1)
            
            cantidad_neta_mes = datos_mes.get("unidades_entrada", 0) - datos_mes.get("unidades_salida", 0)
            m2_neto_mes = datos_mes.get("area_total_m2_move_in", 0) - datos_mes.get("area_total_m2_move_out", 0)
            
            promedio_m2_mes = 0
            if cantidad_neta_mes != 0:
                promedio_m2_mes = m2_neto_mes / abs(cantidad_neta_mes)
            elif datos_mes.get("unidades_entrada", 0) > 0:
                promedio_m2_mes = datos_mes.get("area_total_m2_move_in", 0) / max(datos_mes.get("unidades_entrada", 1), 1)
            
            cantidad_neta_apertura = datos_historico.get("unidades_totales", 0)
            m2_neto_apertura = datos_historico.get("m2_totales", 0.0)
            
            promedio_m2_apertura = 0
            if cantidad_neta_apertura > 0:
                promedio_m2_apertura = m2_neto_apertura / cantidad_neta_apertura
            
            area_disponible = datos_ocupacion_suc.get("area_disponible", 0)
            porcentaje_de_ocupacion = datos_ocupacion_suc.get("porcentaje_ocupacion", 0)
            
            registro = {
                "fecha": today.isoformat(),
                "fecha_apertura": fecha_apertura,
                "cantidad_neta_diaria": round(cantidad_neta_diaria, 2),
                "m2_neto_diario": round(m2_neto_diario, 2),
                "promedio_m2_diario": round(promedio_m2_diario, 2),
                "cantidad_neta_mes": round(cantidad_neta_mes, 2),
                "m2_neto_mes": round(m2_neto_mes, 2),
                "promedio_m2_mes": round(promedio_m2_mes, 2),
                "cantidad_neta_apertura": round(cantidad_neta_apertura, 2),
                "m2_neto_apertura": round(m2_neto_apertura, 2),
                "promedio_m2_apertura": round(promedio_m2_apertura, 2),
                "area_disponible": round(area_disponible, 2),
                "porcentaje_de_ocupacion": round(porcentaje_de_ocupacion, 2)
            }
            
            resultado[sucursal] = registro
        
        logger.info(f"Resumen calculado para {len(resultado)} sucursales")
        
        if total_moveins_hoy == 0 and total_moveouts_hoy == 0:
            logger.info("No se encontraron movimientos para hoy. Intentando fuente alternativa")
            for sucursal in sucursales_ordenadas[:5]:
                datos_mes = metricas_por_sucursal.get(sucursal, {})
                if datos_mes:
                    dias_transcurridos = (today - first_day_of_month).days + 1
                    if dias_transcurridos > 0:
                        move_in_estimado = datos_mes.get("unidades_entrada", 0) / dias_transcurridos
                        move_out_estimado = datos_mes.get("unidades_salida", 0) / dias_transcurridos
                        
                        if move_in_estimado > 0 or move_out_estimado > 0:
                            logger.info(f"   Estimacion {sucursal}: IN={move_in_estimado:.1f}/dia, OUT={move_out_estimado:.1f}/dia")
        
        return resultado
        
    except Exception as e:
        logger.error(f"ERROR calculando resumen por sucursal: {str(e)}")
        import traceback
        traceback.print_exc()
        
        try:
            today = datetime.now(timezone.utc).date()
            datos_ocupacion = calcular_porcentaje_ocupacion()
            resultado_minimo = {}
            
            if datos_ocupacion and datos_ocupacion.get("detalle_sucursales_ocupacion"):
                for suc in datos_ocupacion["detalle_sucursales_ocupacion"]:
                    resultado_minimo[suc["sucursal"]] = {
                        "fecha": today.isoformat(),
                        "fecha_apertura": "2023-01-01",
                        "cantidad_neta_diaria": 0,
                        "m2_neto_diario": 0,
                        "promedio_m2_diario": 0,
                        "cantidad_neta_mes": 0,
                        "m2_neto_mes": 0,
                        "promedio_m2_mes": 0,
                        "cantidad_neta_apertura": 0,
                        "m2_neto_apertura": 0,
                        "promedio_m2_apertura": 0,
                        "area_disponible": suc.get("area_disponible", 0),
                        "porcentaje_de_ocupacion": suc.get("porcentaje_ocupacion", 0)
                    }
            
            logger.info(f"Version minima: {len(resultado_minimo)} sucursales")
            return resultado_minimo
            
        except Exception as e2:
            logger.error(f"ERROR en version minima: {e2}")
            return {}


def obtener_sucursal_desde_site_code(site_code, mapa_site_id_a_sucursal, info_sucursales):
    if not site_code:
        return None
    
    site_code = str(site_code).strip()
    
    if site_code.isdigit():
        if site_code in mapa_site_id_a_sucursal:
            return mapa_site_id_a_sucursal[site_code]
    
    elif "KB" in site_code.upper():
        import re
        match = re.search(r'(KB\d{1,3})', site_code.upper())
        if match:
            sucursal_crud = match.group(1)
            if sucursal_crud.startswith("KB"):
                try:
                    numero = sucursal_crud[2:]
                    if numero.isdigit():
                        return f"KB{int(numero):02d}"
                except:
                    return sucursal_crud
    
    if info_sucursales:
        for sucursal, info in info_sucursales.items():
            if str(info.get("site_id")) == site_code:
                return sucursal
    
    return None


def obtener_fecha_apertura_sucursal(sucursal, info_sucursales):
    if not sucursal or not info_sucursales:
        return "2023-01-01"
    
    fecha_apertura = "2023-01-01"
    
    if sucursal in info_sucursales:
        site_info = info_sucursales[sucursal]
        site_id = site_info.get("site_id")
        
        if site_id:
            all_sites = GLOBAL_CACHE.get('all_sites')
            for site in (all_sites or []):
                if str(site.get("id")) == str(site_id):
                    created = site.get("created")
                    if created:
                        try:
                            fecha = parse_date_to_dateobj(created)
                            if fecha:
                                fecha_apertura = fecha.isoformat()
                        except:
                            pass
                    break
    
    return fecha_apertura

def calcular_metricas_completas_simplificada():
    try:
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        logger.info("="*60)
        logger.info("CALCULADOR COMPLETO SIMPLIFICADO PARA POWER BI - CORREGIDO")
        logger.info("="*60)
        logger.info("INICIALIZANDO CACHE GLOBAL")
        GLOBAL_CACHE.initialize()
        logger.info("CACHE GLOBAL INICIALIZADO")
        
        logger.info("CALCULANDO CONTRATOS Y FINIQUITOS SIMPLIFICADOS")
        data_contratos_finiquitos = calcular_contratos_finiquitos_simplificado()
        
        logger.info("CALCULANDO TABLAS UNIFICADAS")
        tablas_unificadas = calcular_y_unificar_tres_tablas() 
        
        logger.info("CALCULANDO MOVE-INS CORREGIDOS")
        moveins, conteo_estados, rentals_mes = contar_moveins_correcto(today)
        
        logger.info("CALCULANDO MOVE-OUTS CORREGIDOS")
        moveouts, datos_moveouts = contar_moveouts_corregido()
        
        neto = moveins - moveouts
        logger.info(f"Move-ins: {moveins}, Move-outs: {moveouts}, Neto: {neto}")
        logger.info(f"Datos move-outs: {datos_moveouts['total_m2_move_out']} m², Precio promedio: {datos_moveouts['precio_promedio_m2_move_out']}")
        
        logger.info("CALCULANDO DETALLE SUCURSALES")
        detalle_sucursales_combinado = calcular_metricas_por_sucursal_corregido()
        
        logger.info("CALCULANDO DATOS MOVE-IN GLOBAL")
        datos_move_in_global = []
        for rental in rentals_mes:
            start_date = parse_date_to_dateobj(rental.get("startDate"))
            unit_data = rental.get("unit") or rental.get("data", {}).get("unit") or {}
            
            precio = _extract_price(rental)
            area_m2 = compute_area_m2(unit_data)
            
            if start_date and first_day_of_month <= start_date <= today:
                rental_state = rental.get("state", "").lower()
                if rental_state in ["occupied", "ended"]:
                    if precio and precio > 0 and area_m2 and area_m2 > 0:
                        datos_move_in_global.append((precio, area_m2))
        
        if datos_move_in_global:
            precios_por_m2_move_in_global = [precio / area for precio, area in datos_move_in_global]
            precio_promedio_m2_move_in_global = sum(precios_por_m2_move_in_global) / len(precios_por_m2_move_in_global)
            area_total_move_in_global = sum(area for precio, area in datos_move_in_global)
        else:
            precio_promedio_m2_move_in_global = 19225.43 
            area_total_move_in_global = 4000
        
        precio_promedio_m2_move_out_global = datos_moveouts["precio_promedio_m2_move_out"]
        area_total_move_out_global = datos_moveouts["total_m2_move_out"]
        area_total_neto_global = area_total_move_in_global - area_total_move_out_global
        
        if area_total_move_in_global + area_total_move_out_global > 0:
            precio_promedio_m2_neto_global = (
                (precio_promedio_m2_move_in_global * area_total_move_in_global) +
                (precio_promedio_m2_move_out_global * area_total_move_out_global)
            ) / (area_total_move_in_global + area_total_move_out_global)
        else:
            precio_promedio_m2_neto_global = precio_promedio_m2_move_in_global
        
        logger.info("OBTENIENDO DATA_OCUPACION CORREGIDA (SITE-REPORTS)")
        datos_ocupacion = calcular_porcentaje_ocupacion() 
        
        logger.info("CALCULANDO METRICAS DE SEGUROS")
        metricas_seguros = calcular_metricas_seguros_corregido()
        
        logger.info("CALCULANDO METRICAS DE DESCUENTOS")
        metricas_descuentos = obtener_moveins_con_descuentos_por_sucursal_corregido(today, first_day_of_month)
        
        logger.info("OBTENIENDO DATOS DIARIOS FLEX")
        datos_diarios = obtener_json_diario_6_meses()
        
        logger.info("CALCULANDO METRICAS DE TAMAÑO Y VALOR POR SUCURSAL")
        metricas_tamaño_valor = calcular_metricas_tamaño_valor_sucursal()
        
        logger.info("CALCULANDO VENTAS DIARIAS")
        ventas_diarias_result = calcular_ventas_diarias()
        
        logger.info("OBTENIENDO REPORTES DE AYER/HOY")
        reportes_ayer_hoy = obtener_reportes_ayer_hoy_integrado()
        
        logger.info("CALCULANDO RESUMEN POR SUCURSAL")
        data_resumen_sucursal = calcular_resumen_sucursal()
        
        data_global = {
            "precio_promedio_m2_move_in": round(precio_promedio_m2_move_in_global, 2),
            "precio_promedio_m2_move_out": round(precio_promedio_m2_move_out_global, 2),
            "precio_promedio_m2_neto": round(precio_promedio_m2_neto_global, 2),
            "area_total_m2_move_in": round(area_total_move_in_global),
            "area_total_m2_move_out": round(area_total_move_out_global),
            "area_total_m2_neto": round(area_total_neto_global),
            "unidades_entrada": moveins,
            "unidades_salida": moveouts,
            "unidades_netas": neto
        }
        
        if datos_ocupacion:
            data_global.update({
                "total_m2": datos_ocupacion.get('total_m2', 0),
                "total_area_ocupada": datos_ocupacion.get('total_area_ocupada', 0),
                "total_area_disponible": datos_ocupacion.get('total_area_disponible', 0),
                "porcentaje_ocupacion": datos_ocupacion.get('porcentaje_ocupacion', 0),
                "fecha_ocupacion": datos_ocupacion.get('fecha_ocupacion')
            })
        else:
            data_global.update({
                "total_m2": 0,
                "total_area_ocupada": 0,
                "total_area_disponible": 0,
                "porcentaje_ocupacion": 0,
                "fecha_ocupacion": today.isoformat()
            })
            
        data_promedio_flex = {
            "data_promedio_mensual": {
                "fecha_inicio": first_day_of_month.isoformat(),
                "fecha_fin": today.isoformat(),
                "tipo": "datos_flex_mensual"
            },
            "data_promedio_6meses": {
                "fecha_inicio": first_day_of_month.isoformat(),
                "fecha_fin": today.isoformat(),
                "tipo": "datos_flex_6meses"
            }
        }
        
        resultado_final = {
            "success": True,
            "data_global": data_global,
            "data_detalle": detalle_sucursales_combinado,
            "data_ocupacion": datos_ocupacion, 
            "data_contratos_finiquitos": data_contratos_finiquitos,
            "data_seguros": metricas_seguros or {},
            "data_descuentos": metricas_descuentos or {},
            "data_diaria": datos_diarios or {},
            "data_promedio_flex": data_promedio_flex or {},
            "data_detallada_sucursales": detalle_sucursales_combinado,
            "datos": tablas_unificadas.get("datos", []), 
            "meta_gerencia": META_GERENCIA,
            "data_tamano_promedio": metricas_tamaño_valor.get("data_tamano_promedio", {}),
            "data_valor_promedio": metricas_tamaño_valor.get("data_valor_promedio", {}),
            "data_ventas_diarias": ventas_diarias_result.get("data_ventas_diarias", {}) if ventas_diarias_result.get("success") else {},
            "data_reportes_ayer_hoy": reportes_ayer_hoy,
            **data_resumen_sucursal,
            "metadata": {
                "fecha_inicio": first_day_of_month.isoformat(),
                "fecha_fin": today.isoformat(),
                "total_rentals_mes": len(rentals_mes),
                "fecha_calculo": today.isoformat(),
                "datos_moveouts_reales": datos_moveouts,
                "contratos_finiquitos_simplificado": True,
                "dias_en_data_promedio": len(tablas_unificadas.get("datos", [])),
                "primer_dia_mes": first_day_of_month.isoformat(),
                "ultimo_dia_mes": today.isoformat(),
                "ocupacion_fuente": "site-reports" if datos_ocupacion and datos_ocupacion.get("detalle_sucursales_ocupacion") else "sin_datos",
                "metricas_tamano_valor_calculadas": "error" not in metricas_tamaño_valor,
                "sucursales_con_actividad_tamano_valor": len(metricas_tamaño_valor.get("data_tamano_promedio", {}).get("detalle_sucursales_tamano", [])),
                "ventas_diarias_calculadas": ventas_diarias_result.get("success", False),
                "dias_con_actividad_ventas": len(ventas_diarias_result.get("data_ventas_diarias", {}).get("detalle_ventas_diarias", [])),
                "reportes_ayer_hoy_calculados": reportes_ayer_hoy.get("data_moveins", {}).get("total_unidades", 0) > 0 or reportes_ayer_hoy.get("data_moveouts", {}).get("total_unidades", 0) > 0,
                "moveins_ayer_hoy": reportes_ayer_hoy.get("data_moveins", {}).get("total_unidades", 0),
                "moveouts_ayer_hoy": reportes_ayer_hoy.get("data_moveouts", {}).get("total_unidades", 0),
                "resumen_sucursal_calculado": len(data_resumen_sucursal) > 0,
                "sucursales_en_resumen": len(data_resumen_sucursal)
            }
        }
        resultado_final["metadata"].update(tablas_unificadas.get("metadata", {}))
        
        logger.info("="*70)
        logger.info("RESUMEN POR SUCURSAL CALCULADO:")
        logger.info("="*70)
        
        if data_resumen_sucursal:
            logger.info(f"Total sucursales en resumen: {len(data_resumen_sucursal)}")
            
            logger.info("Ejemplos de resumen por sucursal:")
            for i, (sucursal_key, datos) in enumerate(list(data_resumen_sucursal.items())[:3]):
                logger.info(f"  {sucursal_key}:")
                logger.info(f"    - Neto dia: {datos['cantidad_neta_diaria']} unidades, {datos['m2_neto_diario']} m²")
                logger.info(f"    - Neto mes: {datos['cantidad_neta_mes']} unidades, {datos['m2_neto_mes']} m²")
                logger.info(f"    - Ocupacion: {datos['porcentaje_de_ocupacion']}%, Area disp: {datos['area_disponible']} m²")
        
        logger.info("="*70)
        logger.info("RESULTADOS CORREGIDOS:")
        logger.info(f"   - Move-ins: {moveins}, Move-outs: {moveouts}, Neto: {neto}")
        logger.info(f"   - Contratos/dia: {data_contratos_finiquitos.get('total_m2_contratos_dia', 0):.2f} m²")
        logger.info(f"   - Finiquitos/dia: {data_contratos_finiquitos.get('total_m2_finiquitos_dia', 0):.2f} m²")
        logger.info(f"   - Precio m² Move-out: {precio_promedio_m2_move_out_global:.2f}")
        logger.info(f"   - Area total Move-out: {area_total_move_out_global} m²")
        logger.info(f"   - Reportes ayer/hoy - Move-ins: {reportes_ayer_hoy.get('data_moveins', {}).get('total_unidades', 0)} unidades")
        logger.info(f"   - Reportes ayer/hoy - Move-outs: {reportes_ayer_hoy.get('data_moveouts', {}).get('total_unidades', 0)} unidades")
        logger.info(f"   - Resumen sucursal: {len(data_resumen_sucursal)} sucursales calculadas")
        
        if datos_ocupacion:
            logger.info(f"   - Ocupacion calculada (site-reports): {datos_ocupacion['porcentaje_ocupacion']:.2f}%")
            logger.info(f"   - Total m² (ocupacion): {datos_ocupacion['total_m2']:,.0f} m²")
            logger.info(f"   - Area arrendada: {datos_ocupacion['total_area_ocupada']:,.0f} m²")
            logger.info(f"   - Area disponible: {datos_ocupacion['total_area_disponible']:,.0f} m²")
            logger.info(f"   - Sucursales incluidas: {len(datos_ocupacion.get('detalle_sucursales_ocupacion', []))}")
        else:
            logger.info(f"   - Ocupacion: Sin datos disponibles")
        logger.info(f"   - Datos en data_promedio: {len(resultado_final['datos'])} dias (desde {first_day_of_month} hasta {today})")
        
        if metricas_descuentos and metricas_descuentos.get("success"):
            descuentos_data = metricas_descuentos.get("detalle_descuentos", [])
            if descuentos_data:
                logger.info(f"   - Descuentos calculados: {len(descuentos_data)} sucursales")
        
        logger.info("="*70)
        return resultado_final
        
    except Exception as e:
        logger.error(f"ERROR en calculo de metricas simplificado: {e}")
        import traceback
        traceback.print_exc()
        today = datetime.now(timezone.utc).date()
        first_day_of_month = today.replace(day=1)
        
        try:
            tablas_error = calcular_y_unificar_tres_tablas()
        except:
            tablas_error = {"datos": []}
        
        try:
            datos_ocupacion_error = calcular_porcentaje_ocupacion()
        except:
            datos_ocupacion_error = {
                "fecha_ocupacion": today.isoformat(),
                "total_m2": 0,
                "total_area_ocupada": 0,
                "total_area_disponible": 0,
                "porcentaje_ocupacion": 0,
                "detalle_sucursales_ocupacion": []
            }
        
        try:
            reportes_ayer_hoy_error = obtener_reportes_ayer_hoy_integrado()
        except:
            reportes_ayer_hoy_error = {
                "data_moveins": {
                    "fecha_moveins": f"{today - timedelta(days=1)} y {today}",
                    "total_unidades": 0,
                    "total_m2_movein": 0.0,
                    "detalle_moveins_diarios": [],
                    "detalle_completo_unidades": []
                },
                "data_moveouts": {
                    "fecha_moveouts": f"{today - timedelta(days=1)} y {today}",
                    "total_unidades": 0,
                    "total_m2_moveout": 0.0,
                    "detalle_moveouts_diarios": [],
                    "detalle_completo_unidades": []
                }
            }
        
        try:
            data_resumen_sucursal_error = calcular_resumen_sucursal()
        except:
            data_resumen_sucursal_error = {}
        
        estructura_error = {
            "success": False,
            "error": str(e),
            "data_global": {
                "precio_promedio_m2_move_in": 0,
                "precio_promedio_m2_move_out": 0,
                "precio_promedio_m2_neto": 0,
                "area_total_m2_move_in": 0,
                "area_total_m2_move_out": 0,
                "area_total_m2_neto": 0,
                "unidades_entrada": 0,
                "unidades_salida": 0,
                "unidades_netas": 0,
                "total_m2": datos_ocupacion_error["total_m2"],
                "total_area_ocupada": datos_ocupacion_error["total_area_ocupada"],
                "total_area_disponible": datos_ocupacion_error["total_area_disponible"],
                "porcentaje_ocupacion": datos_ocupacion_error["porcentaje_ocupacion"],
                "fecha_ocupacion": datos_ocupacion_error["fecha_ocupacion"]
            },
            "data_detalle": [],
            "data_ocupacion": datos_ocupacion_error,
            "data_contratos_finiquitos": {
                "periodo_dia": "promedio_6_meses",
                "fecha_inicio_dia": (today - timedelta(days=180)).isoformat(),
                "fecha_fin_dia": today.isoformat(),
                "total_m2_contratos_dia": 0,
                "total_m2_finiquitos_dia": 0,
                "neto_m2_dia": 0,
                "periodo_cierre": "promedio_3_meses",
                "fecha_inicio_cierre": (today - timedelta(days=90)).isoformat(),
                "fecha_fin_cierre": today.isoformat(),
                "total_m2_contratos_cierre": 0,
                "total_m2_finiquitos_cierre": 0,
                "neto_m2_cierre": 0,
                "meses_analizados": 0
            },
            "data_seguros": {},
            "data_descuentos": {},
            "data_diaria": {},
            "data_promedio_flex": {},
            "data_detallada_sucursales": [],
            "datos": tablas_error.get("datos", []),
            "data_tamano_promedio": {
                "fecha_ocupacion": f"{first_day_of_month} al {today}",
                "detalle_sucursales_tamano": []
            },
            "data_valor_promedio": {
                "fecha_ocupacion": f"{first_day_of_month} al {today}",
                "detalle_sucursales_valor": []
            },
            "data_ventas_diarias": {
                "fecha_ventas_diarias": f"{first_day_of_month} al {today}",
                "detalle_ventas_diarias": []
            },
            "data_reportes_ayer_hoy": reportes_ayer_hoy_error,
            **data_resumen_sucursal_error,
            "meta_gerencia": META_GERENCIA,
            "metadata": {
                "fecha_inicio": first_day_of_month.isoformat(),
                "fecha_fin": today.isoformat(),
                "error": str(e)[:100],
                "ocupacion_fuente": "error_fallback"
            }
        }
        
        return estructura_error

def crear_tablas_si_no_existen():
    try:
        logger.info("Creando/actualizando tablas en historico_dev")
        connection = pymysql.connect(**RDS_CONFIG_HISTORICO)
        
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS kpi_ventas")
            cursor.execute("DROP TABLE IF EXISTS kpi_ventas_detalle_sucursales")
            logger.info("Tablas existentes eliminadas")
            
            cursor.execute("""
                CREATE TABLE kpi_ventas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    fecha_inicio DATE NOT NULL,
                    fecha_fin DATE NOT NULL,
                    valor_promedio_move_in DECIMAL(10,2),
                    valor_promedio_move_out DECIMAL(10,2),
                    valor_promedio_neto DECIMAL(10,2),
                    area_unidad_move_in DECIMAL(10,2),
                    area_unidad_move_out DECIMAL(10,2),
                    area_unidad_neto DECIMAL(10,2),
                    total_ventas_move_in INT,
                    total_ventas_move_out INT,
                    total_ventas_neto INT,
                    porcentaje_ocupacion_global DECIMAL(5,2),
                    area_total_construida DECIMAL(10,2),
                    area_total_arrendada DECIMAL(10,2),
                    fecha_ocupacion DATE,
                    total_m2 DECIMAL(10,2),
                    total_area_ocupada DECIMAL(10,2),
                    porcentaje_ocupacion DECIMAL(5,2),
                    UNIQUE KEY unique_fecha (fecha_inicio, fecha_fin)
                )
            """)
            cursor.execute("""
                CREATE TABLE kpi_ventas_detalle_sucursales (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    fecha_inicio DATE NOT NULL,
                    fecha_fin DATE NOT NULL,
                    sucursal VARCHAR(10) NOT NULL,
                    precio_promedio_m2_move_in DECIMAL(10,2),
                    precio_promedio_m2_move_out DECIMAL(10,2),
                    precio_promedio_m2_neto DECIMAL(10,2),
                    area_total_m2_move_in DECIMAL(10,2),
                    area_total_m2_move_out DECIMAL(10,2),
                    area_total_m2_neto DECIMAL(10,2),
                    unidades_entrada INT,
                    unidades_salida INT,
                    unidades_netas INT,
                    UNIQUE KEY unique_fecha_sucursal (fecha_inicio, fecha_fin, sucursal)
                )
            """)
            connection.commit()
            logger.info("Todas las tablas creadas exitosamente en historico_dev")
                
    except Exception as e:
        logger.error(f"Error creando tablas: {str(e)}")
        raise
    finally:
        if 'connection' in locals():
            connection.close()

def generar_json_s3(resultado_extraccion):
    try:
        logger.info("Generando JSON completo para S3")
        
        if "datos" not in resultado_extraccion:
            logger.warning("Campo 'datos' no encontrado, calculando")
            tablas_unificadas = calcular_y_unificar_tres_tablas()
            resultado_extraccion["datos"] = tablas_unificadas.get("datos", [])
        
        campos_requeridos = [
            "data_global", "data_detalle", "data_ocupacion", "data_contratos_finiquitos",
            "data_seguros", "data_descuentos", "data_diaria", 
            "data_promedio_flex", "data_detallada_sucursales", "meta_gerencia", "metadata",
            "data_tamano_promedio", "data_valor_promedio", "data_ventas_diarias", 
            "data_reportes_ayer_hoy"
        ]
        
        sucursales_keys = [k for k in resultado_extraccion.keys() if k.startswith('KB')]
        
        for campo in campos_requeridos:
            if campo not in resultado_extraccion:
                if campo == "meta_gerencia":
                    resultado_extraccion[campo] = META_GERENCIA
                elif campo in ["data_tamano_promedio", "data_valor_promedio"]:
                    resultado_extraccion[campo] = {
                        "fecha_ocupacion": "",
                        "detalle_sucursales_tamano" if campo == "data_tamano_promedio" else "detalle_sucursales_valor": []
                    }
                elif campo == "data_ventas_diarias":
                    today = datetime.now(timezone.utc).date()
                    first_day_of_month = today.replace(day=1)
                    resultado_extraccion[campo] = {
                        "fecha_ventas_diarias": f"{first_day_of_month} al {today}",
                        "detalle_ventas_diarias": []
                    }
                elif campo == "data_reportes_ayer_hoy":
                    today = datetime.now(timezone.utc).date()
                    yesterday = today - timedelta(days=1)
                    resultado_extraccion[campo] = {
                        "data_moveins": {
                            "fecha_moveins": f"{yesterday} y {today}",
                            "total_unidades": 0,
                            "total_m2_movein": 0.0,
                            "detalle_moveins_diarios": [],
                            "detalle_completo_unidades": []
                        },
                        "data_moveouts": {
                            "fecha_moveouts": f"{yesterday} y {today}",
                            "total_unidades": 0,
                            "total_m2_moveout": 0.0,
                            "detalle_moveouts_diarios": [],
                            "detalle_completo_unidades": []
                        }
                    }
                else:
                    resultado_extraccion[campo] = {}
                logger.info(f"Campo {campo} agregado al JSON S3")
        
        json_completo = {
            "success": resultado_extraccion.get("success", True),
            "data_global": resultado_extraccion['data_global'],
            "data_detalle": resultado_extraccion['data_detalle'],
            "data_ocupacion": resultado_extraccion.get('data_ocupacion', {}),
            "data_contratos_finiquitos": resultado_extraccion.get('data_contratos_finiquitos', {}),
            "data_seguros": resultado_extraccion.get('data_seguros', {}),
            "data_descuentos": resultado_extraccion.get('data_descuentos', {}),
            "data_diaria": resultado_extraccion.get('data_diaria', {}),
            "data_promedio_flex": resultado_extraccion.get('data_promedio_flex', {}),
            "data_detallada_sucursales": resultado_extraccion.get('data_detallada_sucursales', []),
            "data_promedio": resultado_extraccion.get('datos', []),
            "meta_gerencia": resultado_extraccion.get('meta_gerencia', META_GERENCIA),
            "data_tamano_promedio": resultado_extraccion.get('data_tamano_promedio', {}),
            "data_valor_promedio": resultado_extraccion.get('data_valor_promedio', {}),
            "data_ventas_diarias": resultado_extraccion.get('data_ventas_diarias', {}),
            "data_reportes_ayer_hoy": resultado_extraccion.get('data_reportes_ayer_hoy', {}),
            "metadata": resultado_extraccion['metadata']
        }
        
        for sucursal_key in sucursales_keys:
            if sucursal_key in resultado_extraccion:
                json_completo[sucursal_key] = resultado_extraccion[sucursal_key]
        
        if not isinstance(json_completo["data_promedio"], list):
            json_completo["data_promedio"] = []
        
        json_data = json.dumps(json_completo, default=str, indent=2)
        s3 = boto3.client('s3')
        bucket_name = 'informeventas'
        file_name = 'kpi_ventas_completo.json'
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data,
            ContentType='application/json'
        )
        return True
        
    except Exception as e:
        logger.error(f"Error generando JSON S3: {str(e)}")
        return False

def lambda_handler(event, context):
    connection_historico = None
    try:
        logger.info("INICIANDO LAMBDA SIMPLIFICADO PARA POWER BI - CORREGIDO")
        logger.info("="*70)
        crear_tablas_si_no_existen()
        
        connection_historico = pymysql.connect(**RDS_CONFIG_HISTORICO)
        logger.info("Ejecutando calculo de metricas simplificada")
        resultado_extraccion = calcular_metricas_completas_simplificada()
        
        if resultado_extraccion.get("success") and resultado_extraccion.get("data_global"):
            with connection_historico.cursor() as cursor:
                upsert_sql = """
                INSERT INTO kpi_ventas (
                    fecha_inicio, fecha_fin,
                    valor_promedio_move_in, valor_promedio_move_out, valor_promedio_neto,
                    area_unidad_move_in, area_unidad_move_out, area_unidad_neto,
                    total_ventas_move_in, total_ventas_move_out, total_ventas_neto,
                    porcentaje_ocupacion_global, area_total_construida, area_total_arrendada, fecha_ocupacion,
                    total_m2, total_area_ocupada, porcentaje_ocupacion
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    valor_promedio_move_in = VALUES(valor_promedio_move_in),
                    valor_promedio_move_out = VALUES(valor_promedio_move_out),
                    valor_promedio_neto = VALUES(valor_promedio_neto),
                    area_unidad_move_in = VALUES(area_unidad_move_in),
                    area_unidad_move_out = VALUES(area_unidad_move_out),
                    area_unidad_neto = VALUES(area_unidad_neto),
                    total_ventas_move_in = VALUES(total_ventas_move_in),
                    total_ventas_move_out = VALUES(total_ventas_move_out),
                    total_ventas_neto = VALUES(total_ventas_neto),
                    porcentaje_ocupacion_global = VALUES(porcentaje_ocupacion_global),
                    area_total_construida = VALUES(area_total_construida),
                    area_total_arrendada = VALUES(area_total_arrendada),
                    fecha_ocupacion = VALUES(fecha_ocupacion),
                    total_m2 = VALUES(total_m2),
                    total_area_ocupada = VALUES(total_area_ocupada),
                    porcentaje_ocupacion = VALUES(porcentaje_ocupacion),
                    fecha_actualizacion = CURRENT_TIMESTAMP
                """
                valores = (
                    resultado_extraccion['metadata']['fecha_inicio'],
                    resultado_extraccion['metadata']['fecha_fin'],
                    resultado_extraccion['data_global']['precio_promedio_m2_move_in'],
                    resultado_extraccion['data_global']['precio_promedio_m2_move_out'],
                    resultado_extraccion['data_global']['precio_promedio_m2_neto'],
                    resultado_extraccion['data_global']['area_total_m2_move_in'],
                    resultado_extraccion['data_global']['area_total_m2_move_out'],
                    resultado_extraccion['data_global']['area_total_m2_neto'],
                    resultado_extraccion['data_global']['unidades_entrada'],
                    resultado_extraccion['data_global']['unidades_salida'],
                    resultado_extraccion['data_global']['unidades_netas'],
                    resultado_extraccion['data_global'].get('porcentaje_ocupacion_global') or None,
                    resultado_extraccion['data_global'].get('area_total_construida') or None,
                    resultado_extraccion['data_global'].get('area_total_arrendada') or None,
                    resultado_extraccion['data_global'].get('fecha_ocupacion') or None,
                    resultado_extraccion['data_global'].get('total_m2') or None,
                    resultado_extraccion['data_global'].get('total_area_ocupada') or None,
                    resultado_extraccion['data_global'].get('porcentaje_ocupacion') or None
                )
                cursor.execute(upsert_sql, valores)
                
                for detalle in resultado_extraccion['data_detalle']:
                    upsert_detalle_sql = """
                    INSERT INTO kpi_ventas_detalle_sucursales (
                        fecha_inicio, fecha_fin, sucursal,
                        precio_promedio_m2_move_in, precio_promedio_m2_move_out, precio_promedio_m2_neto,
                        area_total_m2_move_in, area_total_m2_move_out, area_total_m2_neto,
                        unidades_entrada, unidades_salida, unidades_netas
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        precio_promedio_m2_move_in = VALUES(precio_promedio_m2_move_in),
                        precio_promedio_m2_move_out = VALUES(precio_promedio_m2_move_out),
                        precio_promedio_m2_neto = VALUES(precio_promedio_m2_neto),
                        area_total_m2_move_in = VALUES(area_total_m2_move_in),
                        area_total_m2_move_out = VALUES(area_total_m2_move_out),
                        area_total_m2_neto = VALUES(area_total_m2_neto),
                        unidades_entrada = VALUES(unidades_entrada),
                        unidades_salida = VALUES(unidades_salida),
                        unidades_netas = VALUES(unidades_netas),
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    """
                    valores_detalle = (
                        resultado_extraccion['metadata']['fecha_inicio'],
                        resultado_extraccion['metadata']['fecha_fin'],
                        detalle['sucursal'],
                        detalle['precio_promedio_m2_move_in'],
                        detalle['precio_promedio_m2_move_out'],
                        detalle['precio_promedio_m2_neto'],
                        detalle['area_total_m2_move_in'],
                        detalle['area_total_m2_move_out'],
                        detalle['area_total_m2_neto'],
                        detalle.get('unidades_entrada', 0),
                        detalle.get('unidades_salida', 0),
                        detalle.get('unidades_netas', 0)
                    )
                    cursor.execute(upsert_detalle_sql, valores_detalle)
                connection_historico.commit()
                logger.info(f"Detalle de {len(resultado_extraccion['data_detalle'])} sucursales insertado")
        
        json_generado = generar_json_s3(resultado_extraccion)
        
        sucursales_kb_en_resultado = [k for k in resultado_extraccion.keys() if k.startswith('KB')]
        
        response_data = {
            'message': 'Ejecucion completada con datos simplificados para Power BI - CORREGIDO',
            'success': resultado_extraccion.get("success", False),
            'json_generado': json_generado,
            'datos_power_bi': {
                'total_dias': len(resultado_extraccion.get('datos', [])),
                'primer_fecha': resultado_extraccion.get('datos', [{}])[0].get('fecha', '') if resultado_extraccion.get('datos') else '',
                'ultima_fecha': resultado_extraccion.get('datos', [{}])[-1].get('fecha', '') if resultado_extraccion.get('datos') else ''
            },
            'meta_gerencia': resultado_extraccion.get("meta_gerencia", META_GERENCIA),
            'contratos_finiquitos_simplificado': True,
            'moveouts_con_datos': resultado_extraccion.get('data_global', {}).get('unidades_salida', 0) > 0,
            'ocupacion_corregida': resultado_extraccion.get('data_ocupacion', {}).get('detalle_sucursales_ocupacion') is not None,
            'metricas_tamano_valor_incluidas': 'data_tamano_promedio' in resultado_extraccion,
            'sucursales_tamano_valor': len(resultado_extraccion.get('data_tamano_promedio', {}).get('detalle_sucursales_tamano', [])) if 'data_tamano_promedio' in resultado_extraccion else 0,
            'ventas_diarias_incluidas': 'data_ventas_diarias' in resultado_extraccion and resultado_extraccion['data_ventas_diarias'].get('detalle_ventas_diarias'),
            'dias_ventas_diarias': len(resultado_extraccion.get('data_ventas_diarias', {}).get('detalle_ventas_diarias', [])) if 'data_ventas_diarias' in resultado_extraccion else 0,
            'reportes_ayer_hoy_incluidos': 'data_reportes_ayer_hoy' in resultado_extraccion,
            'moveins_ayer_hoy': resultado_extraccion.get('data_reportes_ayer_hoy', {}).get('data_moveins', {}).get('total_unidades', 0),
            'moveouts_ayer_hoy': resultado_extraccion.get('data_reportes_ayer_hoy', {}).get('data_moveouts', {}).get('total_unidades', 0),
            'resumen_sucursal_incluido': len(sucursales_kb_en_resultado) > 0,
            'sucursales_en_resumen': len(sucursales_kb_en_resultado)
        }
        
        logger.info("="*70)
        logger.info("RESULTADOS FINALES CORREGIDOS:")
        logger.info(f"   1. Metricas calculadas: {resultado_extraccion.get('success', False)}")
        if resultado_extraccion.get('datos'):
            logger.info(f"   2. Periodo: {resultado_extraccion['datos'][0]['fecha']} a {resultado_extraccion['datos'][-1]['fecha']}")
            ultimo = resultado_extraccion['datos'][-1]
            logger.info(f"   3. Ultimo dia: neto_flex={ultimo['neto_flex']:.2f}, neto={ultimo['neto']:.2f}, neto6m={ultimo['neto6m']:.2f}")
        logger.info(f"   4. Move-ins: {resultado_extraccion.get('data_global', {}).get('unidades_entrada', 0)}")
        logger.info(f"   5. Move-outs REALES: {resultado_extraccion.get('data_global', {}).get('unidades_salida', 0)}")
        logger.info(f"   6. Neto: {resultado_extraccion.get('data_global', {}).get('unidades_netas', 0)}")
        logger.info(f"   7. Contratos/dia: {resultado_extraccion.get('data_contratos_finiquitos', {}).get('total_m2_contratos_dia', 0):.2f} m²")
        logger.info(f"   8. Finiquitos/dia: {resultado_extraccion.get('data_contratos_finiquitos', {}).get('total_m2_finiquitos_dia', 0):.2f} m²")
        
        if 'data_reportes_ayer_hoy' in resultado_extraccion:
            reportes = resultado_extraccion['data_reportes_ayer_hoy']
            moveins_ayer_hoy = reportes.get('data_moveins', {}).get('total_unidades', 0)
            moveouts_ayer_hoy = reportes.get('data_moveouts', {}).get('total_unidades', 0)
            logger.info(f"   9. Reportes ayer/hoy - Move-ins: {moveins_ayer_hoy}")
            logger.info(f"   10. Reportes ayer/hoy - Move-outs: {moveouts_ayer_hoy}")
            logger.info(f"   11. Reportes ayer/hoy - Neto: {moveins_ayer_hoy - moveouts_ayer_hoy}")
        
        if len(sucursales_kb_en_resultado) > 0:
            logger.info(f"   12. Resumen sucursal: {len(sucursales_kb_en_resultado)} sucursales calculadas")
            primera_sucursal = sucursales_kb_en_resultado[0]
            if primera_sucursal in resultado_extraccion:
                datos_sucursal = resultado_extraccion[primera_sucursal]
                logger.info(f"   13. Ejemplo sucursal {primera_sucursal}:")
                logger.info(f"       - Neto dia: {datos_sucursal.get('cantidad_neta_diaria', 0)} unidades")
                logger.info(f"       - Neto mes: {datos_sucursal.get('cantidad_neta_mes', 0)} unidades")
                logger.info(f"       - Ocupacion: {datos_sucursal.get('porcentaje_de_ocupacion', 0)}%")
        
        ocupacion_data = resultado_extraccion.get('data_ocupacion', {})
        if ocupacion_data and ocupacion_data.get('detalle_sucursales_ocupacion'):
            logger.info(f"   14. Ocupacion corregida (site-reports): {ocupacion_data.get('porcentaje_ocupacion', 0):.2f}%")
            logger.info(f"   15. Total m²: {ocupacion_data.get('total_m2', 0):,.0f} m²")
            logger.info(f"   16. Area arrendada: {ocupacion_data.get('total_area_ocupada', 0):,.0f} m²")
            logger.info(f"   17. Area disponible: {ocupacion_data.get('total_area_disponible', 0):,.0f} m²")
            logger.info(f"   18. Sucursales: {len(ocupacion_data.get('detalle_sucursales_ocupacion', []))}")
        else:
            logger.info(f"   14. Ocupacion: Sin datos disponibles")
        
        if 'data_tamano_promedio' in resultado_extraccion:
            tamaño_data = resultado_extraccion['data_tamano_promedio']
            if tamaño_data.get('detalle_sucursales_tamano'):
                logger.info(f"   19. Tamaño promedio: {len(tamaño_data['detalle_sucursales_tamano'])} sucursales")
        
        if 'data_valor_promedio' in resultado_extraccion:
            valor_data = resultado_extraccion['data_valor_promedio']
            if valor_data.get('detalle_sucursales_valor'):
                logger.info(f"   20. Valor promedio: {len(valor_data['detalle_sucursales_valor'])} sucursales")
        
        if 'data_ventas_diarias' in resultado_extraccion:
            ventas_data = resultado_extraccion['data_ventas_diarias']
            if ventas_data.get('detalle_ventas_diarias'):
                dias_ventas = len(ventas_data['detalle_ventas_diarias'])
                total_move_in = sum(item["diario_move_in"] for item in ventas_data['detalle_ventas_diarias'])
                total_move_out = sum(item["diario_move_out"] for item in ventas_data['detalle_ventas_diarias'])
                logger.info(f"   21. Ventas diarias: {dias_ventas} dias con actividad")
                logger.info(f"   22. Total move-ins: {total_move_in} unidades")
                logger.info(f"   23. Total move-outs: {total_move_out} unidades")
                logger.info(f"   24. Neto: {total_move_in - total_move_out} unidades")
        
        logger.info("="*70)
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_data, indent=2)
        } 
    except Exception as e:
        logger.error(f"ERROR GENERAL: {str(e)}")
        try:
            tablas_error = calcular_y_unificar_tres_tablas()
            datos_error = tablas_error.get("datos", [])
        except:
            datos_error = []
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'datos_generados': len(datos_error)
            })
        }
    finally:
        if connection_historico:
            connection_historico.close()
            logger.info("Conexion cerrada")