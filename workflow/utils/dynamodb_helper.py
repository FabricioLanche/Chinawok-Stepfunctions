import boto3
import os
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
import random
import json

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def calcular_duracion_cocinado(productos, combos, modo_realista=False):
    """Calcula el tiempo de cocinado basado en cantidad de items"""
    # Tiempo base por producto individual (minutos)
    tiempo_por_producto = 5 if modo_realista else 0.5
    # Tiempo base por combo (minutos)
    tiempo_por_combo = 8 if modo_realista else 0.8
    # Tiempo de preparación base (minutos)
    tiempo_base = 10 if modo_realista else 1
    
    total_productos = sum(item.get('cantidad', 1) for item in productos) if productos else 0
    total_combos = sum(item.get('cantidad', 1) for item in combos) if combos else 0
    
    duracion = tiempo_base + (total_productos * tiempo_por_producto) + (total_combos * tiempo_por_combo)
    
    # Agregar variabilidad aleatoria (±20%)
    variacion = random.uniform(0.8, 1.2)
    duracion = duracion * variacion
    
    return int(duracion * 60)  # Convertir a segundos

def calcular_duracion_empacado(productos, combos, modo_realista=False):
    """Calcula el tiempo de empacado basado en cantidad de items"""
    tiempo_por_item = 2 if modo_realista else 0.3
    tiempo_base = 5 if modo_realista else 0.5
    
    total_items = 0
    if productos:
        total_items += sum(item.get('cantidad', 1) for item in productos)
    if combos:
        total_items += sum(item.get('cantidad', 1) for item in combos)
    
    duracion = tiempo_base + (total_items * tiempo_por_item)
    variacion = random.uniform(0.8, 1.2)
    duracion = duracion * variacion
    
    return int(duracion * 60)

def calcular_duracion_envio(modo_realista=False):
    """Calcula el tiempo de envío (más aleatorio por tráfico/distancia)"""
    if modo_realista:
        # Entre 15 y 45 minutos
        duracion = random.uniform(15, 45)
    else:
        # Entre 1 y 3 minutos
        duracion = random.uniform(1, 3)
    
    return int(duracion * 60)

def buscar_empleado_disponible(local_id, rol):
    """Busca un empleado disponible por rol"""
    table = dynamodb.Table(os.environ['TABLE_EMPLEADOS'])
    
    try:
        print(f'Buscando {rol} disponible en local {local_id}')
        
        response = table.query(
            KeyConditionExpression=Key('local_id').eq(local_id),
            FilterExpression=Attr('role').eq(rol) & Attr('ocupado').eq(False)
        )
        
        empleados = response.get('Items', [])
        
        print(f'Empleados encontrados con role={rol} y ocupado=False: {len(empleados)}')
        
        if empleados:
            print(f'Empleados disponibles: {json.dumps(empleados, default=str)}')
        
        if not empleados:
            print(f'No se encontraron {rol}s disponibles en local {local_id}')
            
            # Debug: buscar todos los empleados del rol sin filtro de ocupado
            response_all = table.query(
                KeyConditionExpression=Key('local_id').eq(local_id),
                FilterExpression=Attr('role').eq(rol)
            )
            todos = response_all.get('Items', [])
            print(f'Total de {rol}s en el local (sin filtro ocupado): {len(todos)}')
            if todos:
                for emp in todos:
                    print(f"  - {emp['dni']}: ocupado={emp.get('ocupado')} (tipo: {type(emp.get('ocupado'))})")
            
            return None
        
        # Retornar el empleado con mejor calificación
        empleado = sorted(
            empleados, 
            key=lambda x: float(x.get('calificacion_prom', 0)), 
            reverse=True
        )[0]
        
        print(f'Empleado {rol} seleccionado: {empleado["dni"]} - {empleado["nombre"]} {empleado["apellido"]} (calificación: {empleado.get("calificacion_prom")})')
        
        return empleado
        
    except Exception as e:
        print(f'Error buscando empleado: {str(e)}')
        import traceback
        print(f'Traceback: {traceback.format_exc()}')
        raise

def marcar_empleado_ocupado(local_id, dni):
    """Marca un empleado como ocupado"""
    table = dynamodb.Table(os.environ['TABLE_EMPLEADOS'])
    table.update_item(
        Key={'local_id': local_id, 'dni': dni},
        UpdateExpression='SET ocupado = :true',
        ExpressionAttributeValues={':true': True}
    )

def marcar_empleado_libre(local_id, dni):
    """Marca un empleado como libre"""
    table = dynamodb.Table(os.environ['TABLE_EMPLEADOS'])
    table.update_item(
        Key={'local_id': local_id, 'dni': dni},
        UpdateExpression='SET ocupado = :false',
        ExpressionAttributeValues={':false': False}
    )

def cerrar_estado_actual(local_id, pedido_id):
    """Cierra el estado actual del pedido"""
    table = dynamodb.Table(os.environ['TABLE_PEDIDOS'])
    now = datetime.utcnow().isoformat() + 'Z'
    
    response = table.get_item(Key={'local_id': local_id, 'pedido_id': pedido_id})
    pedido = response['Item']
    historial = pedido.get('historial_estados', [])
    
    # Cerrar el estado activo
    for h in historial:
        if h.get('activo'):
            h['activo'] = False
            h['hora_fin'] = now
    
    table.update_item(
        Key={'local_id': local_id, 'pedido_id': pedido_id},
        UpdateExpression='SET historial_estados = :historial',
        ExpressionAttributeValues={':historial': historial}
    )

def iniciar_nuevo_estado(local_id, pedido_id, nuevo_estado, empleado=None, duracion_segundos=0):
    """Inicia un nuevo estado en el historial del pedido"""
    table = dynamodb.Table(os.environ['TABLE_PEDIDOS'])
    now = datetime.utcnow()
    hora_inicio = now.isoformat() + 'Z'
    hora_fin = (now + timedelta(seconds=duracion_segundos)).isoformat() + 'Z'
    
    response = table.get_item(Key={'local_id': local_id, 'pedido_id': pedido_id})
    pedido = response['Item']
    historial = pedido.get('historial_estados', [])
    
    # Crear nuevo estado
    nuevo_historial_entry = {
        'estado': nuevo_estado,
        'hora_inicio': hora_inicio,
        'hora_fin': hora_fin,
        'activo': True,
        'empleado': None
    }
    
    if empleado:
        rol_map = {'Cocinero': 'cocinero', 'Despachador': 'despachador', 'Repartidor': 'repartidor'}
        nuevo_historial_entry['empleado'] = {
            'dni': empleado['dni'],
            'nombre_completo': f"{empleado['nombre']} {empleado['apellido']}",
            'rol': rol_map.get(empleado['role'], empleado['role'].lower()),
            'calificacion_prom': empleado.get('calificacion_prom', 0)
        }
    
    historial.append(nuevo_historial_entry)
    
    # Actualizar pedido con fecha de entrega aproximada si es el estado enviando
    update_expr = 'SET estado = :estado, historial_estados = :historial'
    expr_values = {
        ':estado': nuevo_estado,
        ':historial': historial
    }
    
    if nuevo_estado == 'enviando':
        update_expr += ', fecha_entrega_aproximada = :fecha'
        expr_values[':fecha'] = hora_fin
    
    response = table.update_item(
        Key={'local_id': local_id, 'pedido_id': pedido_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ReturnValues='ALL_NEW'
    )
    
    return response['Attributes']

def actualizar_estado_pedido(local_id, pedido_id, nuevo_estado, empleado=None):
    """Actualiza el estado del pedido y su historial"""
    table = dynamodb.Table(os.environ['TABLE_PEDIDOS'])
    now = datetime.utcnow().isoformat() + 'Z'
    
    # Obtener pedido actual
    response = table.get_item(Key={'local_id': local_id, 'pedido_id': pedido_id})
    pedido = response['Item']
    historial = pedido.get('historial_estados', [])
    
    # Desactivar estados anteriores
    for h in historial:
        if h.get('activo'):
            h['activo'] = False
            h['hora_fin'] = now
    
    # Agregar nuevo estado
    nuevo_historial_entry = {
        'estado': nuevo_estado,
        'hora_inicio': now,
        'hora_fin': now,
        'activo': True,
        'empleado': None
    }
    
    if empleado:
        rol_map = {'Cocinero': 'cocinero', 'Despachador': 'despachador', 'Repartidor': 'repartidor'}
        nuevo_historial_entry['empleado'] = {
            'dni': empleado['dni'],
            'nombre_completo': f"{empleado['nombre']} {empleado['apellido']}",
            'rol': rol_map.get(empleado['role'], empleado['role'].lower()),
            'calificacion_prom': empleado.get('calificacion_prom', 0)
        }
    
    historial.append(nuevo_historial_entry)
    
    # Actualizar pedido
    response = table.update_item(
        Key={'local_id': local_id, 'pedido_id': pedido_id},
        UpdateExpression='SET estado = :estado, historial_estados = :historial',
        ExpressionAttributeValues={
            ':estado': nuevo_estado,
            ':historial': historial
        },
        ReturnValues='ALL_NEW'
    )
    
    return response['Attributes']

def agregar_pedido_a_usuario(correo, pedido_id):
    """Agrega un pedido al historial del usuario"""
    table = dynamodb.Table(os.environ['TABLE_USUARIOS'])
    table.update_item(
        Key={'correo': correo},
        UpdateExpression='SET historial_pedidos = list_append(if_not_exists(historial_pedidos, :empty), :pedido)',
        ExpressionAttributeValues={
            ':empty': [],
            ':pedido': [pedido_id]
        }
    )
