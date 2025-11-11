import json
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.dynamodb_helper import (
    obtener_pedido,
    marcar_empleado_libre
)

def lambda_handler(event, context):
    """Lambda para liberar todos los empleados asignados a un pedido"""
    print(f'Liberando empleados del pedido: {json.dumps(event)}')
    
    local_id = event.get('local_id')
    pedido_id = event.get('pedido_id')
    motivo = event.get('motivo', 'error_workflow')
    
    if not local_id or not pedido_id:
        print('Faltan parámetros, no se puede liberar empleados')
        return {'liberados': 0}
    
    try:
        # Obtener el pedido para ver qué empleados están asignados
        pedido = obtener_pedido(local_id, pedido_id)
        historial = pedido.get('historial_estados', [])
        
        empleados_liberados = []
        
        # Buscar en el historial los empleados que estaban activos
        for estado in historial:
            if estado.get('activo') and estado.get('empleado'):
                empleado_dni = estado['empleado']['dni']
                empleado_rol = estado['empleado']['rol']
                
                try:
                    marcar_empleado_libre(local_id, empleado_dni)
                    empleados_liberados.append({
                        'dni': empleado_dni,
                        'rol': empleado_rol
                    })
                    print(f'Empleado {empleado_rol} {empleado_dni} liberado por {motivo}')
                except Exception as e:
                    print(f'Error liberando empleado {empleado_dni}: {str(e)}')
        
        print(f'Total empleados liberados: {len(empleados_liberados)}')
        
        return {
            'liberados': len(empleados_liberados),
            'empleados': empleados_liberados,
            'motivo': motivo
        }
        
    except Exception as e:
        print(f'Error al liberar empleados: {str(e)}')
        return {'liberados': 0, 'error': str(e)}
