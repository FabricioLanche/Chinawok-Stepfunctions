import json
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.dynamodb_helper import (
    buscar_empleado_disponible,
    marcar_empleado_ocupado,
    actualizar_estado_pedido
)

def lambda_handler(event, context):
    """Lambda para asignar cocinero y comenzar a cocinar el pedido"""
    print(f'Iniciando proceso de cocinar: {json.dumps(event)}')
    
    local_id = event.get('local_id')
    pedido_id = event.get('pedido_id')
    
    if not local_id or not pedido_id:
        raise ValueError('Faltan parámetros requeridos: local_id o pedido_id')
    
    try:
        # Buscar cocinero disponible
        cocinero = buscar_empleado_disponible(local_id, 'Cocinero')
        
        if not cocinero:
            raise Exception('No hay cocineros disponibles en este momento')
        
        # Marcar cocinero como ocupado
        marcar_empleado_ocupado(local_id, cocinero['dni'])
        
        # Actualizar estado del pedido
        pedido_actualizado = actualizar_estado_pedido(
            local_id,
            pedido_id,
            'cocinando',
            cocinero
        )
        
        print(f"Pedido asignado a cocinero {cocinero['dni']}")
        
        return {
            **event,
            **pedido_actualizado,
            'cocinero_dni': cocinero['dni']
        }
        
    except Exception as e:
        print(f'Error en lambda cocinar: {str(e)}')
        raise
