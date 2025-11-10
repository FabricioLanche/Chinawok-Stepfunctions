import json
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.dynamodb_helper import (
    buscar_empleado_disponible,
    marcar_empleado_ocupado,
    marcar_empleado_libre,
    actualizar_estado_pedido
)

def lambda_handler(event, context):
    """Lambda para asignar despachador y empacar el pedido"""
    print(f'Iniciando proceso de empacar: {json.dumps(event)}')
    
    local_id = event.get('local_id')
    pedido_id = event.get('pedido_id')
    cocinero_dni = event.get('cocinero_dni')
    
    if not local_id or not pedido_id:
        raise ValueError('Faltan parámetros requeridos: local_id o pedido_id')
    
    try:
        # Liberar al cocinero
        if cocinero_dni:
            marcar_empleado_libre(local_id, cocinero_dni)
        
        # Buscar despachador disponible
        despachador = buscar_empleado_disponible(local_id, 'Despachador')
        
        if not despachador:
            raise Exception('No hay despachadores disponibles en este momento')
        
        # Marcar despachador como ocupado
        marcar_empleado_ocupado(local_id, despachador['dni'])
        
        # Actualizar estado del pedido
        pedido_actualizado = actualizar_estado_pedido(
            local_id,
            pedido_id,
            'empacando',
            despachador
        )
        
        print(f"Pedido asignado a despachador {despachador['dni']}")
        
        result = {**event, **pedido_actualizado, 'despachador_dni': despachador['dni']}
        result.pop('cocinero_dni', None)
        
        return result
        
    except Exception as e:
        print(f'Error en lambda empacar: {str(e)}')
        raise
