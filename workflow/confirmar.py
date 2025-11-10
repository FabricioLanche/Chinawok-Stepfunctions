import json
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.dynamodb_helper import (
    marcar_empleado_libre,
    actualizar_estado_pedido,
    agregar_pedido_a_usuario
)

def lambda_handler(event, context):
    """Lambda para confirmar la entrega del pedido"""
    print(f'Iniciando proceso de confirmar: {json.dumps(event)}')
    
    local_id = event.get('local_id')
    pedido_id = event.get('pedido_id')
    repartidor_dni = event.get('repartidor_dni')
    usuario_correo = event.get('usuario_correo')
    
    if not local_id or not pedido_id:
        raise ValueError('Faltan parámetros requeridos: local_id o pedido_id')
    
    try:
        # Liberar al repartidor
        if repartidor_dni:
            marcar_empleado_libre(local_id, repartidor_dni)
        
        # Actualizar estado del pedido a recibido
        pedido_actualizado = actualizar_estado_pedido(
            local_id,
            pedido_id,
            'recibido',
            None
        )
        
        # Agregar pedido al historial del usuario
        if usuario_correo:
            agregar_pedido_a_usuario(usuario_correo, pedido_id)
        
        print(f'Pedido confirmado y completado: {pedido_id}')
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Pedido completado exitosamente',
                'pedido_id': pedido_id,
                'estado': 'recibido',
                'pedido': pedido_actualizado
            }
        }
        
    except Exception as e:
        print(f'Error en lambda confirmar: {str(e)}')
        raise
