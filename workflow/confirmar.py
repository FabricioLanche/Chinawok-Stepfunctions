import json
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.dynamodb_helper import (
    obtener_pedido,
    marcar_empleado_libre,
    finalizar_pedido,
    agregar_pedido_a_usuario
)
from utils.json_encoder import json_dumps

def lambda_handler(event, context):
    """Lambda para confirmar la entrega del pedido"""
    print(f'Iniciando proceso de confirmar: {json.dumps(event)}')
    
    # Manejar invocación desde API Gateway (HTTP) o Step Functions (directo)
    if 'body' in event:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    else:
        body = event
    
    local_id = body.get('local_id')
    pedido_id = body.get('pedido_id')
    repartidor_dni = body.get('repartidor_dni')
    
    if not local_id or not pedido_id:
        raise ValueError('Faltan parámetros requeridos: local_id o pedido_id')
    
    try:
        # Obtener información del pedido
        pedido = obtener_pedido(local_id, pedido_id)
        usuario_correo = pedido.get('usuario_correo')
        
        # Liberar al repartidor
        if repartidor_dni:
            marcar_empleado_libre(local_id, repartidor_dni)
        
        # Finalizar pedido (actualizar estado a recibido y cerrar historial)
        pedido_actualizado = finalizar_pedido(local_id, pedido_id)
        
        # Agregar pedido al historial del usuario
        if usuario_correo:
            agregar_pedido_a_usuario(usuario_correo, pedido_id)
        
        print(f'Pedido confirmado y completado: {pedido_id}')
        
        result = {
            'message': 'Pedido completado exitosamente',
            'pedido_id': pedido_id,
            'estado': 'recibido',
            'pedido': pedido_actualizado
        }
        
        if 'body' in event:
            return {
                'statusCode': 200,
                'body': json_dumps(result),
                'headers': {'Content-Type': 'application/json'}
            }
        
        return result
        
    except Exception as e:
        print(f'Error en lambda confirmar: {str(e)}')
        
        if 'body' in event:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)}),
                'headers': {'Content-Type': 'application/json'}
            }
        raise
