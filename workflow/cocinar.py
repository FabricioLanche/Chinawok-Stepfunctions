import json
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.dynamodb_helper import (
    buscar_empleado_disponible,
    marcar_empleado_ocupado,
    actualizar_estado_pedido
)
from utils.json_encoder import json_dumps

def lambda_handler(event, context):
    """Lambda para asignar cocinero y comenzar a cocinar el pedido"""
    print(f'Iniciando proceso de cocinar: {json.dumps(event)}')
    
    # Manejar invocación desde API Gateway (HTTP) o Step Functions (directo)
    if 'body' in event:
        # Invocación HTTP desde API Gateway
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    else:
        # Invocación directa desde Step Functions
        body = event
    
    local_id = body.get('local_id')
    pedido_id = body.get('pedido_id')
    
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
        
        result = {
            **body,
            **pedido_actualizado,
            'cocinero_dni': cocinero['dni']
        }
        
        # Si fue invocado por HTTP, devolver respuesta HTTP
        if 'body' in event:
            return {
                'statusCode': 200,
                'body': json_dumps(result),
                'headers': {'Content-Type': 'application/json'}
            }
        
        return result
        
    except Exception as e:
        print(f'Error en lambda cocinar: {str(e)}')
        
        if 'body' in event:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)}),
                'headers': {'Content-Type': 'application/json'}
            }
        raise
