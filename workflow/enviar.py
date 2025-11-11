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
from utils.json_encoder import json_dumps

def lambda_handler(event, context):
    """Lambda para asignar repartidor y enviar el pedido"""
    print(f'Iniciando proceso de enviar: {json.dumps(event)}')
    
    # Manejar invocación desde API Gateway (HTTP) o Step Functions (directo)
    if 'body' in event:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    else:
        body = event
    
    local_id = body.get('local_id')
    pedido_id = body.get('pedido_id')
    despachador_dni = body.get('despachador_dni')
    
    if not local_id or not pedido_id:
        raise ValueError('Faltan parámetros requeridos: local_id o pedido_id')
    
    try:
        # Liberar al despachador
        if despachador_dni:
            marcar_empleado_libre(local_id, despachador_dni)
        
        # Buscar repartidor disponible
        repartidor = buscar_empleado_disponible(local_id, 'Repartidor')
        
        if not repartidor:
            raise Exception('No hay repartidores disponibles en este momento')
        
        # Marcar repartidor como ocupado
        marcar_empleado_ocupado(local_id, repartidor['dni'])
        
        # Actualizar estado del pedido
        pedido_actualizado = actualizar_estado_pedido(
            local_id,
            pedido_id,
            'enviando',
            repartidor
        )
        
        print(f"Pedido asignado a repartidor {repartidor['dni']}")
        
        result = {**body, **pedido_actualizado, 'repartidor_dni': repartidor['dni']}
        result.pop('despachador_dni', None)
        
        if 'body' in event:
            return {
                'statusCode': 200,
                'body': json_dumps(result),
                'headers': {'Content-Type': 'application/json'}
            }
        
        return result
        
    except Exception as e:
        print(f'Error en lambda enviar: {str(e)}')
        
        if 'body' in event:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)}),
                'headers': {'Content-Type': 'application/json'}
            }
        raise
