import json
import boto3
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
stepfunctions = boto3.client('stepfunctions', region_name='us-east-1')

def lambda_handler(event, context):
    """Lambda para procesar la confirmación del usuario y continuar el Step Function"""
    print(f'Procesando confirmación de recepción: {json.dumps(event)}')
    
    # Este lambda puede ser invocado por API Gateway cuando el usuario confirma
    body = json.loads(event.get('body', '{}')) if isinstance(event.get('body'), str) else event
    
    local_id = body.get('local_id')
    pedido_id = body.get('pedido_id')
    confirmado = body.get('confirmado', True)
    
    if not local_id or not pedido_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan parámetros requeridos'})
        }
    
    try:
        # Obtener el taskToken del pedido
        table = dynamodb.Table(os.environ['TABLE_PEDIDOS'])
        response = table.get_item(
            Key={
                'local_id': local_id,
                'pedido_id': pedido_id
            }
        )
        
        pedido = response.get('Item')
        if not pedido:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Pedido no encontrado'})
            }
        
        task_token = pedido.get('task_token')
        if not task_token:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No hay confirmación pendiente para este pedido'})
            }
        
        # Enviar éxito al Step Function para continuar
        stepfunctions.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                'confirmado': confirmado,
                'tipo': 'manual',
                'mensaje': 'Usuario confirmó la recepción del pedido'
            })
        )
        
        # Limpiar el taskToken
        table.update_item(
            Key={
                'local_id': local_id,
                'pedido_id': pedido_id
            },
            UpdateExpression='REMOVE task_token, esperando_confirmacion'
        )
        
        print(f'Confirmación procesada exitosamente para pedido {pedido_id}')
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Confirmación procesada exitosamente',
                'pedido_id': pedido_id
            })
        }
        
    except Exception as e:
        print(f'Error al procesar confirmación: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
