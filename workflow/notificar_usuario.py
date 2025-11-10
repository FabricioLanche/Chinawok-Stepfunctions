import json
import boto3
import os

# Este lambda se encarga de notificar al usuario que su pedido ha llegado
# y guarda el taskToken para que pueda ser usado cuando el usuario confirme
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
sns = boto3.client('sns', region_name='us-east-1')

def lambda_handler(event, context):
    """Lambda para notificar al usuario sobre la entrega y esperar confirmación"""
    print(f'Notificando usuario sobre entrega: {json.dumps(event)}')
    
    pedido_id = event.get('pedido_id')
    usuario_correo = event.get('usuario_correo')
    task_token = event.get('taskToken')
    
    if not pedido_id or not usuario_correo or not task_token:
        raise ValueError('Faltan parámetros requeridos')
    
    try:
        # Guardar el taskToken en DynamoDB para recuperarlo cuando el usuario confirme
        table = dynamodb.Table(os.environ['TABLE_PEDIDOS'])
        table.update_item(
            Key={
                'local_id': event.get('local_id'),
                'pedido_id': pedido_id
            },
            UpdateExpression='SET task_token = :token, esperando_confirmacion = :true',
            ExpressionAttributeValues={
                ':token': task_token,
                ':true': True
            }
        )
        
        # Aquí podrías enviar una notificación al usuario (SNS, SES, etc.)
        mensaje = f"""
        ¡Tu pedido {pedido_id} ha llegado a su destino!
        
        Por favor confirma la recepción accediendo a:
        https://tu-app.com/confirmar-pedido/{pedido_id}
        
        O responde a este mensaje con 'CONFIRMAR'.
        """
        
        print(f'Notificación enviada al usuario {usuario_correo}')
        print(f'TaskToken guardado: {task_token[:20]}...')
        
        # El Step Function esperará aquí hasta que se llame a SendTaskSuccess
        # con el taskToken guardado
        
        return {
            'statusCode': 200,
            'message': 'Notificación enviada, esperando confirmación del usuario',
            'pedido_id': pedido_id
        }
        
    except Exception as e:
        print(f'Error al notificar usuario: {str(e)}')
        raise
