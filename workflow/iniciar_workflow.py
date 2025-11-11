import json
import boto3
import os

stepfunctions = boto3.client('stepfunctions', region_name='us-east-1')

def lambda_handler(event, context):
    """Lambda para iniciar el workflow de Step Functions"""
    print(f'Iniciando workflow: {json.dumps(event)}')
    
    # Manejar invocación desde API Gateway
    if 'body' in event:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    else:
        body = event
    
    local_id = body.get('local_id')
    pedido_id = body.get('pedido_id')
    
    if not local_id or not pedido_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan parámetros requeridos: local_id y pedido_id'}),
            'headers': {'Content-Type': 'application/json'}
        }
    
    try:
        # ARN de la máquina de estados (se obtiene del environment)
        state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
        
        if not state_machine_arn:
            raise ValueError('STATE_MACHINE_ARN no está configurado en las variables de entorno')
        
        print(f'State Machine ARN: {state_machine_arn}')
        
        # Iniciar la ejecución del Step Function
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f'pedido-{pedido_id}',
            input=json.dumps({
                'local_id': local_id,
                'pedido_id': pedido_id
            })
        )
        
        execution_arn = response['executionArn']
        start_date = response['startDate'].isoformat()
        
        print(f'Workflow iniciado exitosamente: {execution_arn}')
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Workflow iniciado exitosamente',
                'execution_arn': execution_arn,
                'execution_name': f'pedido-{pedido_id}',
                'pedido_id': pedido_id,
                'local_id': local_id,
                'start_date': start_date,
                'console_url': f'https://console.aws.amazon.com/states/home?region=us-east-1#/executions/details/{execution_arn}'
            }),
            'headers': {'Content-Type': 'application/json'}
        }
        
    except stepfunctions.exceptions.ExecutionAlreadyExists:
        return {
            'statusCode': 409,
            'body': json.dumps({
                'error': f'Ya existe una ejecución en curso para el pedido {pedido_id}',
                'pedido_id': pedido_id
            }),
            'headers': {'Content-Type': 'application/json'}
        }
        
    except Exception as e:
        print(f'Error al iniciar workflow: {str(e)}')
        import traceback
        print(f'Traceback: {traceback.format_exc()}')
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'type': type(e).__name__
            }),
            'headers': {'Content-Type': 'application/json'}
        }
