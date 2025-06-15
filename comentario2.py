import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    # Proceso
    uuidv1 = str(uuid.uuid1())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        },
        'timestamp': timestamp
    }
    
    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_dynamo = table.put_item(Item=comentario)
    
    # Estrategia de Ingesta Push - Guardar JSON en S3
    s3_client = boto3.client('s3')
    
    # Crear nombre de archivo con estructura tenant_id/año/mes/día/uuid_timestamp.json
    fecha_actual = datetime.now()
    s3_key = f"{tenant_id}/{fecha_actual.year}/{fecha_actual.month:02d}/{fecha_actual.day:02d}/comentario_{uuidv1}_{timestamp}.json"
    
    try:
        # Convertir el comentario a JSON y subirlo a S3
        comentario_json = json.dumps(comentario, ensure_ascii=False, indent=2)
        
        s3_response = s3_client.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=comentario_json,
            ContentType='application/json',
            Metadata={
                'tenant_id': tenant_id,
                'uuid': uuidv1,
                'timestamp': timestamp
            }
        )
        
        print(f"Archivo JSON guardado en S3: s3://{nombre_bucket}/{s3_key}")
        
    except Exception as e:
        print(f"Error al guardar en S3: {str(e)}")
        # Continúa el proceso aunque falle S3, ya que el comentario se guardó en DynamoDB
        s3_response = {"Error": str(e)}
    
    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'dynamodb_response': response_dynamo,
        's3_response': s3_response,
        's3_location': f"s3://{nombre_bucket}/{s3_key}"
    }
