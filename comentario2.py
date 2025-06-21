import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    # Entrada de datos
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]

    # Generar UUID para el comentario
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {'texto': texto}
    }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_dynamo = table.put_item(Item=comentario)

    # Estrategia Ingesta Push: subir JSON al bucket S3
    s3 = boto3.client('s3')
    s3_key = f"{tenant_id}/comentario_{uuidv1}.json"
    comentario_json = json.dumps(comentario, ensure_ascii=False)

    try:
        response_s3 = s3.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=comentario_json,
            ContentType='application/json'
        )
        print(f"Comentario subido a S3: s3://{nombre_bucket}/{s3_key}")
    except Exception as e:
        print(f"Error al subir a S3: {e}")
        response_s3 = {'Error': str(e)}

    # Salida de la función
    return {
        'statusCode': 200,
        'comentario': comentario,
        'dynamodb_response': response_dynamo,
        's3_response': response_s3,
        's3_location': f"s3://{nombre_bucket}/{s3_key}"
    }
