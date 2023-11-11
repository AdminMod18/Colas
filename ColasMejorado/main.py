from fastapi import FastAPI
app = FastAPI()

import boto3

aws_access_key_id = 'AKIAXII2BKO7PPH2CJMQ'
aws_secret_access_key = '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'

sqs = boto3.client(
    'sqs',
    region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
queue_url = 'https://sqs.us-east-1.amazonaws.com/498807690174/transacciones_banco'

@app.get("/")

def root():
    return {
        "Servicio": "Estructura de datos"
    }

@app.post("/algoritmo-sqs")

def post(message : dict):
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody='hola......'
    )
    print(f'Mensaje publicado con éxito: {response["MessageId"]}')
    return {
        "id":response["MessageId"]
    }

@app.get("/algoritmo-sqs")
def process():
    count = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=1,  # Modificado para recibir el máximo de mensajes
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )

        if response.get('Messages'):
            for message in response['Messages']:
                print(f"Mensaje recibido: {message['MessageId']}")  # Imprimir el ID del mensaje

                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                count += 1
                return {"RESPUESTA": f"Se recibieron y eliminaron {count} mensajes de la cola. Último mensaje procesado: {message['MessageId']}"}

        else:
            print("No se encontraron mensajes en la cola.")
            return {"RESPUESTA": "No se encontraron mensajes en la cola."}
            break

    print(f"Total de mensajes consumidos: {count}")
    sqs.close()
